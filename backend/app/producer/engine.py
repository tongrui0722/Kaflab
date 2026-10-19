from __future__ import annotations
import asyncio
import random
import string
import time
import uuid

from ..models import KafkaTraceEvent, ProduceRequest, SecurityConfig, error_name
from ..network.client import KafkaClient
from ..network.tcp import parse_hostport
from ..protocol import codec
from ..session.manager import SessionManager
from ..trace.manager import TraceManager, now_ms
from .shared import KafkaFailure, _fail, _metadata_dict, _pick


def _sec(req: ProduceRequest) -> SecurityConfig:
    return SecurityConfig(securityProtocol=req.securityProtocol,
                          saslMechanism=req.saslMechanism,
                          saslUsername=req.saslUsername,
                          saslPassword=req.saslPassword)


class ProducerEngine:
    def __init__(self, sessions: SessionManager, traces: TraceManager):
        self.sessions = sessions
        self.traces = traces

    async def run(self, req: ProduceRequest, session_id: str) -> None:
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        t0 = now_ms()
        sec = _sec(req)
        clients: list[KafkaClient] = []
        sent_count = 0
        confirmed_bytes = 0
        sample_key: str | None = None
        sample_value: str | None = None
        sample_key_format = "text"
        sample_value_format = "text"
        sample_truncated = False
        first_offset: int | None = None
        last_offset: int | None = None

        def emit(**kw) -> KafkaTraceEvent:
            ev = KafkaTraceEvent(traceId=trace_id, **kw)
            self.traces.append(session_id, ev)
            return ev

        def new_client(host: str, port: int, broker_id=None) -> KafkaClient:
            c = KafkaClient(self.traces, session_id, trace_id, req.clientId,
                            req.requestTimeoutMs, host, port, sec, broker_id)
            clients.append(c)
            return c

        emit(step="session.start", stepIndex=0, status="success",
             topic=req.topic, direction="local", apiName="LOCAL",
             request={"bootstrap": req.bootstrap, "clientId": req.clientId,
                      "securityProtocol": req.securityProtocol,
                      "saslMechanism": req.saslMechanism if sec.securityProtocol != "PLAINTEXT" else None})

        try:
            host, port = parse_hostport(req.bootstrap)
            bs = new_client(host, port)
            await bs.connect(1, label="bootstrap")

            # 2. ApiVersions
            api_dec, _, _ = await bs.call(18, "ApiVersions", 0,
                                          codec.encode_apiversions_v0(),
                                          2, "apiversions",
                                          extra_req={"apiVersions": "0 (probe)"},
                                          decode=codec.decode_apiversions_v0)
            supported = {a["apiKey"]: (a["minVersion"], a["maxVersion"])
                         for a in api_dec.get("apis", [])}
            emit(step="apiversions.negotiated", stepIndex=2, status="success",
                 direction="local", apiName="ApiVersions",
                 response={"negotiated": {
                     "Produce": _pick(supported, 0, 9),
                     "Fetch": _pick(supported, 1, 1),
                     "Metadata": _pick(supported, 3, 0),
                     "FindCoordinator": _pick(supported, 10, 0),
                     "JoinGroup": _pick(supported, 11, 1),
                     "SyncGroup": _pick(supported, 14, 1),
                     "Heartbeat": _pick(supported, 12, 1),
                     "SaslHandshake": _pick(supported, 17, 1),
                     "SaslAuthenticate": _pick(supported, 36, 1)}})

            # 3. Metadata
            meta_dec, _, _ = await bs.call(
                3, "Metadata", 0, codec.encode_metadata_v0([req.topic]),
                3, "metadata", extra_req={"topics": [req.topic]}, topic=req.topic,
                decode=codec.decode_metadata_v0)
            # decode_metadata_v0 returns object; convert to dict form
            meta_dec = _metadata_dict(meta_dec)
            brokers = meta_dec["brokers"]
            tmeta = next((x for x in meta_dec["topics"] if x["topic"] == req.topic), None)
            if tmeta is None:
                raise _fail(f"topic '{req.topic}' missing from metadata", "UNKNOWN_TOPIC_OR_PARTITION", 3)
            topic_err = tmeta.get("error", 0)
            parts = tmeta.get("partitions", [])
            if topic_err != 0 or not parts:
                raise _fail(f"metadata error for '{req.topic}': {error_name(topic_err)}",
                            error_name(topic_err), 3, topic_err)
            emit(step="metadata.updated", stepIndex=3, status="success",
                 direction="local", apiName="Metadata", topic=req.topic,
                 response={"clusterId": meta_dec.get("clusterId"),
                           "controller": meta_dec.get("controller"),
                           "brokers": brokers, "partitions": parts})

            # 4. Partition selection
            if req.partition.lower() == "auto":
                healthy = [p for p in parts if p.get("error", 0) == 0] or parts
                chosen = healthy[0]
            else:
                idx = int(req.partition)
                chosen = next((p for p in parts if p["partition"] == idx), None)
                if chosen is None:
                    raise _fail(f"partition {idx} not found", "UNKNOWN_PARTITION", 4)
            if chosen.get("error", 0) != 0:
                raise _fail(f"partition {chosen['partition']} error: {error_name(chosen['error'])}",
                            error_name(chosen["error"]), 4, chosen["error"])
            leader_id = chosen["leader"]
            if leader_id is None or leader_id < 0:
                raise _fail(f"partition {chosen['partition']} has no leader (LeaderNotAvailable)",
                            "LEADER_NOT_AVAILABLE", 4, 5)
            leader = next((b for b in brokers if b.get("id") == leader_id), None)
            emit(step="partition.selected", stepIndex=4, status="success",
                 direction="local", apiName="PARTITION", topic=req.topic,
                 partition=chosen["partition"],
                 broker=leader or {"host": host, "port": port},
                 latencyMs=1, request={"strategy": req.partition},
                 response={"partition": chosen["partition"], "leader": leader_id})

            # 5. Connect leader (advertised host may be unreachable → fallback bootstrap host)
            lhost, lport = (leader["host"], leader["port"]) if leader else (host, port)
            lc = new_client(lhost, lport, leader_id)
            try:
                await lc.connect(5, label=f"leader-{leader_id}")
            except OSError as e:
                emit(step="tcp.fallback", stepIndex=5, status="success",
                     direction="local", apiName="TCP", topic=req.topic,
                     partition=chosen["partition"],
                     response={"warning": f"leader {lhost}:{lport} unreachable ({e}); "
                                          f"retry {host}:{port}"})
                lc.host, lc.port = host, port
                await lc.connect(5, label=f"leader-{leader_id}-via-bootstrap")
                lhost, lport = host, port

            # 6/7. Produce — this broker rejects RecordBatch v2 (closes conn),
            # so use legacy MessageSet v0 which it ACKs. Real offset returned.
            acks = codec.pick_acks(req.acks)
            produce_v = 0
            def _raw(s: str, h: str) -> bytes:
                hs = (h or "").strip().replace(" ", "")
                if hs:
                    return bytes.fromhex(hs)
                return (s or "").encode()

            def _preview(data: bytes, as_hex: bool) -> tuple[str, bool]:
                preview = data[:256]
                return (preview.hex() if as_hex else preview.decode("utf-8", errors="replace"),
                        len(data) > len(preview))
            records = [(_raw(m.key, m.keyHex), _raw(m.value, m.valueHex)) for m in req.messages] or [
                (_raw(req.key, req.keyHex), _raw(req.value, req.valueHex))]
            generated = req.messageCount > 0
            total_records = req.messageCount if generated else len(records)
            chunk_size = 100
            chunks = (total_records + chunk_size - 1) // chunk_size if generated else 1
            alphabet = string.ascii_letters + string.digits

            def _generated_records(count: int) -> list[tuple[bytes, bytes]]:
                fixed_key = _raw(req.key, req.keyHex) if req.key or req.keyHex else None
                fixed_value = _raw(req.value, req.valueHex) if req.value or req.valueHex else None
                return [
                    (fixed_key if fixed_key is not None else f"k-{''.join(random.choices(alphabet, k=6))}".encode(),
                     fixed_value if fixed_value is not None else ''.join(random.choices(alphabet, k=32)).encode())
                    for _ in range(count)
                ]

            for chunk_index in range(chunks):
                if generated:
                    records = _generated_records(min(chunk_size, total_records - sent_count))
                is_batch = total_records > 1
                last_err: Exception | None = None
                chunk_sent = False
                for attempt in range(max(req.retries, 1)):
                    body = codec.encode_produce_v0_multi(req.topic, chosen["partition"],
                                                           records,
                                                           acks=max(acks, 1),
                                                           timeout_ms=req.requestTimeoutMs)
                    dec, _, _ = await lc.call(
                        0, "Produce", produce_v, body, 6, "produce",
                        extra_req={"topic": req.topic, "partition": chosen["partition"],
                                   "acks": req.acks, "timeoutMs": req.requestTimeoutMs,
                                   "attempt": attempt + 1, "produceVersion": 0,
                                   "records": len(records),
                                   "batchIndex": chunk_index + 1, "batchTotal": chunks,
                                   "batch": is_batch,
                                   "key": records[0][0].decode(errors="replace") if not is_batch and acks != 0 else (None if acks == 0 else f"{len(records)} keys"),
                                   "value": records[0][1].decode(errors="replace") if not is_batch and acks != 0 else (None if acks == 0 else f"{len(records)} values")},
                        topic=req.topic, partition=chosen["partition"],
                        decode=codec.decode_produce_v0, resp_index=7,
                        capture_raw=not generated or chunk_index in (0, chunks - 1))
                    perr = dec["topics"][0]["partitions"][0]["errorCode"] if dec["topics"] else -1
                    base_off = dec["topics"][0]["partitions"][0]["baseOffset"] if dec["topics"] else 0
                    if perr == 6:  # NotLeaderForPartition → refresh metadata once, then retry
                        emit(step="produce.retry", stepIndex=7, status="error",
                             direction="local", apiKey=0, apiName="Produce",
                             topic=req.topic, partition=chosen["partition"],
                             errorCode=6, errorName="NOT_LEADER_FOR_PARTITION",
                             response={"action": "refresh metadata + retry on new leader",
                                       "attempt": attempt + 1})
                        try:
                            lc.close()
                        except Exception:
                            pass
                        try:
                            m2, _, _ = await bs.call(
                                3, "Metadata", 0, codec.encode_metadata_v0([req.topic]),
                                3, "metadata.refresh",
                                extra_req={"topics": [req.topic], "reason": "NotLeader"},
                                topic=req.topic, decode=codec.decode_metadata_v0)
                            m2 = _metadata_dict(m2)
                            t2 = next((x for x in m2["topics"] if x["topic"] == req.topic), None)
                            ps2 = t2.get("partitions", []) if t2 else []
                            c2 = next((p for p in ps2 if p["partition"] == chosen["partition"]), None)
                            if c2 and c2.get("leader", -1) >= 0 and c2["leader"] != leader_id:
                                leader_id = c2["leader"]
                                leader = next((b for b in m2["brokers"] if b.get("id") == leader_id), None)
                                chosen = c2
                                emit(step="metadata.refreshed", stepIndex=7, status="success",
                                     direction="local", apiName="Metadata",
                                     topic=req.topic, partition=chosen["partition"],
                                     response={"newLeader": leader_id})
                                lhost, lport = (leader["host"], leader["port"]) if leader else (host, port)
                                lc = new_client(lhost, lport, leader_id)
                                await lc.connect(5, label=f"leader-{leader_id}-refreshed")
                                continue  # retry produce on new leader (same attempt budget)
                        except Exception as re:
                            emit(step="metadata.refresh_failed", stepIndex=7, status="error",
                                 direction="local", apiName="Metadata",
                                 response={"error": str(re)[:200]})
                        last_err = _fail("NotLeaderForPartition: leader moved, refresh metadata and retry",
                                         "NOT_LEADER_FOR_PARTITION", 7, 6)
                        break
                    if perr != 0:
                        last_err = _fail(f"produce failed: {error_name(perr)}",
                                         error_name(perr), 7, perr)
                        if perr in (7, 8, 13) and attempt + 1 < max(req.retries, 1):
                            await asyncio.sleep(min(0.2 * (attempt + 1), 1.0))
                            continue
                        break
                    if first_offset is None:
                        first_offset = base_off
                    if sample_key is None:
                        key_hex = bool(req.messages[0].keyHex) if req.messages else bool(req.keyHex)
                        value_hex = bool(req.messages[0].valueHex) if req.messages else bool(req.valueHex)
                        sample_key, key_cut = _preview(records[0][0], key_hex)
                        sample_value, value_cut = _preview(records[0][1], value_hex)
                        sample_key_format = "hex" if key_hex else "text"
                        sample_value_format = "hex" if value_hex else "text"
                        sample_truncated = key_cut or value_cut
                    last_offset = base_off + len(records) - 1
                    sent_count += len(records)
                    confirmed_bytes += sum(len(key) + len(value) for key, value in records)
                    chunk_sent = True
                    if generated:
                        emit(step="produce.progress", stepIndex=6, status="success",
                             direction="local", apiName="Produce", topic=req.topic,
                             partition=chosen["partition"],
                             response={"sent": sent_count, "total": total_records,
                                       "batchIndex": chunk_index + 1, "batchTotal": chunks,
                                       "lastOffset": last_offset})
                    break
                if not chunk_sent:
                    assert last_err is not None
                    raise last_err

            total = now_ms() - t0
            emit(step="session.done", stepIndex=8, status="success",
                 direction="local", apiName="TOTAL", topic=req.topic,
                 partition=chosen["partition"], latencyMs=total,
                 response={"offset": first_offset, "lastOffset": last_offset,
                           "partition": chosen["partition"], "totalMs": total,
                           "records": sent_count, "batch": total_records > 1,
                           "batches": chunks,
                           "totalBytes": confirmed_bytes,
                           "sampleKey": sample_key, "sampleValue": sample_value,
                           "sampleKeyFormat": sample_key_format,
                           "sampleValueFormat": sample_value_format,
                           "sampleTruncated": sample_truncated,
                           "offsets": [first_offset + i for i in range(sent_count)]
                           if not generated and total_records > 1 and first_offset is not None else None})
            self.sessions.finish(session_id, True, partition=chosen["partition"],
                                 offset=first_offset, mode="live")
        except Exception as e:  # noqa: BLE001 — NO mock: surface the real failure
            emit(step="session.failed", stepIndex=8, status="error",
                 direction="local", apiName="TOTAL", topic=req.topic,
                 errorCode=getattr(e, "kafka_code", None),
                 errorName=getattr(e, "kafka_name", None),
                 response={"error": str(e), "sent": sent_count,
                           "requested": req.messageCount or len(req.messages) or 1,
                           "confirmedBytes": confirmed_bytes,
                           "completedBatches": (sent_count + 99) // 100 if req.messageCount else (1 if sent_count else 0),
                           "sampleKey": sample_key, "sampleValue": sample_value,
                           "sampleKeyFormat": sample_key_format,
                           "sampleValueFormat": sample_value_format,
                           "sampleTruncated": sample_truncated,
                           "firstOffset": first_offset,
                           "lastOffset": last_offset})
            self.sessions.finish(session_id, False, mode="live",
                                 error=str(e)[:500])
        finally:
            for c in clients:
                c.close()


def _pick(supported: dict, key: int, want: int) -> int:
    if key not in supported:
        return want
    lo, hi = supported[key]
    return max(lo, min(hi, want))


def _metadata_dict(dec) -> dict:
    if isinstance(dec, dict):
        return dec
    return {"brokers": dec.brokers,
            "topics": [{"topic": t.topic, "error": t.error,
                        "partitions": [{"partition": p.partition, "leader": p.leader,
                                        "replicas": p.replicas, "isr": p.isr,
                                        "error": p.error} for p in t.partitions]}
                       for t in dec.topics]}


class KafkaFailure(RuntimeError):
    def __init__(self, msg: str, name: str, step: int, code: int | None = None):
        super().__init__(msg)
        self.kafka_name = name
        self.kafka_code = code
        self.kafka_step = step


def _fail(msg: str, name: str, step: int, code: int | None = None) -> KafkaFailure:
    return KafkaFailure(msg, name, step, code)
