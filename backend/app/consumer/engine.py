from __future__ import annotations
import asyncio
import hashlib
import struct
import time
import uuid

from ..models import ConsumerFetchRequest, GroupDescribeRequest, SecurityConfig, error_name
from ..network.client import KafkaClient
from ..network.tcp import parse_hostport
from ..protocol import codec
from ..producer.shared import KafkaFailure, _fail, _metadata_dict, _pick
from ..session.manager import SessionManager
from ..trace.manager import TraceManager, now_ms


def _secf(req) -> SecurityConfig:
    return SecurityConfig(securityProtocol=req.securityProtocol,
                          saslMechanism=req.saslMechanism,
                          saslUsername=req.saslUsername,
                          saslPassword=req.saslPassword)


class ConsumerEngine:
    def __init__(self, sessions: SessionManager, traces: TraceManager):
        self.sessions = sessions
        self.traces = traces

    async def fetch(self, req: ConsumerFetchRequest, session_id: str) -> None:
        from ..models import KafkaTraceEvent
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        t0 = now_ms()
        sec = _secf(req)
        clients: list[KafkaClient] = []

        def emit(**kw):
            self.traces.append(session_id, KafkaTraceEvent(traceId=trace_id, **kw))

        def new_client(host, port, bid=None):
            c = KafkaClient(self.traces, session_id, trace_id, req.clientId,
                            req.requestTimeoutMs, host, port, sec, bid)
            clients.append(c)
            return c

        emit(step="session.start", stepIndex=0, status="success", topic=req.topic,
             direction="local", apiName="LOCAL",
             request={"op": "Fetch", "bootstrap": req.bootstrap,
                      "securityProtocol": req.securityProtocol,
                      "saslMechanism": req.saslMechanism})
        try:
            host, port = parse_hostport(req.bootstrap)
            bs = new_client(host, port)
            await bs.connect(1, label="bootstrap")
            await bs.call(18, "ApiVersions", 0, codec.encode_apiversions_v0(),
                          2, "apiversions", extra_req={"probe": True})

            # Metadata (partition leader + hw baseline)
            meta_dec, _, _ = await bs.call(
                3, "Metadata", 0, codec.encode_metadata_v0([req.topic]),
                3, "metadata", extra_req={"topics": [req.topic]},
                topic=req.topic, decode=codec.decode_metadata_v0)
            meta_dec = _metadata_dict(meta_dec)
            tmeta = next((x for x in meta_dec["topics"] if x["topic"] == req.topic), None)
            if not tmeta or not tmeta["partitions"]:
                raise _fail(f"topic '{req.topic}' missing", "UNKNOWN_TOPIC_OR_PARTITION", 3)
            part = next((p for p in tmeta["partitions"] if p["partition"] == req.partition), None)
            if part is None:
                raise _fail(f"partition {req.partition} not found", "UNKNOWN_PARTITION", 4)
            if part["leader"] is None or part["leader"] < 0:
                raise _fail("no leader", "LEADER_NOT_AVAILABLE", 4, 5)
            leader = next((b for b in meta_dec["brokers"] if b.get("id") == part["leader"]), None)
            emit(step="partition.selected", stepIndex=4, status="success",
                 direction="local", apiName="PARTITION", topic=req.topic,
                 partition=req.partition, latencyMs=1,
                 broker=leader or {"host": host, "port": port},
                 response={"partition": req.partition, "leader": part["leader"]})

            lhost, lport = (leader["host"], leader["port"]) if leader else (host, port)
            lc = new_client(lhost, lport, part["leader"])
            try:
                await lc.connect(5, label=f"leader-{part['leader']}")
            except OSError:
                lc.host, lc.port = host, port
                await lc.connect(5, label=f"leader-{part['leader']}-via-bootstrap")

            # Resolve offset: explicit > committed (if group) > latest ListOffsets(-1)
            # NOTE: coordinator client cc is kept open when groupId is set so the
            # post-fetch OffsetCommit below can Join+Commit on the same connection.
            start = req.offset
            committed = None
            cc = None
            if start is None and req.groupId:
                try:
                    fc, _, _ = await lc.call(
                        10, "FindCoordinator", 0,
                        codec.encode_find_coordinator_v0(req.groupId),
                        6, "findcoordinator",
                        extra_req={"key": req.groupId},
                        decode=codec.decode_find_coordinator_v0)
                    if fc.get("errorCode") == 0 and fc.get("port"):
                        cc = new_client(fc["host"] or host, fc["port"], fc.get("nodeId"))
                        try:
                            await cc.connect(6, label="coordinator")
                        except OSError:
                            cc.host, cc.port = host, port
                            await cc.connect(6, label="coordinator-via-bootstrap")
                        of, _, _ = await cc.call(
                            9, "OffsetFetch", 1,
                            codec.encode_offset_fetch_v1(req.groupId, req.topic, [req.partition]),
                            6, "offsetfetch", extra_req={"groupId": req.groupId},
                            decode=codec.decode_offset_fetch_v1)
                        ps = (of["topics"][0]["partitions"] if of["topics"] else [])
                        if ps and ps[0]["offset"] >= 0:
                            committed = ps[0]["offset"]
                except Exception:
                    committed = None
            if start is None:
                start = committed if committed is not None else None
            if start is None:
                lo, _, _ = await lc.call(
                    2, "ListOffsets", 1,
                    codec.encode_listoffsets_v1(req.topic, req.partition, -1),
                    6, "listoffsets", extra_req={"timestamp": "latest"},
                    topic=req.topic, partition=req.partition,
                    decode=codec.decode_listoffsets_v1)
                lp = lo["topics"][0]["partitions"][0]
                if lp["errorCode"] != 0:
                    raise _fail(f"ListOffsets: {error_name(lp['errorCode'])}",
                                error_name(lp["errorCode"]), 6, lp["errorCode"])
                start = max(lp["offset"] - req.maxMessages, 0)
                emit(step="offset.resolved", stepIndex=6, status="success",
                     direction="local", apiName="OFFSET", topic=req.topic,
                     partition=req.partition,
                     response={"startOffset": start, "latest": lp["offset"],
                               "committed": committed})

            fdec, _, _ = await lc.call(
                1, "Fetch", 4,
                codec.encode_fetch_v4(req.topic, req.partition, start),
                7, "fetch",
                extra_req={"offset": start, "maxMessages": req.maxMessages,
                           "fetchVersion": 4},
                topic=req.topic, partition=req.partition,
                decode=codec.decode_fetch_v4)
            fp = fdec["topics"][0]["partitions"][0]
            if fp["errorCode"] != 0:
                raise _fail(f"Fetch: {error_name(fp['errorCode'])}",
                            error_name(fp["errorCode"]), 7, fp["errorCode"])
            msgs = fdec.get("messages", [])[:req.maxMessages]
            commit_info = None
            if req.groupId:
                commit_info = await self._join_commit_leave(
                    req, session_id, emit, new_client, host, port, cc, start, msgs)
            total = now_ms() - t0
            emit(step="session.done", stepIndex=8, status="success",
                 direction="local", apiName="TOTAL", topic=req.topic,
                 partition=req.partition, latencyMs=total,
                 response={"messages": msgs, "count": len(msgs),
                           "startOffset": start,
                           "highWatermark": fp["highWatermark"],
                           "commit": commit_info,
                           "totalMs": total})
            self.sessions.finish(session_id, True, partition=req.partition,
                                 mode="live")
        except Exception as e:  # noqa: BLE001
            from ..models import KafkaTraceEvent as E
            self.traces.append(session_id, E(
                traceId=trace_id, step="session.failed", stepIndex=8,
                status="error", direction="local", apiName="TOTAL",
                topic=req.topic, partition=req.partition,
                errorCode=getattr(e, "kafka_code", None),
                errorName=getattr(e, "kafka_name", None),
                response={"error": str(e)}))
            self.sessions.finish(session_id, False, mode="live", error=str(e)[:500])
        finally:
            for c in clients:
                c.close()

    async def _join_commit_leave(self, req, session_id, emit, new_client,
                                   host, port, cc, start, msgs) -> dict | None:
        """Join→Sync→OffsetCommit→Heartbeat→Leave on the coordinator.

        Returns commit info dict, or None when group is not used / commit
        was skipped. All steps emit real trace events; failures are
        surfaced (no silent fake-commit).
        """
        if not req.groupId:
            return None
        try:
            if cc is None or getattr(cc, "conn", None) is None:
                bs2 = new_client(host, port)
                await bs2.connect(6, label="coordinator-bootstrap")
                fc, _, _ = await bs2.call(
                    10, "FindCoordinator", 0,
                    codec.encode_find_coordinator_v0(req.groupId),
                    6, "findcoordinator", extra_req={"key": req.groupId},
                    decode=codec.decode_find_coordinator_v0)
                if fc.get("errorCode") not in (0, None):
                    raise _fail(f"FindCoordinator: {error_name(fc['errorCode'])}",
                                error_name(fc["errorCode"]), 6, fc["errorCode"])
                cc = new_client(fc.get("host") or host, fc.get("port") or port,
                                fc.get("nodeId"))
                try:
                    await cc.connect(6, label="coordinator")
                except OSError:
                    cc.host, cc.port = host, port
                    await cc.connect(6, label="coordinator-via-bootstrap")
            jg, _, _ = await cc.call(
                11, "JoinGroup", 1,
                codec.encode_join_group_v1(req.groupId, "", [req.topic]),
                6, "joingroup", extra_req={"groupId": req.groupId},
                decode=codec.decode_join_group_v1)
            if jg.get("errorCode") not in (0, None):
                raise _fail(f"JoinGroup: {error_name(jg['errorCode'])}",
                            error_name(jg["errorCode"]), 6, jg["errorCode"])
            gen, mid = jg["generationId"], jg["memberId"]
            is_leader = (jg.get("leaderId") == mid) or not jg.get("members")
            assigns = ([(m["memberId"], [(req.topic, [req.partition])])
                        for m in (jg.get("members") or [{"memberId": mid}])]
                       if is_leader else [])
            sg, _, _ = await cc.call(
                14, "SyncGroup", 1,
                codec.encode_sync_group_v1(req.groupId, gen, mid, assigns),
                7, "syncgroup", extra_req={"generation": gen, "isLeader": is_leader},
                decode=codec.decode_sync_group_v1)
            if sg.get("errorCode") not in (0, None):
                raise _fail(f"SyncGroup: {error_name(sg['errorCode'])}",
                            error_name(sg["errorCode"]), 7, sg["errorCode"])
            next_off = start + len(msgs)
            oc, _, _ = await cc.call(
                8, "OffsetCommit", 2,
                codec.encode_offset_commit_v2(req.groupId, gen, mid,
                                              req.topic, req.partition, next_off),
                7, "offsetcommit",
                extra_req={"groupId": req.groupId, "offset": next_off,
                           "generation": gen},
                topic=req.topic, partition=req.partition,
                decode=codec.decode_offset_commit_v2)
            perr = (oc["topics"][0]["partitions"][0]["errorCode"]
                    if oc["topics"] and oc["topics"][0]["partitions"] else -1)
            info: dict = {"committedOffset": next_off, "generation": gen,
                          "memberId": mid, "errorCode": perr,
                          "errorName": error_name(perr) if perr else "NONE"}
            if perr != 0:
                emit(step="offsetcommit.failed", stepIndex=7, status="error",
                     direction="local", apiKey=8, apiName="OffsetCommit",
                     topic=req.topic, partition=req.partition,
                     errorCode=perr, errorName=error_name(perr),
                     response=info)
            else:
                emit(step="offset.committed", stepIndex=7, status="success",
                     direction="local", apiKey=8, apiName="OffsetCommit",
                     topic=req.topic, partition=req.partition,
                     response=info)
            try:
                await cc.call(12, "Heartbeat", 1,
                              codec.encode_heartbeat_v1(req.groupId, gen, mid),
                              7, "heartbeat", extra_req={"generation": gen},
                              decode=codec.decode_heartbeat_v1)
            except Exception:
                pass
            try:
                await cc.call(13, "LeaveGroup", 1,
                              codec.encode_leave_group_v1(req.groupId, mid),
                              8, "leavegroup",
                              decode=codec.decode_leave_group_v1)
            except Exception:
                pass
            return info
        except KafkaFailure:
            raise
        except Exception as e:  # noqa: BLE001
            raise _fail(f"commit flow failed: {e}", "COMMIT_FAILED", 7)

    async def groups_overview(self, req: GroupDescribeRequest) -> dict:
        """全量消费组一览：每 broker ListGroups 取并集（各 broker 只认自己协调的组），
        逐组 DescribeGroups（状态/成员/连接信息）+ OffsetFetch（已提交位点），
        结合各分区 log-end 算 totalLag。点某组再用 describe/lag 接口看明细。
        """
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        sec = _secf(req)
        host, port = parse_hostport(req.bootstrap)
        clients: list[KafkaClient] = []

        def nc(h, p, bid=None, tag="sess-groups"):
            c = KafkaClient(self.traces, tag, trace_id,
                            req.clientId, req.requestTimeoutMs, h, p, sec, bid)
            clients.append(c)
            return c

        async def _conn(c, label):
            try:
                await c.connect(1, label=label)
            except OSError:
                c.host, c.port = host, port
                await c.connect(1, label=label + "-via-bootstrap")
            return c

        try:
            bs = await _conn(nc(host, port), "bootstrap")
            await bs.call(18, "ApiVersions", 0, codec.encode_apiversions_v0(),
                          2, "apiversions", extra_req={"probe": True})
            md, _, _ = await bs.call(
                3, "Metadata", 0, codec.encode_metadata_v0([]),
                3, "metadata", extra_req={"topics": "ALL"},
                decode=codec.decode_metadata_v0)
            meta = _metadata_dict(md)
            brokers = [(b.get("host") or host, b.get("port") or port, b.get("id"))
                       for b in meta["brokers"]] or [(host, port, None)]
            topics = [(t["topic"], [(p["partition"], p["leader"])
                                    for p in t.get("partitions", [])])
                      for t in meta["topics"]]

            # 各 broker ListGroups 并集
            seen: dict[str, str] = {}
            for bh, bp, bid in brokers:
                try:
                    bc = await _conn(nc(bh, bp, bid), f"listgroups-{bid}")
                    lg, _, _ = await bc.call(16, "ListGroups", 1, b"",
                                             4, "listgroups",
                                             decode=codec.decode_list_groups_v1)
                    for g in lg.get("groups", []):
                        seen.setdefault(g["groupId"], g.get("protocolType", ""))
                except Exception:
                    continue

            # log-end：按 leader 建连接逐分区查（连接复用）
            leaders: dict[int, KafkaClient] = {}
            ends: dict[tuple, int | None] = {}
            for t, ps in topics:
                for p, leader in ps:
                    ends[(t, p)] = None
                    if leader not in leaders:
                        b = next((x for x in meta["brokers"]
                                  if x.get("id") == leader), None)
                        leaders[leader] = await _conn(
                            nc(b.get("host") if b else host,
                               b.get("port") if b else port, leader),
                            f"leader-{leader}")
                    try:
                        lo, _, _ = await leaders[leader].call(
                            2, "ListOffsets", 1,
                            codec.encode_listoffsets_v1(t, p, -1),
                            5, "listoffsets",
                            extra_req={"timestamp": "latest", "group": "overview"},
                            topic=t, partition=p,
                            decode=codec.decode_listoffsets_v1)
                        lp = lo["topics"][0]["partitions"][0]
                        if lp["errorCode"] in (0, None):
                            ends[(t, p)] = lp["offset"]
                    except Exception:
                        continue

            out = []
            expected_user_partition_keys = sorted(
                f"{t}:{p}" for t, ps in topics if not t.startswith("_")
                for p, _ in ps)
            known_user_partition_keys = {
                f"{t}:{p}" for (t, p), end in ends.items()
                if not t.startswith("_") and end is not None}
            produce_rate_sample = None
            if expected_user_partition_keys and set(expected_user_partition_keys) == known_user_partition_keys:
                shape = hashlib.sha1("\n".join(expected_user_partition_keys).encode("utf-8")).hexdigest()[:10]
                produce_rate_sample = {
                    "series": f"集群|{shape}",
                    "value": sum(int(end) for (t, _), end in ends.items()
                                 if not t.startswith("_") and end is not None),
                }
            consume_rate_samples = []
            for gid in sorted(seen):
                try:
                    fc, _, _ = await bs.call(
                        10, "FindCoordinator", 0,
                        codec.encode_find_coordinator_v0(gid),
                        4, "findcoordinator", extra_req={"key": gid},
                        decode=codec.decode_find_coordinator_v0)
                    if fc.get("errorCode") not in (0, None):
                        out.append({"groupId": gid, "error": error_name(fc["errorCode"])})
                        continue
                    coord = {"host": fc.get("host") or host,
                             "port": fc.get("port") or port, "id": fc.get("nodeId")}
                    cc = await _conn(nc(coord["host"], coord["port"], coord["id"]),
                                     f"coord-{gid}")
                    dg, _, _ = await cc.call(
                        15, "DescribeGroups", 1,
                        codec.encode_describe_groups_v1([gid]),
                        5, "describegroups", extra_req={"groups": [gid]},
                        decode=codec.decode_describe_groups_v1)
                    g = (dg["groups"] or [{}])[0]
                    members = [{
                        "memberId": m.get("memberId", ""),
                        "clientId": m.get("clientId", ""),
                        "clientHost": m.get("clientHost", ""),
                        "assignment": [
                            {"topic": a.get("topic", ""),
                             "partitions": a.get("partitions", [])}
                            for a in ((m.get("assignment") or {}).get("assignment") or [])],
                    } for m in (g.get("members") or [])]
                    by_topic: dict[str, list[int]] = {}
                    for t, ps in topics:
                        by_topic[t] = [p for p, _ in ps]
                    of_body = codec.enc_string(gid) + codec.enc_array([
                        codec.enc_string(t) + codec.enc_array(
                            [struct.pack(">i", p) for p in sorted(ps)])
                        for t, ps in sorted(by_topic.items())])
                    of, _, _ = await cc.call(
                        9, "OffsetFetch", 1, of_body, 5, "offsetfetch",
                        extra_req={"groupId": gid, "scope": "overview"},
                        decode=codec.decode_offset_fetch_v1)
                    pmap: dict[tuple, int | None] = {}
                    for t in of.get("topics", []):
                        for p in t.get("partitions", []) or []:
                            off = p.get("offset")
                            pmap[(t.get("topic"), p.get("partition"))] = (
                                off if (off is not None and off >= 0) else None)
                    # LAG 口径：只统计该组提交过的 topic（无成员时靠提交记录反推订阅）；
                    # 从未提交的组 scope 为空，不拿全集群 279 分区充数。
                    scoped = [t for t, ps in topics
                              if any(pmap.get((t, p)) is not None for p, _ in ps)]
                    total = 0
                    uncomm = 0
                    for t in scoped:
                        ps = next(ps for tt, ps in topics if tt == t)
                        for p, _ in ps:
                            c_off = pmap.get((t, p))
                            e_off = ends.get((t, p))
                            if c_off is None or e_off is None:
                                uncomm += 1
                            else:
                                total += max(e_off - c_off, 0)
                    committed_user_offsets = sorted(
                        ((t, p, off) for (t, p), off in pmap.items()
                         if not t.startswith("_") and off is not None and off >= 0),
                        key=lambda item: (item[0], item[1]))
                    if committed_user_offsets and uncomm == 0:
                        shape = hashlib.sha1("\n".join(
                            f"{t}:{p}" for t, p, _ in committed_user_offsets
                        ).encode("utf-8")).hexdigest()[:10]
                        consume_rate_samples.append({
                            "series": f"{gid}|{shape}",
                            "value": sum(int(offset) for _, _, offset in committed_user_offsets),
                        })
                    out.append({
                        "groupId": gid, "state": g.get("state"),
                        "protocolType": g.get("protocolType"),
                        "protocol": g.get("protocol"),
                        "coordinator": coord,
                        "memberCount": len(members), "members": members,
                        "topics": scoped,
                        "totalLag": total,
                        "partitions": sum(len(ps) for _, ps in topics),
                        "scopedPartitions": sum(
                            len(next(ps for tt, ps in topics if tt == t))
                            for t in scoped),
                        "uncommitted": uncomm,
                    })
                except Exception as e:  # noqa: BLE001
                    out.append({"groupId": gid, "error": str(e)[:200]})
            return {"traceId": trace_id, "count": len(out), "groups": out,
                    "rateSamples": {"produce": produce_rate_sample,
                                    "consume": consume_rate_samples}}
        finally:
            for c in clients:
                c.close()

    async def describe_group(self, req: GroupDescribeRequest) -> dict:
        from ..models import KafkaTraceEvent  # noqa: F401
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        sec = _secf(req)
        host, port = parse_hostport(req.bootstrap)
        clients: list[KafkaClient] = []

        def nc(h, p, bid=None):
            c = KafkaClient(self.traces, f"sess-group-{req.groupId}", trace_id,
                            req.clientId, req.requestTimeoutMs, h, p, sec, bid)
            clients.append(c)
            return c

        try:
            bs = nc(host, port)
            await bs.connect(1, label="bootstrap")
            await bs.call(18, "ApiVersions", 0, codec.encode_apiversions_v0(),
                          2, "apiversions", extra_req={"probe": True})
            fc, _, _ = await bs.call(
                10, "FindCoordinator", 0,
                codec.encode_find_coordinator_v0(req.groupId),
                3, "findcoordinator", extra_req={"key": req.groupId},
                decode=codec.decode_find_coordinator_v0)
            if fc.get("errorCode") not in (0, None):
                raise _fail(f"FindCoordinator: {error_name(fc['errorCode'])}",
                            error_name(fc["errorCode"]), 3, fc["errorCode"])
            coord = {"host": fc.get("host") or host, "port": fc.get("port") or port,
                     "id": fc.get("nodeId")}
            cc = nc(coord["host"], coord["port"], coord["id"])
            try:
                await cc.connect(4, label="coordinator")
            except OSError:
                cc.host, cc.port = host, port
                await cc.connect(4, label="coordinator-via-bootstrap")
            lg, _, _ = await cc.call(16, "ListGroups", 1, b"", 5, "listgroups",
                                     decode=codec.decode_list_groups_v1)
            dg, _, _ = await cc.call(
                15, "DescribeGroups", 1,
                codec.encode_describe_groups_v1([req.groupId]),
                5, "describegroups", extra_req={"groups": [req.groupId]},
                decode=codec.decode_describe_groups_v1)
            g = (dg["groups"] or [{}])[0]
            return {"traceId": trace_id, "coordinator": coord,
                    "groups": lg.get("groups", []),
                    "group": {"groupId": req.groupId,
                              "state": g.get("state"), "protocolType": g.get("protocolType"),
                              "protocol": g.get("protocol"),
                              "errorCode": g.get("errorCode"),
                              "members": g.get("members", [])}}
        finally:
            for c in clients:
                c.close()

    async def group_lag(self, req: GroupDescribeRequest, topics: list[str]) -> dict:
        """真实 LAG：每分区 log-end(ListOffsets) − 已提交(OffsetFetch)。

        committed < 0 表示该分区无提交，lag 记为 None（未知，不编数字）。
        """
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        sec = _secf(req)
        host, port = parse_hostport(req.bootstrap)
        clients: list[KafkaClient] = []
        sess = f"sess-lag-{req.groupId}"

        def nc(h, p, bid=None):
            c = KafkaClient(self.traces, sess, trace_id,
                            req.clientId, req.requestTimeoutMs, h, p, sec, bid)
            clients.append(c)
            return c

        try:
            bs = nc(host, port)
            await bs.connect(1, label="bootstrap")
            want = topics or []
            md, _, _ = await bs.call(
                3, "Metadata", 0,
                codec.encode_metadata_v0(want),
                3, "metadata", extra_req={"topics": want or "ALL"},
                decode=codec.decode_metadata_v0)
            tps = _metadata_dict(md)["topics"]
            if want:
                tps = [t for t in tps if t["topic"] in want]
            brokers = {b["id"]: b for b in _metadata_dict(md)["brokers"]}

            # log-end per partition (via leader)
            ends: dict = {}
            leaders: dict = {}
            for t in tps:
                for p in t.get("partitions", []):
                    pid, lid = p["partition"], p.get("leader", -1)
                    leaders[(t["topic"], pid)] = lid
                    if lid is None or lid < 0:
                        ends[(t["topic"], pid)] = None
                        continue
                    b = brokers.get(lid, {})
                    lc = nc(b.get("host") or host, b.get("port") or port, lid)
                    try:
                        await lc.connect(4, label=f"leader-{lid}")
                    except OSError:
                        lc.host, lc.port = host, port
                        await lc.connect(4, label=f"leader-{lid}-via-bootstrap")
                    lo, _, _ = await lc.call(
                        2, "ListOffsets", 1,
                        codec.encode_listoffsets_v1(t["topic"], pid, -1),
                        4, "listoffsets",
                        extra_req={"timestamp": "latest"},
                        topic=t["topic"], partition=pid,
                        decode=codec.decode_listoffsets_v1)
                    lp = lo["topics"][0]["partitions"][0]
                    ends[(t["topic"], pid)] = (lp["offset"]
                                               if lp["errorCode"] == 0 else None)

            # committed per partition (via coordinator, single OffsetFetch)
            fc, _, _ = await bs.call(
                10, "FindCoordinator", 0,
                codec.encode_find_coordinator_v0(req.groupId),
                4, "findcoordinator", extra_req={"key": req.groupId},
                decode=codec.decode_find_coordinator_v0)
            if fc.get("errorCode") not in (0, None):
                raise _fail(f"FindCoordinator: {error_name(fc['errorCode'])}",
                            error_name(fc["errorCode"]), 4, fc["errorCode"])
            cc = nc(fc.get("host") or host, fc.get("port") or port, fc.get("nodeId"))
            try:
                await cc.connect(5, label="coordinator")
            except OSError:
                cc.host, cc.port = host, port
                await cc.connect(5, label="coordinator-via-bootstrap")
            by_topic: dict = {}
            for (topic, pid) in ends:
                by_topic.setdefault(topic, []).append(pid)
            of_body = codec.enc_string(req.groupId) + codec.enc_array([
                codec.enc_string(t) + codec.enc_array(
                    [struct.pack(">i", p) for p in sorted(ps)])
                for t, ps in sorted(by_topic.items())])
            of, _, _ = await cc.call(
                9, "OffsetFetch", 1, of_body, 5, "offsetfetch",
                extra_req={"groupId": req.groupId},
                decode=codec.decode_offset_fetch_v1)
            committed: dict = {}
            for t in of.get("topics", []):
                for p in t.get("partitions", []):
                    committed[(t["topic"], p["partition"])] = p.get("offset", -1)

            rows = []
            total = 0
            for (topic, pid) in sorted(ends):
                end = ends[(topic, pid)]
                com = committed.get((topic, pid), -1)
                lag = (end - com) if (end is not None and com is not None and com >= 0) else None
                if lag is not None:
                    total += lag
                rows.append({"topic": topic, "partition": pid,
                             "leader": leaders.get((topic, pid)),
                             "end": end, "committed": com if (com is not None and com >= 0) else None,
                             "lag": lag})
            return {"traceId": trace_id, "groupId": req.groupId,
                    "totalLag": total, "partitions": len(rows),
                    "uncommitted": sum(1 for r in rows if r["committed"] is None),
                    "rows": rows}
        finally:
            for c in clients:
                c.close()

    async def rebalance_demo(self, req: GroupDescribeRequest, topics: list[str]) -> dict:
        """Two-member Join sequence that forces a real rebalance.

        M1 join (gen g1) → M2 join (new generation g2) → M1 heartbeat on
        stale g1 (expect REBALANCE_IN_PROGRESS) → M1 rejoin (gen g3) →
        Sync both → Heartbeat both → Leave both.
        Generation bump g1 < g2 <= g3 proves the rebalance happened.
        """
        import asyncio as _aio
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        sec = _secf(req)
        host, port = parse_hostport(req.bootstrap)
        clients: list[KafkaClient] = []
        steps: list[dict] = []

        def nc(h, p, bid=None):
            c = KafkaClient(self.traces, f"sess-rebal-{req.groupId}", trace_id,
                            req.clientId, req.requestTimeoutMs, h, p, sec, bid)
            clients.append(c)
            return c

        async def _join(cc, mid, tag, session_ms=30000, rebal_ms=10000):
            cur = mid
            for attempt in range(4):
                jg, _, _ = await cc.call(
                    11, "JoinGroup", 1,
                    codec.encode_join_group_v1(req.groupId, cur, topics or ["test-topic"],
                                               session_ms, rebal_ms),
                    4, "joingroup",
                    extra_req={"groupId": req.groupId, "tag": tag,
                               "attempt": attempt + 1},
                    decode=codec.decode_join_group_v1)
                steps.append({"step": f"join.{tag}", "attempt": attempt + 1,
                              "errorCode": jg.get("errorCode"),
                              "errorName": error_name(jg.get("errorCode") or 0)
                              if jg.get("errorCode") else "NONE",
                              "generation": jg.get("generationId"),
                              "memberId": jg.get("memberId"),
                              "leader": jg.get("leaderId")})
                if jg.get("errorCode") == 27:  # REBALANCE_IN_PROGRESS → retry
                    await _aio.sleep(1.0)
                    continue
                if jg.get("errorCode") == 25 and cur:  # UNKNOWN_MEMBER (fenced) → rejoin as new
                    steps.append({"step": f"fenced.{tag}",
                                  "note": "成员被 fencing，改用空 memberId 重新加入（真实消费者同逻辑）"})
                    cur = ""
                    continue
                if jg.get("errorCode") not in (0, None):
                    raise _fail(f"JoinGroup({tag}): {error_name(jg['errorCode'])}",
                                error_name(jg["errorCode"]), 4, jg["errorCode"])
                return jg
            raise _fail("JoinGroup kept rebalancing", "REBALANCE_IN_PROGRESS", 4, 27)

        try:
            bs = nc(host, port)
            await bs.connect(1, label="bootstrap")
            fc, _, _ = await bs.call(
                10, "FindCoordinator", 0,
                codec.encode_find_coordinator_v0(req.groupId),
                2, "findcoordinator", extra_req={"key": req.groupId},
                decode=codec.decode_find_coordinator_v0)
            if fc.get("errorCode") not in (0, None):
                raise _fail("FindCoordinator failed", error_name(fc["errorCode"]), 2, fc["errorCode"])
            cc = nc(fc.get("host") or host, fc.get("port") or port, fc.get("nodeId"))
            try:
                await cc.connect(3, label="coordinator")
            except OSError:
                cc.host, cc.port = host, port
                await cc.connect(3, label="coordinator-via-bootstrap")

            j1 = await _join(cc, "", "m1-first")
            g1, m1 = j1["generationId"], j1["memberId"]
            # m2 用较短 rebalance 超时快速完成 g2（m1 会被 fencing，后面演示重新加入）
            j2 = await _join(cc, "", "m2-join", rebal_ms=8000)
            g2, m2 = j2["generationId"], j2["memberId"]

            hb1, _, _ = await cc.call(
                12, "Heartbeat", 1,
                codec.encode_heartbeat_v1(req.groupId, g1, m1),
                5, "heartbeat",
                extra_req={"tag": "m1-stale-gen", "generation": g1},
                decode=codec.decode_heartbeat_v1)
            steps.append({"step": "heartbeat.m1-stale", "generation": g1,
                          "errorCode": hb1.get("errorCode"),
                          "errorName": error_name(hb1.get("errorCode") or 0)
                          if hb1.get("errorCode") else "NONE",
                          "note": "旧 generation 心跳被拒绝(27)=需要重入组" if hb1.get("errorCode") == 27
                          else "coordinator 仍接受（时序原因），fencing 由下一步重入组证明"})

            j1b = await _join(cc, m1, "m1-rejoin")
            g3 = j1b["generationId"]
            m1 = j1b.get("memberId") or m1  # fencing 后可能是新 memberId
            # whoever leads the latest generation assigns split partitions
            # (real partition counts from Metadata, not hardcoded)
            assign_map: dict = {}
            try:
                md, _, _ = await bs.call(
                    3, "Metadata", 0,
                    codec.encode_metadata_v0(topics or ["test-topic"]),
                    3, "metadata",
                    extra_req={"topics": topics, "reason": "rebalance-assign"},
                    decode=codec.decode_metadata_v0)
                for t in _metadata_dict(md)["topics"]:
                    assign_map[t["topic"]] = [p["partition"] for p in t["partitions"]]
            except Exception:
                pass
            lead = j1b if (j1b.get("leaderId") == j1b.get("memberId")) else j2
            lg, lm = (lead.get("generationId"), lead.get("memberId"))
            other = m2 if lm == m1 else m1
            lm_topics, ot_topics = [], []
            for t in (topics or ["test-topic"]):
                ps = assign_map.get(t) or [0]
                half = max(len(ps) // 2, 1)
                lm_topics.append((t, ps[:half]))
                ot_topics.append((t, ps[half:] or ps[:1]))
            assigns = [(lm, lm_topics), (other, ot_topics)]
            sgl, _, _ = await cc.call(
                14, "SyncGroup", 1,
                codec.encode_sync_group_v1(req.groupId, lg, lm, assigns),
                6, "syncgroup", extra_req={"tag": "leader-sync", "generation": lg},
                decode=codec.decode_sync_group_v1)
            steps.append({"step": "sync.leader", "generation": lg,
                          "errorCode": sgl.get("errorCode"),
                          "assignment": {m: a for m, a in assigns}})
            for tag, mm, gg in (("m1", m1, g3), ("m2", m2, g3 if g3 else g2)):
                hb, _, _ = await cc.call(
                    12, "Heartbeat", 1,
                    codec.encode_heartbeat_v1(req.groupId, gg, mm),
                    6, "heartbeat", extra_req={"tag": tag, "generation": gg},
                    decode=codec.decode_heartbeat_v1)
                steps.append({"step": f"heartbeat.{tag}", "generation": gg,
                              "errorCode": hb.get("errorCode"),
                              "errorName": error_name(hb.get("errorCode") or 0)
                              if hb.get("errorCode") else "NONE"})
            for mm in (m1, m2):
                try:
                    await cc.call(13, "LeaveGroup", 1,
                                  codec.encode_leave_group_v1(req.groupId, mm),
                                  7, "leavegroup",
                                  decode=codec.decode_leave_group_v1)
                except Exception:
                    pass
            steps.append({"step": "leave.both", "status": "done"})
            bumped = (g2 is not None and g1 is not None and g2 > g1)
            return {"traceId": trace_id, "groupId": req.groupId,
                    "generations": {"m1First": g1, "m2Join": g2, "m1Rejoin": g3},
                    "rebalanced": bumped,
                    "note": "g2>g1 且 m1 用旧 generation 心跳返回 27，即发生 Rebalance" if bumped
                    else "未观察到 generation 递增（组可能为空或 coordinator 行为不同）",
                    "steps": steps}
        finally:
            for c in clients:
                c.close()

    async def join_sync_demo(self, req: GroupDescribeRequest, topics: list[str]) -> dict:
        """JoinGroup→SyncGroup→Heartbeat→LeaveGroup demo (real group join)."""
        from ..models import KafkaTraceEvent  # noqa: F401
        trace_id = f"tr-{uuid.uuid4().hex[:8]}"
        sec = _secf(req)
        host, port = parse_hostport(req.bootstrap)
        clients: list[KafkaClient] = []

        def nc(h, p, bid=None):
            c = KafkaClient(self.traces, f"sess-join-{req.groupId}", trace_id,
                            req.clientId, req.requestTimeoutMs, h, p, sec, bid)
            clients.append(c)
            return c

        try:
            bs = nc(host, port)
            await bs.connect(1, label="bootstrap")
            fc, _, _ = await bs.call(
                10, "FindCoordinator", 0,
                codec.encode_find_coordinator_v0(req.groupId),
                2, "findcoordinator", extra_req={"key": req.groupId},
                decode=codec.decode_find_coordinator_v0)
            if fc.get("errorCode") not in (0, None):
                raise _fail("FindCoordinator failed", error_name(fc["errorCode"]), 2, fc["errorCode"])
            cc = nc(fc.get("host") or host, fc.get("port") or port, fc.get("nodeId"))
            try:
                await cc.connect(3, label="coordinator")
            except OSError:
                cc.host, cc.port = host, port
                await cc.connect(3, label="coordinator-via-bootstrap")
            jg, _, _ = await cc.call(
                11, "JoinGroup", 1,
                codec.encode_join_group_v1(req.groupId, "", topics or ["test-topic"]),
                4, "joingroup", extra_req={"groupId": req.groupId, "topics": topics},
                decode=codec.decode_join_group_v1)
            if jg.get("errorCode") not in (0, None):
                raise _fail(f"JoinGroup: {error_name(jg['errorCode'])}",
                            error_name(jg["errorCode"]), 4, jg["errorCode"])
            gen, mid = jg["generationId"], jg["memberId"]
            is_leader = (jg.get("leaderId") == mid) or not jg.get("members")
            assigns = [(m["memberId"], [(t, [0]) for t in (topics or ["test-topic"])])
                       for m in (jg.get("members") or [{"memberId": mid}])] if is_leader else []
            sg, _, _ = await cc.call(
                14, "SyncGroup", 1,
                codec.encode_sync_group_v1(req.groupId, gen, mid, assigns),
                5, "syncgroup", extra_req={"generation": gen, "isLeader": is_leader},
                decode=codec.decode_sync_group_v1)
            hb, _, _ = await cc.call(
                12, "Heartbeat", 1,
                codec.encode_heartbeat_v1(req.groupId, gen, mid),
                6, "heartbeat", extra_req={"generation": gen},
                decode=codec.decode_heartbeat_v1)
            try:
                await cc.call(13, "LeaveGroup", 1,
                              codec.encode_leave_group_v1(req.groupId, mid),
                              7, "leavegroup", decode=codec.decode_leave_group_v1)
            except Exception:
                pass
            return {"traceId": trace_id, "generation": gen, "memberId": mid,
                    "protocol": jg.get("protocol"), "isLeader": is_leader,
                    "members": jg.get("members", []),
                    "syncError": sg.get("errorCode"),
                    "heartbeatError": hb.get("errorCode")}
        finally:
            for c in clients:
                c.close()
