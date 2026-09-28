"""专业压测执行器：轻量 Produce 循环，无 Trace/Session 开销，可支撑大吞吐。

与 ProducerEngine.run 的区别：
- 不创建 Session、不写 Trace 事件（NullTraces 吸收握手阶段的少量 emit）
- Produce 热循环走原始帧收发，不做 HEX dump，单连接复用 + 多 worker 并发
- 支持按条数 / 按时长两种模式
"""
from __future__ import annotations
import asyncio
import random
import struct
import string
import time

from ..models import SecurityConfig
from ..network.client import KafkaClient
from ..network.tcp import TcpConnector, parse_hostport
from ..protocol import codec
from .shared import _metadata_dict


class _NullTraces:
    def append(self, *args, **kwargs) -> None:
        pass


def _sec_of(body: dict) -> SecurityConfig:
    return SecurityConfig(
        securityProtocol=body.get("securityProtocol", "PLAINTEXT"),
        saslMechanism=body.get("saslMechanism", "PLAIN"),
        saslUsername=body.get("saslUsername", ""),
        saslPassword=body.get("saslPassword", ""),
    )


def _rand_value(n: int) -> bytes:
    if n <= 64:
        return "".join(random.choices(string.ascii_letters + string.digits, k=n)).encode()
    return bytes(random.getrandbits(8) for _ in range(n))


async def _setup_leader(body: dict, sec: SecurityConfig, topic: str):
    """建 bootstrap 连接 → Metadata → 选分区/Leader → 连 Leader，返回 (clients, leader_client, partition)。"""
    clients: list[KafkaClient] = []
    host, port = parse_hostport(body.get("bootstrap", "") or "localhost:19092")

    def new_client(h: str, p: int, bid=None) -> KafkaClient:
        c = KafkaClient(_NullTraces(), "load", "load", "kafka-load-01",
                        int(body.get("requestTimeoutMs", 10000)), h, p, sec, bid)
        clients.append(c)
        return c

    bs = new_client(host, port)
    await bs.connect(0, label="load-bootstrap")
    meta_dec, _, _ = await bs.call(
        3, "Metadata", 0, codec.encode_metadata_v0([topic]),
        0, "metadata", extra_req={"topics": [topic]}, topic=topic,
        decode=codec.decode_metadata_v0)
    meta = _metadata_dict(meta_dec)
    tmeta = next((x for x in meta["topics"] if x["topic"] == topic), None)
    if tmeta is None or tmeta.get("error", 0) != 0 or not tmeta.get("partitions"):
        raise RuntimeError(f"metadata unavailable for '{topic}'")
    parts = tmeta["partitions"]
    part = body.get("partition", "auto")
    if str(part).lower() == "auto":
        healthy = [p for p in parts if p.get("error", 0) == 0] or parts
        chosen = random.choice(healthy)
    else:
        chosen = next((p for p in parts if p["partition"] == int(part)), None)
        if chosen is None:
            raise RuntimeError(f"partition {part} not found")
    leader_id = chosen["leader"]
    brokers = meta["brokers"]
    leader = next((b for b in brokers if b.get("id") == leader_id), None)
    lhost, lport = (leader["host"], leader["port"]) if leader else (host, port)
    lc = new_client(lhost, lport, leader_id)
    try:
        await lc.connect(0, label="load-leader")
    except OSError:
        lc.host, lc.port = host, port
        await lc.connect(0, label="load-leader-via-bootstrap")
    return clients, lc, bs, chosen["partition"]


async def _raw_produce(lc: KafkaClient, topic: str, partition: int,
                       key: bytes, val: bytes, acks: int, timeout_ms: int) -> tuple[int, int]:
    """一次 Produce v0，返回 (baseOffset, 延迟ms)。失败抛错。"""
    body = codec.encode_produce_v0_multi(topic, partition, [(key, val)],
                                         acks=max(acks, 1), timeout_ms=timeout_ms)
    lc.corr += 1
    frame = codec.build_request(0, 0, lc.corr, lc.client_id, body)
    t = time.monotonic()
    raw = await asyncio.to_thread(TcpConnector.request, lc.conn, frame[4:])
    lat_ms = (time.monotonic() - t) * 1000
    (rcorr,) = struct.unpack_from(">i", raw, 0)
    if rcorr != lc.corr:
        raise RuntimeError(f"correlation mismatch {rcorr}!={lc.corr}")
    dec = codec.decode_produce_v0(raw[4:])
    topics = dec.get("topics") if isinstance(dec, dict) else None
    if not topics:
        raise RuntimeError("empty produce response")
    p = topics[0]["partitions"][0]
    if p.get("errorCode", 0) != 0:
        e = RuntimeError(f"produce error {p.get('errorCode')}")
        e.kafka_code = p.get("errorCode")  # type: ignore[attr-defined]
        raise e
    return p.get("baseOffset", 0), lat_ms


async def run_load_job(job: dict, body: dict) -> None:
    topic = body.get("topic", "test-topic")
    sec = _sec_of(body)
    acks = codec.pick_acks(body.get("acks", "all"))
    timeout_ms = int(body.get("requestTimeoutMs", 10000))
    mode = body.get("mode", "count")
    target = int(body.get("total", 100))
    duration_target = float(body.get("durationSec", 60))
    conc = max(1, int(body.get("concurrency", 4)))
    vsize = max(1, int(body.get("valueSize", 128)))

    t_start = time.monotonic()
    deadline = t_start + duration_target if mode == "duration" else None
    state = {"sent": 0, "ok": 0, "fail": 0, "bytes": 0}
    lat: list[float] = []
    errs: dict[str, int] = {}
    per_sec: dict[int, int] = {}
    lock = asyncio.Lock()

    async def worker(_idx: int):
        try:
            clients, lc, bs, partition = await _setup_leader(body, sec, topic)
        except Exception as e:  # noqa: BLE001
            async with lock:
                errs[f"setup: {str(e)[:100]}"] = errs.get(f"setup: {str(e)[:100]}", 0) + 1
            job["setupError"] = str(e)[:300]
            return
        try:
            while True:
                async with lock:
                    if mode == "count":
                        if state["sent"] >= target:
                            return
                        state["sent"] += 1
                    else:
                        if time.monotonic() >= deadline:
                            return
                        state["sent"] += 1
                key = f"k-{random.randint(100000, 999999)}".encode()
                val = _rand_value(vsize)
                try:
                    _, dt = await _raw_produce(lc, topic, partition, key, val, acks, timeout_ms)
                except Exception as e:  # noqa: BLE001
                    code = getattr(e, "kafka_code", None)
                    if code == 6:  # NotLeader → 刷新一次 Metadata 重试
                        try:
                            clients2, lc2, _, part2 = await _setup_leader(body, sec, topic)
                            clients.extend(clients2)
                            lc = lc2
                            partition = part2
                            _, dt = await _raw_produce(lc, topic, partition, key, val, acks, timeout_ms)
                        except Exception as e2:  # noqa: BLE001
                            async with lock:
                                state["fail"] += 1
                                k = str(e2)[:80]
                                errs[k] = errs.get(k, 0) + 1
                            continue
                    else:
                        async with lock:
                            state["fail"] += 1
                            k = str(e)[:80]
                            errs[k] = errs.get(k, 0) + 1
                        continue
                async with lock:
                    state["ok"] += 1
                    state["bytes"] += len(key) + len(val)
                    lat.append(dt)
                    elapsed = int(time.monotonic() - t_start)
                    per_sec[elapsed] = per_sec.get(elapsed, 0) + 1
                    job["done"] = state["ok"] + state["fail"]
        finally:
            for c in clients:
                try:
                    c.close()
                except Exception:
                    pass

    try:
        await asyncio.gather(*[worker(i) for i in range(conc)])
        if job.get("setupError") and state["sent"] == 0:
            job.update({"status": "failed", "error": job["setupError"]})
            return
        dur = max(0.001, time.monotonic() - t_start)
        total = state["ok"] + state["fail"]
        lat_sorted = sorted(lat)
        def perc(p: float):
            if not lat_sorted:
                return None
            i = min(len(lat_sorted) - 1, max(0, int(len(lat_sorted) * p / 100)))
            return round(float(lat_sorted[i]), 1)
        job.update({
            "status": "done",
            "ok": state["ok"], "fail": state["fail"],
            "durationS": round(dur, 2),
            "msgPerS": round(total / dur, 1),
            "mbPerS": round(state["bytes"] / dur / 1048576, 3),
            "avgMs": round(sum(lat_sorted) / len(lat_sorted), 1) if lat_sorted else None,
            "p50Ms": perc(50), "p95Ms": perc(95), "p99Ms": perc(99),
            "maxMs": round(float(lat_sorted[-1]), 1) if lat_sorted else None,
            "errors": sorted(errs.items(), key=lambda x: -x[1])[:10],
            "perSec": [{"sec": k, "n": per_sec[k]} for k in sorted(per_sec)],
        })
    except Exception as e:  # noqa: BLE001
        job.update({"status": "failed", "error": str(e)[:300]})


def build_report(job: dict) -> str:
    """压测报告（Markdown）。"""
    cfg = [
        ("Topic", job.get("topic", "")),
        ("Bootstrap", job.get("bootstrap", "")),
        ("模式", "按时长" if job.get("mode") == "duration" else "按条数"),
        ("目标", f"{job.get('durationTarget', '')}s" if job.get("mode") == "duration" else f"{job.get('total', '')} 条"),
        ("并发 worker", job.get("concurrency", "")),
        ("Value 大小", f"{job.get('valueSize', '')} B"),
        ("分区/acks", "auto / all"),
    ]
    res = [
        ("发送总数", job.get("done", 0)),
        ("成功", job.get("ok", 0)),
        ("失败", job.get("fail", 0)),
        ("成功率", f"{(job.get('ok', 0) / max(1, job.get('done', 0)) * 100):.2f}%"),
        ("总耗时", f"{job.get('durationS', 0)}s"),
        ("吞吐", f"{job.get('msgPerS', 0)} msg/s"),
        ("带宽", f"{job.get('mbPerS', 0)} MB/s"),
        ("平均延迟", f"{job.get('avgMs', '—')}ms"),
        ("P50", f"{job.get('p50Ms', '—')}ms"),
        ("P95", f"{job.get('p95Ms', '—')}ms"),
        ("P99", f"{job.get('p99Ms', '—')}ms"),
        ("最大延迟", f"{job.get('maxMs', '—')}ms"),
    ]
    lines = ["# Kafka 压测报告", ""]
    lines.append(f"- 任务 `{job.get('id', '')}` · 状态 {job.get('status', '')}")
    lines += ["", "## 配置", ""]
    lines += [f"- {k}: {v}" for k, v in cfg]
    lines += ["", "## 结果", ""]
    lines += [f"- {k}: {v}" for k, v in res]
    errs = job.get("errors") or []
    lines += ["", "## 错误分布", ""]
    lines += [f"- ×{n}: {m}" for m, n in errs] if errs else ["- 零失败"]
    per = job.get("perSec") or []
    if per:
        lines += ["", "## 每秒吞吐（sec: count）", "", "```", ", ".join(f"{x['sec']}:{x['n']}" for x in per), "```"]
    lines += ["", "> 由 Kafka Client Lab 压测模块生成，Produce v0 真实链路。", ""]
    return "\n".join(lines)
