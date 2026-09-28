"""Topic 消息浏览服务：只读 Consumer 语义，临时连接、查完即关。

保证：
- 不创建 Consumer Group，不 Join/Sync，不 OffsetCommit，不改任何位点
- 连接参数全部来自 ClusterManager 纳管配置，调用方只传 cid
- 报错全部转为中文短提示，不向外抛堆栈/密码
"""
from __future__ import annotations
import asyncio
import struct
import time
from datetime import datetime, timezone

from ..models import SecurityConfig, error_name
from ..network.client import KafkaClient
from ..network.tcp import TcpConnector, parse_hostport
from ..protocol import codec
from ..producer.shared import _metadata_dict
from .config import LIMITS


class _NullTraces:
    def append(self, *args, **kwargs) -> None:
        pass


class BrowseError(Exception):
    def __init__(self, msg: str, code: int | None = None):
        super().__init__(msg)
        self.code = code


def _friendly(prefix: str, e: Exception) -> BrowseError:
    msg = str(e)
    low = msg.lower()
    if "refused" in low or "unreachable" in low or "timed out" in low or "timeout" in low:
        return BrowseError(f"{prefix}：Broker 不可用（{type(e).__name__}）")
    if "sasl" in low or "auth" in low or "ssl" in low:
        return BrowseError(f"{prefix}：集群认证失败，请检查安全协议与账号")
    return BrowseError(f"{prefix}：{msg[:160]}")


def _sec_of(c) -> SecurityConfig:
    return SecurityConfig(securityProtocol=c.securityProtocol,
                          saslMechanism=c.saslMechanism,
                          saslUsername=c.saslUsername, saslPassword=c.saslPassword)


def _ts_text(ms: int | None) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, OverflowError, ValueError):
        return None


class Browser:
    """单次查询的临时 Consumer 生命周期管理：建连 → 查 → 关。"""

    def __init__(self, bootstrap: str, sec: SecurityConfig, timeout_ms: int = 10000):
        self.bootstrap = bootstrap
        self.sec = sec
        self.timeout_ms = timeout_ms
        self.clients: list[KafkaClient] = []
        host, port = parse_hostport(bootstrap or "localhost:19092")
        self.host, self.port = host, port

    def _nc(self, h: str, p: int, bid=None) -> KafkaClient:
        c = KafkaClient(_NullTraces(), "browse", "browse", "kafka-browse-01",
                        self.timeout_ms, h, p, self.sec, bid)
        self.clients.append(c)
        return c

    def close(self) -> None:
        for c in self.clients:
            try:
                c.close()
            except Exception:
                pass
        self.clients = []

    async def _leader_client(self, topic: str, partition: int):
        bs = self._nc(self.host, self.port)
        await bs.connect(0, label="browse-bootstrap")
        meta_dec, _, _ = await bs.call(
            3, "Metadata", 0, codec.encode_metadata_v0([topic]),
            0, "metadata", extra_req={"topics": [topic]}, topic=topic,
            decode=codec.decode_metadata_v0)
        meta = _metadata_dict(meta_dec)
        tmeta = next((x for x in meta["topics"] if x["topic"] == topic), None)
        if tmeta is None or tmeta.get("error", 0) != 0:
            raise BrowseError(f"Topic 不存在：'{topic}'")
        part = next((p for p in tmeta.get("partitions", []) if p["partition"] == partition), None)
        if part is None:
            raise BrowseError(f"分区不存在：P{partition}")
        lid = part.get("leader", -1)
        if lid is None or lid < 0:
            raise BrowseError(f"P{partition} 暂无 Leader")
        leader = next((b for b in meta["brokers"] if b.get("id") == lid), None)
        lh, lp = (leader["host"], leader["port"]) if leader else (self.host, self.port)
        lc = self._nc(lh, lp, lid)
        try:
            await lc.connect(0, label="browse-leader")
        except OSError:
            lc.host, lc.port = self.host, self.port
            await lc.connect(0, label="browse-leader-via-bootstrap")
        return lc

    async def _raw_fetch(self, lc: KafkaClient, topic: str, partition: int, offset: int):
        """原始 Fetch 帧收发（无 trace/HEX 开销），返回 (messages, highWatermark)。

        直接解析 records 原始字节走 decode_record_batches_full，保留
        headers / timestamp / size；decode_fetch_v4 的裁剪版解码不用在这里。
        """
        from ..models import error_name as _ename
        body = codec.encode_fetch_v4(topic, partition, offset,
                                     max_bytes=LIMITS.fetch_max_bytes,
                                     max_wait_ms=2000, min_bytes=1)
        lc.corr += 1
        frame = codec.build_request(1, 4, lc.corr, lc.client_id, body)
        raw = await asyncio.to_thread(TcpConnector.request, lc.conn, frame[4:])
        (rcorr,) = struct.unpack_from(">i", raw, 0)
        if rcorr != lc.corr:
            raise BrowseError("Broker 响应错乱（correlation 不一致）")
        buf = raw[4:]
        off = 4  # throttle
        tn, off = codec.dec_array_count(buf, off)
        for _ in range(max(tn, 0)):
            t, off = codec.dec_string(buf, off)
            pn, off = codec.dec_array_count(buf, off)
            for _ in range(max(pn, 0)):
                (pid, err) = struct.unpack_from(">ih", buf, off)
                off += 6
                (hw,) = struct.unpack_from(">q", buf, off)
                off += 8
                off += 8  # lastStableOffset
                (ab_n,) = struct.unpack_from(">i", buf, off)
                off += 4 + max(ab_n, 0) * 16
                (rlen,) = struct.unpack_from(">i", buf, off)
                off += 4
                recs = buf[off:off + max(rlen, 0)]
                off += max(rlen, 0)
                if t == topic and pid == partition:
                    if err != 0:
                        raise BrowseError(f"P{partition} 读取失败：{_ename(err)}", code=err)
                    msgs = codec.decode_record_batches_full(recs) if rlen > 0 else []
                    for m in msgs:
                        m["topic"] = topic
                        m["partition"] = partition
                    return msgs, hw
        return [], offset

    async def _offset_for(self, lc: KafkaClient, topic: str, partition: int, timestamp: int) -> int:
        lo, _, _ = await lc.call(
            2, "ListOffsets", 1, codec.encode_listoffsets_v1(topic, partition, timestamp),
            0, "listoffsets", extra_req={"timestamp": timestamp},
            topic=topic, partition=partition, decode=codec.decode_listoffsets_v1)
        p = lo["topics"][0]["partitions"][0]
        if p.get("errorCode", 0) != 0:
            raise BrowseError(f"定位失败：{error_name(p['errorCode'])}")
        return p["offset"]

    async def query(self, topic: str, partitions: list[int], mode: str,
                    offset: int | None, start_ms: int | None, end_ms: int | None,
                    limit: int, key_q: str, val_q: str,
                    resume: dict[int, int] | None = None) -> dict:
        """核心查询：各分区独立推进游标，合并后按 (timestamp, partition, offset) 排序。

        resume: {partition: offset} 断点续查（Load More / 实时轮询用）。
        """
        t_start = time.monotonic()
        scanned = 0
        truncated = False
        per_part: dict[int, list[dict]] = {}
        next_offsets: dict[int, int] = {}
        hw_map: dict[int, int] = {}
        key_q = (key_q or "").strip()
        val_q = (val_q or "").strip()

        for p in partitions:
            lc = await self._leader_client(topic, p)
            hw = await self._offset_for(lc, topic, p, -1)
            hw_map[p] = hw
            lo_earliest = await self._offset_for(lc, topic, p, -2)

            if resume and p in resume:
                cur = max(resume[p], lo_earliest)
            elif mode == "earliest":
                cur = lo_earliest
            elif mode == "offset":
                cur = max(offset or 0, lo_earliest)
                if cur >= hw:
                    per_part[p] = []
                    next_offsets[p] = hw
                    continue
            elif mode == "timestamp":
                cur = await self._offset_for(lc, topic, p, start_ms or 0)
                cur = max(cur, lo_earliest)
            else:  # latest：从最新向前取 limit 条
                cur = max(hw - limit, lo_earliest)

            got: list[dict] = []
            end_bound = (end_ms or 0) if mode == "timestamp" else 0
            while len(got) < limit and cur < hw:
                if time.monotonic() - t_start > LIMITS.max_scan_seconds:
                    truncated = True
                    break
                if scanned >= LIMITS.max_scan_records:
                    truncated = True
                    break
                try:
                    batch, hw_now = await self._raw_fetch(lc, topic, p, cur)
                except BrowseError as be:
                    if getattr(be, "code", None) == 1:  # OFFSET_OUT_OF_RANGE → 从最早可用的继续
                        cur = lo_earliest
                        continue
                    raise
                except Exception as e:  # noqa: BLE001
                    raise _friendly("读取消息失败", e)
                if hw_now is not None:
                    hw = max(hw, hw_now)
                    hw_map[p] = hw
                if not batch:
                    cur = hw
                    break
                for m in batch:
                    scanned += 1
                    if m["offset"] < cur:
                        continue
                    cur = m["offset"] + 1
                    if mode == "timestamp" and end_bound and (m.get("timestamp") or 0) > end_bound:
                        cur = hw
                        break
                    if key_q and key_q not in (m.get("key") or ""):
                        continue
                    if val_q and val_q not in (m.get("value") or ""):
                        continue
                    m.update({"topic": topic, "partition": p,
                              "timestampText": _ts_text(m.get("timestamp"))})
                    got.append(m)
                    if len(got) >= limit:
                        break
                if scanned >= LIMITS.max_scan_records:
                    truncated = True
                    break
            per_part[p] = got
            next_offsets[p] = cur

        merged: list[dict] = []
        for p in partitions:
            merged.extend(per_part.get(p, []))
        merged.sort(key=lambda m: (m.get("timestamp") or 0, m["partition"], m["offset"]))
        merged = merged[:LIMITS.max_result_records]
        has_more = any(next_offsets.get(p, 0) < hw_map.get(p, 0) for p in partitions)
        return {"topic": topic, "total": len(merged), "hasMore": has_more,
                "messages": merged,
                "nextOffsets": {str(k): v for k, v in next_offsets.items()},
                "highWatermarks": {str(k): v for k, v in hw_map.items()},
                "scanned": scanned, "truncated": truncated,
                "limits": {"maxScanRecords": LIMITS.max_scan_records,
                           "maxScanSeconds": LIMITS.max_scan_seconds,
                           "maxResultRecords": LIMITS.max_result_records}}

async def browse(cluster, topic: str, q: dict) -> dict:
    """API 入口：参数校验 + 生命周期管理（建连→查→关）。"""
    from ..models import error_name  # noqa: F401
    mode = (q.get("mode") or "latest").strip().lower()
    if mode not in ("latest", "earliest", "offset", "timestamp"):
        raise BrowseError("mode 非法：latest | earliest | offset | timestamp")
    try:
        limit = int(q.get("limit") or LIMITS.default_limit)
    except (TypeError, ValueError):
        raise BrowseError("limit 非法")
    limit = max(1, min(limit, LIMITS.max_limit))

    partition = q.get("partition", "all")
    if isinstance(partition, str) and partition.strip().lower() == "all":
        parts: list[int] | None = None  # 查 metadata 拿全部分区
    else:
        try:
            parts = [int(partition)]
        except (TypeError, ValueError):
            raise BrowseError("partition 非法")

    offset = q.get("offset")
    if offset not in (None, ""):
        try:
            offset = int(offset)
            if offset < 0:
                raise BrowseError("offset 不能为负")
        except (TypeError, ValueError):
            raise BrowseError("offset 非法")
    else:
        offset = None

    def _ts(v, name: str) -> int | None:
        if v in (None, ""):
            return None
        s = str(v).strip()
        try:
            if "T" in s or "-" in s:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            return int(s)
        except (ValueError, OverflowError):
            raise BrowseError(f"{name} 时间格式非法")
    start_ms = _ts(q.get("startTime"), "开始时间")
    end_ms = _ts(q.get("endTime"), "结束时间")
    if mode == "timestamp" and start_ms is None:
        raise BrowseError("timestamp 模式需要开始时间")
    if mode == "offset" and offset is None:
        raise BrowseError("offset 模式需要 Offset")

    resume: dict[int, int] = {}
    raw_resume = q.get("from") or q.get("nextOffsets")
    if isinstance(raw_resume, dict):
        for k, v in raw_resume.items():
            try:
                resume[int(k)] = int(v)
            except (TypeError, ValueError):
                continue

    b = Browser(cluster.bootstrap, _sec_of(cluster))
    try:
        if parts is None:
            bs = b._nc(b.host, b.port)
            try:
                await bs.connect(0, label="browse-meta")
                meta_dec, _, _ = await bs.call(
                    3, "Metadata", 0, codec.encode_metadata_v0([topic]),
                    0, "metadata", extra_req={"topics": [topic]}, topic=topic,
                    decode=codec.decode_metadata_v0)
            except Exception as e:  # noqa: BLE001
                raise _friendly("连接集群失败", e)
            meta = _metadata_dict(meta_dec)
            tmeta = next((x for x in meta["topics"] if x["topic"] == topic), None)
            if tmeta is None or tmeta.get("error", 0) != 0:
                raise BrowseError(f"Topic 不存在：'{topic}'")
            parts = sorted(p["partition"] for p in tmeta.get("partitions", []))
            if not parts:
                raise BrowseError(f"Topic 无可用分区：'{topic}'")
        try:
            return await b.query(topic, parts, mode, offset, start_ms, end_ms,
                                 limit, q.get("key") or "", q.get("value") or "",
                                 resume or None)
        except BrowseError:
            raise
        except Exception as e:  # noqa: BLE001
            raise _friendly("查询失败", e)
    finally:
        b.close()
