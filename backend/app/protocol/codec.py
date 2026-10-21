from __future__ import annotations
import struct
from dataclasses import dataclass
from typing import List, Tuple, Optional


def enc_string(s: Optional[str]) -> bytes:
    if s is None:
        return struct.pack(">h", -1)
    b = s.encode("utf-8")
    return struct.pack(">h", len(b)) + b


def dec_string(buf: bytes, off: int) -> Tuple[Optional[str], int]:
    (ln,) = struct.unpack_from(">h", buf, off)
    off += 2
    if ln < 0:
        return None, off
    return buf[off:off + ln].decode("utf-8"), off + ln


def enc_bytes(b: Optional[bytes]) -> bytes:
    if b is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(b)) + b


def dec_bytes(buf: bytes, off: int) -> Tuple[Optional[bytes], int]:
    (ln,) = struct.unpack_from(">i", buf, off)
    off += 4
    if ln < 0:
        return None, off
    return buf[off:off + ln], off + ln


def enc_array(items: List[bytes]) -> bytes:
    out = struct.pack(">i", len(items))
    for it in items:
        out += it
    return out


def dec_array_count(buf: bytes, off: int) -> Tuple[int, int]:
    (n,) = struct.unpack_from(">i", buf, off)
    return n, off + 4


def _enc_unsigned_varint(n: int) -> bytes:
    out = bytearray()
    v = n
    while (v & ~0x7F) != 0:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _enc_signed_varint(n: int) -> bytes:
    zz = (n << 1) ^ (n >> 63)
    return _enc_unsigned_varint(zz & 0xFFFFFFFFFFFFFFFF)


def enc_varint(n: int) -> bytes:
    return _enc_unsigned_varint(n)


def dec_varint(buf: bytes, off: int) -> Tuple[int, int]:
    shift, res = 0, 0
    while True:
        b = buf[off]
        off += 1
        res |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return res, off


def dec_signed_varint(buf: bytes, off: int) -> Tuple[int, int]:
    u, off = dec_varint(buf, off)
    return ((u >> 1) ^ -(u & 1)), off


def enc_varint_bytes(b: Optional[bytes]) -> bytes:
    # Kafka Record key/value lengths are SIGNED varints (zigzag), -1 = null
    if b is None:
        return _enc_signed_varint(-1)
    return _enc_signed_varint(len(b)) + b


def build_request(api_key: int, api_version: int, correlation_id: int,
                  client_id: str, body: bytes, flexible: bool = False) -> bytes:
    header = struct.pack(">hhi", api_key, api_version, correlation_id) + enc_string(client_id)
    if flexible:
        # flexible 版本用请求头 v1：末尾追加 tagged fields（空 = 0x00）。
        # 少了这一字节 broker 会把 body 首字节当 tag 数来解，直接断连。
        header += b"\x00"
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def skip_response_tags(buf: bytes, off: int = 0) -> int:
    """flexible 回包：correlation 之后先是 tagged fields buffer，跳过它。"""
    tag_n, off = dec_varint(buf, off)
    for _ in range(tag_n):
        _, off = dec_varint(buf, off)  # tag id
        ln, off = dec_varint(buf, off)  # data len
        off += ln
    return off


def encode_apiversions_v0() -> bytes:
    return b""


def decode_apiversions_v0(buf: bytes) -> dict:
    off = 0
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    (n,) = struct.unpack_from(">i", buf, off)
    off += 4
    apis = []
    for _ in range(max(n, 0)):
        k, mn, mx = struct.unpack_from(">hhh", buf, off)
        off += 6
        apis.append({"apiKey": k, "minVersion": mn, "maxVersion": mx})
    return {"errorCode": err, "apis": apis}


def encode_metadata_v0(topics: List[str]) -> bytes:
    return enc_array([enc_string(t) for t in topics])


@dataclass
class PartitionInfo:
    error: int
    partition: int
    leader: int
    replicas: List[int]
    isr: List[int]


@dataclass
class TopicMeta:
    error: int
    topic: str
    partitions: List[PartitionInfo]


@dataclass
class MetadataResponse:
    brokers: List[dict]
    topics: List[TopicMeta]
    controller_id: Optional[int] = None


def decode_metadata_v0(buf: bytes) -> MetadataResponse:
    off = 0
    n, off = dec_array_count(buf, off)
    brokers = []
    for _ in range(max(n, 0)):
        (nid,) = struct.unpack_from(">i", buf, off)
        off += 4
        host, off = dec_string(buf, off)
        (port,) = struct.unpack_from(">i", buf, off)
        off += 4
        brokers.append({"id": nid, "host": host, "port": port})
    tn, off = dec_array_count(buf, off)
    topics: List[TopicMeta] = []
    for _ in range(max(tn, 0)):
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (perr, pid, leader) = struct.unpack_from(">hii", buf, off)
            off += 10
            rn, off = dec_array_count(buf, off)
            replicas = []
            for _ in range(max(rn, 0)):
                (r,) = struct.unpack_from(">i", buf, off)
                off += 4
                replicas.append(r)
            isn, off = dec_array_count(buf, off)
            isr = []
            for _ in range(max(isn, 0)):
                (v,) = struct.unpack_from(">i", buf, off)
                off += 4
                isr.append(v)
            parts.append(PartitionInfo(perr, pid, leader, replicas, isr))
        topics.append(TopicMeta(err, topic or "", parts))
    return MetadataResponse(brokers, topics)


def decode_metadata_v1(buf: bytes) -> MetadataResponse:
    """Metadata v1 adds the controller id and broker rack to the v0 response."""
    off = 0
    n, off = dec_array_count(buf, off)
    if n < 0 or n > 10000:
        raise ValueError(f"bad broker count {n}")
    brokers = []
    for _ in range(n):
        (nid,) = struct.unpack_from(">i", buf, off)
        off += 4
        host, off = dec_string(buf, off)
        (port,) = struct.unpack_from(">i", buf, off)
        off += 4
        rack, off = dec_string(buf, off)
        brokers.append({"id": nid, "host": host, "port": port, "rack": rack})
    (controller_id,) = struct.unpack_from(">i", buf, off)
    off += 4
    tn, off = dec_array_count(buf, off)
    if tn < 0:
        tn = 0
    if tn > 100000:
        raise ValueError(f"bad topic count {tn}")
    topics: List[TopicMeta] = []
    for _ in range(tn):
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        topic, off = dec_string(buf, off)
        if off >= len(buf):
            raise ValueError("truncated Metadata v1 internal flag")
        off += 1  # is_internal
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (perr, pid, leader) = struct.unpack_from(">hii", buf, off)
            off += 10
            rn, off = dec_array_count(buf, off)
            replicas = []
            for _ in range(max(rn, 0)):
                (replica,) = struct.unpack_from(">i", buf, off)
                off += 4
                replicas.append(replica)
            isn, off = dec_array_count(buf, off)
            isr = []
            for _ in range(max(isn, 0)):
                (replica,) = struct.unpack_from(">i", buf, off)
                off += 4
                isr.append(replica)
            parts.append(PartitionInfo(perr, pid, leader, replicas, isr))
        topics.append(TopicMeta(err, topic or "", parts))
    return MetadataResponse(brokers, topics, controller_id)


def _crc32c(data: bytes) -> int:
    import binascii
    return binascii.crc32(data) & 0xFFFFFFFF


def encode_message_set(key: Optional[bytes], value: Optional[bytes]) -> bytes:
    """Legacy MessageSet entry (magic=0) for Produce v0 — accepted by all brokers."""
    import binascii
    k = struct.pack(">i", len(key)) + key if key is not None else struct.pack(">i", -1)
    v = struct.pack(">i", len(value)) + value if value is not None else struct.pack(">i", -1)
    msg = struct.pack(">bb", 0, 0) + k + v  # magic=0, attributes=0 (crc covers this)
    crc = binascii.crc32(msg) & 0xFFFFFFFF
    return struct.pack(">q", 0) + struct.pack(">i", len(msg) + 4) + struct.pack(">I", crc) + msg


def encode_produce_v0(topic: str, partition: int, key: Optional[bytes],
                      value: Optional[bytes], acks: int = 1,
                      timeout_ms: int = 10000) -> bytes:
    return encode_produce_v0_multi(topic, partition, [(key, value)], acks, timeout_ms)


def encode_produce_v0_multi(topic: str, partition: int,
                            records: list,
                            acks: int = 1,
                            timeout_ms: int = 10000) -> bytes:
    """One Produce v0 request carrying N MessageSet entries (true batch)."""
    payload = b"".join(encode_message_set(k, v) for k, v in records)
    out = struct.pack(">h", acks) + struct.pack(">i", timeout_ms)
    out += enc_array([enc_string(topic)
                      + enc_array([struct.pack(">i", partition)
                                   + struct.pack(">i", len(payload)) + payload])])
    return out


def decode_produce_v0(buf: bytes) -> dict:
    off = 0
    tn, off = dec_array_count(buf, off)
    topics = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid, err, base_off) = struct.unpack_from(">ihq", buf, off)
            off += 14
            parts.append({"partition": pid, "errorCode": err,
                          "baseOffset": base_off, "logAppendTime": -1,
                          "logStartOffset": 0})
        topics.append({"topic": topic, "partitions": parts})
    return {"topics": topics, "throttleMs": 0}


def encode_record_batch(key: Optional[bytes], value: Optional[bytes]) -> bytes:
    # record: length(varint) + attributes(1) + tsDelta(varint) + offsetDelta(varint)
    #         + key(varintBytes) + value(varintBytes) + headers(varint count)
    rec_body = (b"\x00" + _enc_signed_varint(0) + _enc_signed_varint(0)
                + enc_varint_bytes(key) + enc_varint_bytes(value)
                + enc_varint(0))  # headers count = 0 (empty, NOT null)
    rec = enc_varint(len(rec_body)) + rec_body
    # batch tail (covered by CRC): attributes..records
    tail = struct.pack(">h", 0)          # attributes
    tail += struct.pack(">i", 0)         # lastOffsetDelta
    tail += struct.pack(">q", -1)        # firstTimestamp (use -1, not 0)
    tail += struct.pack(">q", -1)        # maxTimestamp
    tail += struct.pack(">q", -1)        # producerId
    tail += struct.pack(">h", -1)        # producerEpoch
    tail += struct.pack(">i", -1)        # baseSequence
    tail += struct.pack(">i", 1)         # record count
    tail += rec
    crc = _crc32c(tail)
    out = struct.pack(">q", 0)           # baseOffset
    out += struct.pack(">i", len(tail) + 4 + 1 + 4)  # length: ple+magic+crc+tail
    out += struct.pack(">i", -1)         # partitionLeaderEpoch
    out += struct.pack(">b", 2)          # magic
    out += struct.pack(">I", crc)
    out += tail
    return out


def encode_produce_v2(topic: str, partition: int, key: Optional[bytes],
                      value: Optional[bytes], acks: int = -1,
                      timeout_ms: int = 30000) -> bytes:
    batch = encode_record_batch(key, value)
    out = enc_string(None)               # transactionalId = null
    out += struct.pack(">h", acks) + struct.pack(">i", timeout_ms)
    out += enc_array([enc_string(topic)
                      + enc_array([struct.pack(">i", partition) + enc_bytes(batch)])])
    return out


def decode_produce_v2(buf: bytes) -> dict:
    off = 0
    tn, off = dec_array_count(buf, off)
    topics = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid, err) = struct.unpack_from(">ih", buf, off)
            off += 6
            (base_off,) = struct.unpack_from(">q", buf, off)
            off += 8
            (log_append,) = struct.unpack_from(">q", buf, off)
            off += 8
            (log_start,) = struct.unpack_from(">q", buf, off)
            off += 8
            parts.append({"partition": pid, "errorCode": err,
                          "baseOffset": base_off, "logAppendTime": log_append,
                          "logStartOffset": log_start})
        topics.append({"topic": topic, "partitions": parts})
    (throttle,) = struct.unpack_from(">i", buf, off) if off + 4 <= len(buf) else (0,)
    return {"topics": topics, "throttleMs": throttle}


def pick_acks(name: str) -> int:
    return {"all": -1, "-1": -1, "1": 1, "0": 0}.get(str(name).lower(), -1)


def hexdump(data: bytes, width: int = 16) -> str:
    lines = []
    for i in range(0, len(data), width):
        chunk = data[i:i + width]
        hexs = " ".join(f"{b:02x}" for b in chunk)
        asc = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{i:04x}  {hexs:<{width * 3 - 1}}  {asc}")
    return "\n".join(lines)


# ---------- Fetch (v1, non-flexible) ----------

def encode_fetch_v1(topic: str, partition: int, offset: int,
                    max_bytes: int = 1048576, max_wait_ms: int = 5000,
                    min_bytes: int = 1) -> bytes:
    out = struct.pack(">iiii", -1, max_wait_ms, min_bytes, max_bytes)
    out += struct.pack(">b", 0)  # isolationLevel = READ_UNCOMMITTED
    out += enc_array([enc_string(topic) + enc_array([
        struct.pack(">i", partition) + struct.pack(">q", offset) + struct.pack(">i", max_bytes)])])
    return out


def encode_fetch_v4(topic: str, partition: int, offset: int,
                    max_bytes: int = 1048576, max_wait_ms: int = 5000,
                    min_bytes: int = 1) -> bytes:
    """Fetch v4 with FULL fetch session (sessionId=0, epoch=0).

    NOTE: this broker closes connections on Fetch v0/v1; v4 is the
    lowest version it answers. Response layout (empirically verified):
    throttle + topics[{topic, partitions[{partition, error, hw,
    lastStableOffset, abortedTransactions, records}]}].
    """
    out = struct.pack(">iiii", -1, max_wait_ms, min_bytes, max_bytes)
    out += struct.pack(">b", 0)  # isolationLevel = READ_UNCOMMITTED
    out += enc_array([enc_string(topic) + enc_array([
        struct.pack(">i", partition) + struct.pack(">q", offset) + struct.pack(">i", max_bytes)])])
    out += struct.pack(">ii", 0, 0)  # sessionId=0, epoch=0 → FULL fetch
    out += enc_array([])             # forgottenTopicsData
    out += enc_string("")            # rackId
    return out


def decode_fetch_v4(buf: bytes) -> dict:
    off = 0
    (throttle,) = struct.unpack_from(">i", buf, off)
    off += 4
    tn, off = dec_array_count(buf, off)
    topics = []
    messages: list = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid, err) = struct.unpack_from(">ih", buf, off)
            off += 6
            (hw,) = struct.unpack_from(">q", buf, off)
            off += 8
            (last_stable,) = struct.unpack_from(">q", buf, off)
            off += 8
            (aborted_n,) = struct.unpack_from(">i", buf, off)
            off += 4
            for _ in range(max(aborted_n, 0)):
                off += 16  # producerId + firstOffset
            (rlen,) = struct.unpack_from(">i", buf, off)
            off += 4
            recs = buf[off:off + max(rlen, 0)]
            off += max(rlen, 0)
            batch = decode_record_batches(recs) if rlen > 0 else []
            for m in batch:
                m["topic"] = topic
                m["partition"] = pid
            messages.extend(batch)
            parts.append({"partition": pid, "errorCode": err,
                          "highWatermark": hw, "lastStableOffset": last_stable,
                          "messageCount": len(batch)})
        topics.append({"topic": topic, "partitions": parts})
    return {"topics": topics, "messages": messages, "throttleMs": throttle}


def decode_record_batches(data: bytes) -> list:
    """Decode magic=2 RecordBatches into [{offset,key,value,timestamp}]."""
    msgs: list = []
    off = 0
    try:
        while off + 12 <= len(data):
            (base_off,) = struct.unpack_from(">q", data, off)
            off += 8
            (blen,) = struct.unpack_from(">i", data, off)
            off += 4
            end = off + blen
            if end > len(data) or blen <= 0:
                break
            (ple,) = struct.unpack_from(">i", data, off)
            off += 4
            magic = data[off]
            off += 1
            off += 4  # crc
            if magic != 2:
                off = end
                continue
            off += 2 + 4 + 8 + 8 + 8 + 2 + 4  # attrs,lastDelta,firstTs,maxTs,pid,epoch,seq
            (count,) = struct.unpack_from(">i", data, off)
            off += 4
            for _ in range(max(count, 0)):
                rlen, off = dec_varint(data, off)
                rend = off + rlen
                off += 1  # attributes
                ts_d, off = dec_signed_varint(data, off)
                off_d, off = dec_signed_varint(data, off)
                key, off = _dec_varint_bytes(data, off)
                val, off = _dec_varint_bytes(data, off)
                nh, off = dec_varint(data, off)
                for _ in range(nh):
                    kl, off = dec_varint(data, off)
                    off += kl
                    vl, off = dec_varint(data, off)
                    off += vl if vl >= 0 else 0
                msgs.append({"offset": base_off + off_d,
                             "key": _try_decode(key), "value": _try_decode(val),
                             "timestampDelta": ts_d})
                off = rend
            off = end
    except (struct.error, IndexError):
        pass
    return msgs


def _dec_varint_bytes(buf: bytes, off: int):
    ln, off = dec_signed_varint(buf, off)
    if ln < 0:
        return None, off
    return buf[off:off + ln], off + ln


def _try_decode(b: Optional[bytes]):
    if b is None:
        return None
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.hex()


def decode_record_batches_full(data: bytes) -> list:
    """完整 record 解码（消息浏览用）：[{offset,key,value,timestamp,headers,size}]。

    timestamp = batch.firstTimestamp + record.timestampDelta；
    headers = [{key, value}]（value 非 utf-8 时转 hex）；
    size = record 区字节数。原 decode_record_batches 保持不动。
    """
    msgs: list = []
    off = 0
    try:
        while off + 12 <= len(data):
            (base_off,) = struct.unpack_from(">q", data, off)
            off += 8
            (blen,) = struct.unpack_from(">i", data, off)
            off += 4
            end = off + blen
            if end > len(data) or blen <= 0:
                break
            off += 4  # partitionLeaderEpoch
            magic = data[off]
            off += 1
            off += 4  # crc
            if magic != 2:
                off = end
                continue
            off += 2 + 4  # attributes, lastOffsetDelta
            (first_ts,) = struct.unpack_from(">q", data, off)
            off += 8
            off += 8  # maxTimestamp
            off += 8 + 2 + 4  # producerId, producerEpoch, baseSequence
            (count,) = struct.unpack_from(">i", data, off)
            off += 4
            for _ in range(max(count, 0)):
                rlen, off = dec_varint(data, off)
                rend = off + rlen
                off += 1  # attributes
                ts_d, off = dec_signed_varint(data, off)
                off_d, off = dec_signed_varint(data, off)
                key, off = _dec_varint_bytes(data, off)
                val, off = _dec_varint_bytes(data, off)
                nh, off = dec_varint(data, off)
                headers = []
                for _ in range(max(nh, 0)):
                    kl, off = dec_varint(data, off)
                    kb = data[off:off + kl]
                    off += kl
                    vl, off = dec_varint(data, off)
                    if vl is not None and vl >= 0:
                        vb = data[off:off + vl]
                        off += vl
                    else:
                        vb = None
                    try:
                        hk = kb.decode("utf-8")
                    except UnicodeDecodeError:
                        hk = kb.hex()
                    headers.append({"key": hk, "value": _try_decode(vb)})
                msgs.append({"offset": base_off + off_d,
                             "key": _try_decode(key), "value": _try_decode(val),
                             "timestamp": first_ts + ts_d,
                             "headers": headers, "size": rlen})
                off = rend
            off = end
    except (struct.error, IndexError):
        pass
    return msgs


def decode_fetch_v1(buf: bytes) -> dict:
    off = 0
    (throttle,) = struct.unpack_from(">i", buf, off)
    off += 4
    tn, off = dec_array_count(buf, off)
    topics = []
    messages: list = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid,) = struct.unpack_from(">i", buf, off)
            off += 4
            (err,) = struct.unpack_from(">h", buf, off)
            off += 2
            (hw,) = struct.unpack_from(">q", buf, off)
            off += 8
            (lso,) = struct.unpack_from(">q", buf, off)
            off += 8
            (lsso,) = struct.unpack_from(">q", buf, off)
            off += 8
            ab_n, off = dec_array_count(buf, off)
            off += max(ab_n, 0) * 0  # v1 has no aborted tx content beyond count==0/-1
            recs, off = dec_bytes(buf, off)
            pm = decode_record_batches(recs) if recs else []
            messages.extend([{**m, "topic": topic, "partition": pid} for m in pm])
            parts.append({"partition": pid, "errorCode": err, "highWatermark": hw,
                          "lastStableOffset": lso, "logStartOffset": lsso,
                          "messageCount": len(pm)})
        topics.append({"topic": topic, "partitions": parts})
    return {"throttleMs": throttle, "topics": topics, "messages": messages}


# ---------- ListOffsets (v1) ----------

def encode_listoffsets_v1(topic: str, partition: int, timestamp: int = -1) -> bytes:
    out = struct.pack(">i", -1)
    out += enc_array([enc_string(topic)
                      + enc_array([struct.pack(">i", partition) + struct.pack(">q", timestamp)])])
    return out


def decode_listoffsets_v1(buf: bytes) -> dict:
    off = 0
    tn, off = dec_array_count(buf, off)
    topics = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid,) = struct.unpack_from(">i", buf, off)
            off += 4
            (err,) = struct.unpack_from(">h", buf, off)
            off += 2
            (ts,) = struct.unpack_from(">q", buf, off)
            off += 8
            (voff,) = struct.unpack_from(">q", buf, off)
            off += 8
            parts.append({"partition": pid, "errorCode": err,
                          "timestamp": ts, "offset": voff})
        topics.append({"topic": topic, "partitions": parts})
    return {"topics": topics}


# ---------- FindCoordinator (v0) ----------

def encode_find_coordinator_v0(key: str) -> bytes:
    return enc_string(key)


def decode_find_coordinator_v0(buf: bytes) -> dict:
    off = 0
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    (nid,) = struct.unpack_from(">i", buf, off)
    off += 4
    host, off = dec_string(buf, off)
    (port,) = struct.unpack_from(">i", buf, off)
    return {"errorCode": err, "nodeId": nid, "host": host, "port": port}


# ---------- JoinGroup (v1, consumer protocol) ----------

def encode_consumer_metadata_v0(topics: List[str]) -> bytes:
    return struct.pack(">h", 0) + enc_array([enc_string(t) for t in topics]) + enc_bytes(None)


def encode_join_group_v1(group_id: str, member_id: str, topics: List[str],
                          session_timeout_ms: int = 10000,
                          rebalance_timeout_ms: int = 60000) -> bytes:
    meta = encode_consumer_metadata_v0(topics)
    proto = enc_string("roundrobin") + enc_bytes(meta)
    out = enc_string(group_id)
    out += struct.pack(">i", session_timeout_ms) + struct.pack(">i", rebalance_timeout_ms)
    out += enc_string(member_id) + enc_string("consumer") + enc_array([proto])
    return out


def decode_join_group_v1(buf: bytes) -> dict:
    off = 0
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    (gen,) = struct.unpack_from(">i", buf, off)
    off += 4
    proto, off = dec_string(buf, off)
    leader, off = dec_string(buf, off)
    member, off = dec_string(buf, off)
    n, off = dec_array_count(buf, off)
    members = []
    for _ in range(max(n, 0)):
        mid, off = dec_string(buf, off)
        mmeta, off = dec_bytes(buf, off)
        members.append({"memberId": mid, "metadataSize": len(mmeta) if mmeta else 0})
    return {"errorCode": err, "generationId": gen, "protocol": proto,
            "leaderId": leader, "memberId": member, "members": members}


# ---------- SyncGroup (v1) ----------

def encode_consumer_assignment_v0(assign: List[tuple]) -> bytes:
    """assign: [(topic, [partitions])]."""
    parts = []
    for t, ps in assign:
        parts.append(enc_string(t) + enc_array([struct.pack(">i", p) for p in ps]))
    return struct.pack(">h", 0) + enc_array(parts) + enc_bytes(None)


def encode_sync_group_v1(group_id: str, generation: int, member_id: str,
                         assignments: List[tuple]) -> bytes:
    """assignments: [(memberId, [(topic,[parts])])] — leader fills all, follower sends empty."""
    items = []
    for mid, a in assignments:
        items.append(enc_string(mid) + enc_bytes(encode_consumer_assignment_v0(a)))
    out = enc_string(group_id) + struct.pack(">i", generation) + enc_string(member_id)
    out += enc_array(items)
    return out


def decode_sync_group_v1(buf: bytes) -> dict:
    off = 0
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    assign, off = dec_bytes(buf, off)
    return {"errorCode": err, "assignmentSize": len(assign) if assign else 0}


def decode_consumer_assignment(data: bytes) -> dict:
    try:
        off = 0
        (ver,) = struct.unpack_from(">h", data, off)
        off += 2
        n, off = dec_array_count(data, off)
        parts = []
        for _ in range(max(n, 0)):
            t, off = dec_string(data, off)
            m, off = dec_array_count(data, off)
            ps = []
            for _ in range(max(m, 0)):
                (p,) = struct.unpack_from(">i", data, off)
                off += 4
                ps.append(p)
            parts.append({"topic": t, "partitions": ps})
        return {"version": ver, "assignment": parts}
    except (struct.error, IndexError):
        return {"version": -1, "assignment": []}


# ---------- Heartbeat (v1) ----------

def encode_heartbeat_v1(group_id: str, generation: int, member_id: str) -> bytes:
    return enc_string(group_id) + struct.pack(">i", generation) + enc_string(member_id)


def decode_heartbeat_v1(buf: bytes) -> dict:
    (err,) = struct.unpack_from(">h", buf, 0)
    return {"errorCode": err}


# ---------- LeaveGroup (v1) ----------

def encode_leave_group_v1(group_id: str, member_id: str) -> bytes:
    return enc_string(group_id) + enc_string(member_id)


def decode_leave_group_v1(buf: bytes) -> dict:
    (err,) = struct.unpack_from(">h", buf, 0)
    return {"errorCode": err}


# ---------- OffsetCommit (v2) ----------

def encode_offset_commit_v2(group_id: str, generation: int, member_id: str,
                             topic: str, partition: int, offset: int) -> bytes:
    out = enc_string(group_id) + struct.pack(">i", generation) + enc_string(member_id)
    out += struct.pack(">q", -1)
    out += enc_array([enc_string(topic) + enc_array([
        struct.pack(">i", partition) + struct.pack(">q", offset) + enc_string(None)])])
    return out


def decode_offset_commit_v2(buf: bytes) -> dict:
    off = 0
    tn, off = dec_array_count(buf, off)
    topics = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid,) = struct.unpack_from(">i", buf, off)
            off += 4
            (err,) = struct.unpack_from(">h", buf, off)
            off += 2
            parts.append({"partition": pid, "errorCode": err})
        topics.append({"topic": topic, "partitions": parts})
    return {"topics": topics}


# ---------- OffsetFetch (v1) ----------

def encode_offset_fetch_v1(group_id: str, topic: str, partitions: List[int]) -> bytes:
    out = enc_string(group_id)
    out += enc_array([enc_string(topic)
                      + enc_array([struct.pack(">i", p) for p in partitions])])
    return out


def decode_offset_fetch_v1(buf: bytes) -> dict:
    off = 0
    tn, off = dec_array_count(buf, off)
    topics = []
    for _ in range(max(tn, 0)):
        topic, off = dec_string(buf, off)
        pn, off = dec_array_count(buf, off)
        parts = []
        for _ in range(max(pn, 0)):
            (pid,) = struct.unpack_from(">i", buf, off)
            off += 4
            (voff,) = struct.unpack_from(">q", buf, off)
            off += 8
            meta, off = dec_string(buf, off)
            (err,) = struct.unpack_from(">h", buf, off)
            off += 2
            parts.append({"partition": pid, "offset": voff,
                          "metadata": meta, "errorCode": err})
        topics.append({"topic": topic, "partitions": parts})
    return {"topics": topics}


# ---------- ListGroups (v1) / DescribeGroups (v1) ----------

def decode_list_groups_v1(buf: bytes) -> dict:
    # v1 response: throttle(4) + error(2) + groups[{groupId, protocolType}]
    # （兼容无 throttle 的短包：长度不足时按 error 开头解析）
    off = 0
    if len(buf) >= 8:
        off += 4  # throttle_time_ms
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    n, off = dec_array_count(buf, off)
    groups = []
    for _ in range(max(n, 0)):
        gid, off = dec_string(buf, off)
        pt, off = dec_string(buf, off)
        groups.append({"groupId": gid, "protocolType": pt})
    return {"errorCode": err, "groups": groups}


def encode_describe_groups_v1(groups: List[str]) -> bytes:
    return enc_array([enc_string(g) for g in groups])


def decode_describe_groups_v1(buf: bytes) -> dict:
    # v1 response: throttle(4) + groups[]（兼容无 throttle 短包）
    off = 0
    if len(buf) >= 8:
        off += 4  # throttle_time_ms
    n, off = dec_array_count(buf, off)
    groups = []
    for _ in range(max(n, 0)):
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        gid, off = dec_string(buf, off)
        state, off = dec_string(buf, off)
        ptype, off = dec_string(buf, off)
        proto, off = dec_string(buf, off)
        mn, off = dec_array_count(buf, off)
        members = []
        for _ in range(max(mn, 0)):
            mid, off = dec_string(buf, off)
            cid, off = dec_string(buf, off)
            chost, off = dec_string(buf, off)
            mmeta, off = dec_bytes(buf, off)
            mass, off = dec_bytes(buf, off)
            members.append({"memberId": mid, "clientId": cid, "clientHost": chost,
                            "metadataSize": len(mmeta) if mmeta else 0,
                            "assignment": decode_consumer_assignment(mass) if mass else {}})
        groups.append({"errorCode": err, "groupId": gid, "state": state, "protocolType": ptype,
                       "protocol": proto, "members": members})
    return {"groups": groups}


# ---------- CreateTopics (v0, api 19) / DeleteTopics (v0, api 20) ----------

def encode_create_topics_v0(topics: List[tuple], timeout_ms: int = 10000) -> bytes:
    """topics: [(name, num_partitions, replication_factor, configs|None)]。

    configs: {name: value}，value=None 表示 null（Kafbat NewTopic.configs 对等语义）。
    副本分配留空走 broker 默认（对等 AdminClient 未指定 assignments）。
    """
    items = b""
    for entry in topics:
        name, parts, rf = entry[0], entry[1], entry[2]
        configs = entry[3] if len(entry) > 3 else None
        items += enc_string(name)
        items += struct.pack(">i", int(parts))
        items += struct.pack(">h", int(rf))
        items += struct.pack(">i", 0)  # replicas_assignments: 空
        cfg = configs or {}
        cfg_items = b""
        for k, v in cfg.items():
            cfg_items += enc_string(str(k))
            cfg_items += enc_string(v) if v is not None else struct.pack(">h", -1)
        items += struct.pack(">i", len(cfg)) + cfg_items
    return struct.pack(">i", len(topics)) + items + struct.pack(">i", timeout_ms)


def _parse_topic_errors(buf: bytes, off: int):
    """解析 [topics: {topic, errorCode}]，容忍 v1+ 的 error_message 尾巴。

    有些 broker 回包带 throttle 前缀 / error_message 后缀（v1 形状），
    严格按 v0 解析会错位读出假错误码。调用方用 _decode_topic_errors
    挑选能完整消费缓冲区的版本。
    """
    n, off = dec_array_count(buf, off)
    if n < 0 or n > 1000:
        raise ValueError(f"bad topic count {n}")
    out = []
    for _ in range(n):
        t, off = dec_string(buf, off)
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        msg = None
        if off < len(buf):
            # 可能是 v1+ 的 errorMessage（nullable string）：试探消费
            (mlen,) = struct.unpack_from(">h", buf, off)
            if mlen == -1:
                off += 2
            elif 0 <= mlen <= len(buf) - off - 2:
                msg = buf[off + 2:off + 2 + mlen].decode("utf-8", errors="replace")
                off += 2 + mlen
        out.append({"topic": t, "errorCode": err, "errorMessage": msg})
    return out, off


def _decode_topic_errors(buf: bytes) -> dict:
    """offset 0（标准 v0）与 offset 4（带 throttle 前缀）两种形状都试，
    选择能完整消费缓冲区的解析；都完整则优先标准 v0。
    返回含 _end（消费到的偏移，用于调用方验收包体是否被完整消费）。"""
    v4 = None
    v4_end = 0
    for off in (0, 4):
        try:
            out, end = _parse_topic_errors(buf, off)
            if end == len(buf):
                if off == 0:
                    return {"topics": out, "_end": end}
                v4 = {"topics": out, "throttlePrefixed": True, "_end": end}
        except Exception:  # noqa: BLE001
            continue
    if v4 is not None:
        return v4
    out, end = _parse_topic_errors(buf, 0)
    return {"topics": out, "_end": end}


def decode_create_topics_v0(buf: bytes) -> dict:
    return _decode_topic_errors(buf)


# ---------- Flexible (compact) 编码：CreateTopics v5（与官方 3.x 客户端同版本） ----------

def enc_uvarint(n: int) -> bytes:
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out += struct.pack("B", b | 0x80)
        else:
            out += struct.pack("B", b)
            return out


def enc_compact_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return enc_uvarint(len(b) + 1) + b


def enc_compact_array(items: List[bytes]) -> bytes:
    out = enc_uvarint(len(items) + 1)
    for it in items:
        out += it
    return out


def dec_compact_len(buf: bytes, off: int) -> Tuple[int, int]:
    """返回 (长度, off)：compact 里存的是 len+1，0/-1 表示 null/空。"""
    u, off = dec_varint(buf, off)
    return u - 1, off


def dec_compact_string(buf: bytes, off: int) -> Tuple[Optional[str], int]:
    ln, off = dec_compact_len(buf, off)
    if ln < 0:
        return None, off
    s = buf[off:off + ln].decode("utf-8", errors="replace")
    return s, off + ln


def enc_tagged_empty() -> bytes:
    return b"\x00"  # tagged fields count = 0


def encode_create_topics_v5(topics: List[tuple], timeout_ms: int = 10000,
                             null_assignments: bool = True) -> bytes:
    """CreateTopics v5：官方 3.x kafka-topics.sh 同款版本。

    topics: [(name, num_partitions, replication_factor, configs|None)]，
    assignments=null（0x00）或 empty（0x01）两种写法都支持，
    configs 为 compact {name: compact_nullable value}，validate_only=false。
    """
    items = []
    for entry in topics:
        name, parts, rf = entry[0], entry[1], entry[2]
        configs = entry[3] if len(entry) > 3 else None
        it = enc_compact_string(name)
        it += struct.pack(">i", int(parts))
        it += struct.pack(">h", int(rf))
        it += b"\x00" if null_assignments else b"\x01"
        cfg_items = []
        for k, v in (configs or {}).items():
            ce = enc_compact_string(str(k))
            ce += enc_compact_string(v) if v is not None else b"\x00"
            cfg_items.append(ce)
        it += enc_compact_array(cfg_items)
        it += enc_tagged_empty()
        items.append(it)
    out = enc_compact_array(items)
    out += struct.pack(">i", timeout_ms)
    out += b"\x00"  # validate_only = false
    out += enc_tagged_empty()
    return out


def decode_create_topics_v5(buf: bytes) -> dict:
    """CreateTopics v5 response: tags + throttle + compact topics[{name, error, message, authorizedOps}] + tags。

    附带 _end（消费到的偏移）：调用方校验 _end == len(buf)，防止把异形包
    静默误解（错位解析可能不抛错却读出垃圾错误码）。
    """
    off = skip_response_tags(buf, 0)
    (throttle,) = struct.unpack_from(">i", buf, off)
    off += 4
    n, off = dec_compact_len(buf, off)
    if n < 0 or n > 1000:
        raise ValueError(f"bad v5 topic count {n}")
    out = []
    for _ in range(n):
        t, off = dec_compact_string(buf, off)
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        msg, off = dec_compact_string(buf, off)
        (ops,) = struct.unpack_from(">i", buf, off)
        off += 4
        # tagged fields：跳过
        tag_n, off = dec_varint(buf, off)
        for _ in range(tag_n):
            _, off = dec_varint(buf, off)
            ln, off = dec_varint(buf, off)
            off += ln
        out.append({"topic": t, "errorCode": err, "errorMessage": msg,
                    "authorizedOps": ops})
    try:
        end2 = skip_response_tags(buf, off)
        if end2 == len(buf):
            off = end2
    except Exception:  # noqa: BLE001
        pass
    return {"throttleMs": throttle, "topics": out, "_end": off}


def encode_delete_topics_v0(topics: List[str], timeout_ms: int = 10000) -> bytes:
    return enc_array([enc_string(t) for t in topics]) + struct.pack(">i", timeout_ms)


def decode_delete_topics_v0(buf: bytes) -> dict:
    return _decode_topic_errors(buf)


def decode_delete_topics_v1_to_v3(buf: bytes) -> dict:
    """DeleteTopics v1–v3: throttle_time_ms + name/error_code pairs."""
    (throttle,) = struct.unpack_from(">i", buf, 0)
    off = 4
    count, off = dec_array_count(buf, off)
    if count < 0 or count > 1000:
        raise ValueError(f"bad DeleteTopics response count {count}")
    topics = []
    for _ in range(count):
        name, off = dec_string(buf, off)
        (code,) = struct.unpack_from(">h", buf, off)
        off += 2
        topics.append({"topic": name, "errorCode": code})
    return {"throttleMs": throttle, "topics": topics, "_end": off}


# ---------- CreatePartitions (v0, api 37) / AlterPartitionReassignments (v0, api 45) ----------

def encode_create_partitions_v0(topic: str, count: int, timeout_ms: int = 30000) -> bytes:
    """分区扩容：assignments 留空（null）走 broker 默认分配。"""
    body = struct.pack(">i", 1)
    body += enc_string(topic)
    body += struct.pack(">i", int(count))
    body += struct.pack(">i", -1)  # assignments: null → broker 默认
    body += struct.pack(">i", timeout_ms)
    body += b"\x00"  # validate_only = false
    return body


def decode_create_partitions_v0(buf: bytes) -> dict:
    # v0 response: throttle(4) + [results: {topic, errorCode, errorMessage?}]
    (throttle,) = struct.unpack_from(">i", buf, 0)
    off, out = 4, []
    n, off = dec_array_count(buf, off)
    for _ in range(max(n, 0)):
        t, off = dec_string(buf, off)
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        msg, off = dec_string(buf, off)
        out.append({"topic": t, "errorCode": err, "errorMessage": msg})
    return {"throttleMs": throttle, "results": out}


def encode_alter_reassignments_v0(topic: str, assignment: dict, timeout_ms: int = 30000) -> bytes:
    """AlterPartitionReassignments v0 使用 flexible 编码；调用时还须 flexible=True。"""
    partition_items = []
    for p in sorted(assignment):
        item = struct.pack(">i", int(p))
        item += enc_compact_array([struct.pack(">i", int(b)) for b in assignment[p]])
        item += enc_tagged_empty()
        partition_items.append(item)
    topic_item = enc_compact_string(topic) + enc_compact_array(partition_items)
    topic_item += enc_tagged_empty()
    return (struct.pack(">i", timeout_ms) + enc_compact_array([topic_item])
            + enc_tagged_empty())


def decode_alter_reassignments_v0(buf: bytes) -> dict:
    """解码 AlterPartitionReassignments v0 flexible response。"""
    off = skip_response_tags(buf, 0)
    (throttle,) = struct.unpack_from(">i", buf, off)
    off += 4
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    message, off = dec_compact_string(buf, off)
    n, off = dec_compact_len(buf, off)
    out = []
    for _ in range(max(n, 0)):
        t, off = dec_compact_string(buf, off)
        pn, off = dec_compact_len(buf, off)
        ps = []
        for _ in range(max(pn, 0)):
            (idx,) = struct.unpack_from(">i", buf, off)
            off += 4
            (perr,) = struct.unpack_from(">h", buf, off)
            off += 2
            pmsg, off = dec_compact_string(buf, off)
            off = skip_response_tags(buf, off)
            ps.append({"partition": idx, "errorCode": perr, "errorMessage": pmsg})
        off = skip_response_tags(buf, off)
        out.append({"topic": t, "partitions": ps})
    off = skip_response_tags(buf, off)
    return {"throttleMs": throttle, "errorCode": err,
            "errorMessage": message, "topics": out, "_end": off}


# ---------- DescribeConfigs (v0, api 32) / AlterConfigs (v0, api 33) ----------
# v0 非 flexible 编码，与本文件其他管理类 API 风格一致。
# resourceType: topic = 2。DescribeConfigs 返回 per-config 的 readOnly/isDefault/isSensitive。

def encode_describe_configs_v0(topic: str, names: list | None = None) -> bytes:
    """names 为 None 时查询该 Topic 全部配置。"""
    if names is None:
        name_part = struct.pack(">i", -1)
    else:
        name_part = enc_array([enc_string(n) for n in names])
    resource = struct.pack(">b", 2) + enc_string(topic) + name_part
    return enc_array([resource])


def decode_describe_configs_v0(buf: bytes) -> dict:
    (throttle,) = struct.unpack_from(">i", buf, 0)
    off, out = 4, []
    n, off = dec_array_count(buf, off)
    for _ in range(max(n, 0)):
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        msg, off = dec_string(buf, off)
        (rtype,) = struct.unpack_from(">b", buf, off)
        off += 1
        rname, off = dec_string(buf, off)
        cn, off = dec_array_count(buf, off)
        configs = []
        for _ in range(max(cn, 0)):
            cname, off = dec_string(buf, off)
            cvalue, off = dec_string(buf, off)
            (read_only,) = struct.unpack_from(">b", buf, off)
            off += 1
            (is_default,) = struct.unpack_from(">b", buf, off)
            off += 1
            (is_sensitive,) = struct.unpack_from(">b", buf, off)
            off += 1
            configs.append({"name": cname, "value": cvalue,
                            "readOnly": bool(read_only),
                            "isDefault": bool(is_default),
                            "isSensitive": bool(is_sensitive)})
        out.append({"errorCode": err, "errorMessage": msg,
                    "resourceType": rtype, "resourceName": rname,
                    "configs": configs})
    return {"throttleMs": throttle, "resources": out, "_end": off}


def encode_alter_configs_v0(topic: str, entries: dict) -> bytes:
    """entries: {name: value|None}；value 为 None 表示删除 Topic override（恢复默认）。"""
    items = []
    for name in sorted(entries):
        value = entries[name]
        items.append(enc_string(name) + enc_string(value))
    resource = struct.pack(">b", 2) + enc_string(topic) + enc_array(items)
    return enc_array([resource]) + b"\x00"  # validate_only = false


def decode_alter_configs_v0(buf: bytes) -> dict:
    (throttle,) = struct.unpack_from(">i", buf, 0)
    off, out = 4, []
    n, off = dec_array_count(buf, off)
    for _ in range(max(n, 0)):
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        msg, off = dec_string(buf, off)
        (rtype,) = struct.unpack_from(">b", buf, off)
        off += 1
        rname, off = dec_string(buf, off)
        out.append({"errorCode": err, "errorMessage": msg,
                    "resourceType": rtype, "resourceName": rname})
    return {"throttleMs": throttle, "resources": out, "_end": off}
