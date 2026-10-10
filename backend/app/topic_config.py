"""Topic 配置读写：DescribeConfigs / AlterConfigs（v0），供配置接口调用。"""
from __future__ import annotations

import struct
import time
import uuid

from app.models import SecurityConfig
from app.network.client import KafkaClient
from app.network.tcp import parse_hostport
from app.protocol import codec

# 常见配置的补充说明（DescribeConfigs v0 不返回默认值与文档，前端据此展示）
KNOWN: dict[str, dict] = {
    "cleanup.policy": {"type": "STRING", "default": "delete", "doc": "保留策略：delete 按时间/容量删除，compact 按 Key 压缩"},
    "retention.ms": {"type": "LONG", "default": "604800000", "doc": "消息保留时长（毫秒），-1 为永久保留"},
    "retention.bytes": {"type": "LONG", "default": "-1", "doc": "分区容量上限（字节），-1 为不限制"},
    "segment.ms": {"type": "LONG", "default": "604800000", "doc": "日志段滚动周期（毫秒）"},
    "segment.bytes": {"type": "LONG", "default": "1073741824", "doc": "日志段容量（字节）"},
    "max.message.bytes": {"type": "INT", "default": "1048588", "doc": "单条消息上限（字节，含 Key/Value/头）"},
    "compression.type": {"type": "STRING", "default": "producer", "doc": "压缩算法：producer 跟随生产者，或 gzip/snappy/lz4/zstd"},
    "min.insync.replicas": {"type": "INT", "default": "1", "doc": "acks=all 时最少 ISR 确认数"},
    "unclean.leader.election.enable": {"type": "BOOLEAN", "default": "false", "doc": "是否允许非 ISR 副本当选 Leader（可能丢消息）"},
    "flush.messages": {"type": "LONG", "default": "9223372036854775807", "doc": "刷盘前累计消息数"},
    "flush.ms": {"type": "LONG", "default": "9223372036854775807", "doc": "刷盘时间间隔（毫秒）"},
}

SENSITIVE_HINTS = ("password", "secret", "token", "keystore", "truststore", "sasl.jaas")


def _client(traces, bootstrap: str, sec: SecurityConfig, timeout_ms: int) -> KafkaClient:
    host, port = parse_hostport(bootstrap)
    return KafkaClient(traces, "sess-topic-config", f"tr-{uuid.uuid4().hex[:8]}",
                       "kafka-lab-config", timeout_ms, host, port, sec)


def _decode_alter_v0(buf: bytes) -> list[dict]:
    (throttle,) = struct.unpack_from(">i", buf, 0)
    off = 4
    (n,) = struct.unpack_from(">i", buf, off)
    off += 4
    out = []
    for _ in range(max(n, 0)):
        (err,) = struct.unpack_from(">h", buf, off)
        off += 2
        msg, off = codec.dec_string(buf, off)
        out.append({"errorCode": err, "errorMessage": msg})
    return {"throttleMs": throttle, "resources": out}


async def describe_topic_config(traces, bootstrap: str, topic: str, sec: SecurityConfig,
                                timeout_ms: int = 10000) -> list[dict]:
    client = _client(traces, bootstrap, sec, timeout_ms)
    try:
        await client.connect(1, label="config-bootstrap")
        resp, _, _ = await client.call(
            32, "DescribeConfigs", 0, codec.encode_describe_configs_v0(topic),
            4, "describeconfigs", decode=codec.decode_describe_configs_v0)
        resources = resp.get("resources") or []
        if not resources:
            return []
        res = resources[0]
        if res.get("errorCode") not in (0, None):
            raise RuntimeError(f"{res.get('errorCode')}: {res.get('errorMessage') or 'describe failed'}")
        items = []
        for c in res.get("configs") or []:
            name = c.get("name", "")
            known = KNOWN.get(name, {})
            sensitive = bool(c.get("isSensitive")) or any(h in name.lower() for h in SENSITIVE_HINTS)
            value = c.get("value")
            items.append({
                "name": name,
                "value": value,
                "defaultValue": known.get("default"),
                "effectiveValue": value,
                "isDefault": bool(c.get("isDefault", True)),
                "isSensitive": sensitive,
                "readOnly": bool(c.get("readOnly", False)),
                "type": known.get("type", "STRING"),
                "documentation": known.get("doc", ""),
            })
        return items
    finally:
        client.close()


async def alter_topic_config(traces, bootstrap: str, topic: str, changes: dict[str, str | None],
                             sec: SecurityConfig, timeout_ms: int = 10000) -> dict:
    client = _client(traces, bootstrap, sec, timeout_ms)
    try:
        await client.connect(1, label="config-bootstrap")
        resp, _, _ = await client.call(
            33, "AlterConfigs", 0, codec.encode_alter_configs_v0(topic, changes),
            4, "alterconfigs", decode=_decode_alter_v0)
        resources = (resp or {}).get("resources") or []
        bad = [(r.get("errorCode"), r.get("errorMessage")) for r in resources
               if r.get("errorCode") not in (0, None)]
        if bad:
            raise RuntimeError(f"{bad[0][0]}: {bad[0][1] or 'alter failed'}")
        return {"applied": sorted(changes), "ts": int(time.time())}
    finally:
        client.close()
