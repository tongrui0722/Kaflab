"""Topic 配置与采样存储的纯逻辑单测（不连真实集群）。"""
from __future__ import annotations

import json
import struct
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import monitoring as mon
from app.topic_config import KNOWN, SENSITIVE_HINTS, _decode_alter_v0


def _alter_frame(entries: list[tuple[int, str | None]]) -> bytes:
    buf = struct.pack(">i", 5)
    buf += struct.pack(">i", len(entries))
    for code, msg in entries:
        buf += struct.pack(">h", code)
        raw = (msg or "").encode()
        buf += struct.pack(">h", len(raw)) + raw
    return buf


def test_decode_alter_ok():
    dec = _decode_alter_v0(_alter_frame([(0, None)]))
    assert dec["resources"][0]["errorCode"] == 0


def test_decode_alter_error():
    dec = _decode_alter_v0(_alter_frame([(88, "boom")]))
    assert dec["resources"][0]["errorCode"] == 88


def test_known_docs_cover_common():
    for name in ("cleanup.policy", "retention.ms", "max.message.bytes",
                 "compression.type", "min.insync.replicas"):
        assert KNOWN[name].get("doc"), name


def test_sensitive_hints():
    assert any(h in "ssl.keystore.password" for h in SENSITIVE_HINTS)


def test_monitoring_roundtrip(tmp_path, monkeypatch):
    import time as _t
    now = int(_t.time())
    monkeypatch.setattr(mon, "DB_PATH", str(tmp_path / "t.db"))
    mon.record_sample("c1", "group_lag", "__total__", 12.0, now,
                      {"consumergroup": "g1"})
    pts = mon.query_points("c1", "group_lag", now - 10)
    assert len(pts) == 1
    assert pts[0]["series"] == "__total__"
    assert json.loads(pts[0]["labels"])["consumergroup"] == "g1"


def test_collector_status(tmp_path, monkeypatch):
    import time as _t
    now = int(_t.time())
    monkeypatch.setattr(mon, "DB_PATH", str(tmp_path / "t.db"))
    assert mon.get_collector("c9")["running"] is False
    mon.set_collector("c9", True, now, None)
    got = mon.get_collector("c9")
    assert got["running"] is True and got["last_ok"] == now
