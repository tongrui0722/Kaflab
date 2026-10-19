"""采样存储：集群概览快照落库，供 trends 接口查询。

表结构与历史库 monitoring_samples.db 保持一致：
  samples(cluster, metric, labels, value, ts, series)
  counters(cluster, metric, labels, value, ts)
  collector(cluster, running, last_ok, last_error, updated)
"""
from __future__ import annotations

import json
import os
import sqlite3
import time

_DATA_DIR = os.environ.get("KCL_DATA_DIR") or os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(_DATA_DIR, "monitoring_samples.db")
RETENTION_DAYS = 3

CATALOG = [
    {"metric": "group_lag", "desc": "消费组 Lag（按组 + 集群合计）", "unit": "条"},
    {"metric": "produce_offset_rate", "desc": "生产位点总量（速率口径）", "unit": "位点"},
    {"metric": "consume_offset_rate", "desc": "消费提交位点总量（速率口径）", "unit": "位点"},
]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS samples(cluster TEXT, metric TEXT, labels TEXT, value REAL, ts INTEGER, series TEXT DEFAULT '__total__');
CREATE TABLE IF NOT EXISTS counters(cluster TEXT, metric TEXT, labels TEXT, value REAL, ts INTEGER, PRIMARY KEY(cluster, metric, labels));
CREATE TABLE IF NOT EXISTS collector(cluster TEXT PRIMARY KEY, running INTEGER, last_ok INTEGER, last_error TEXT, updated INTEGER);
CREATE INDEX IF NOT EXISTS idx_samples_lookup ON samples(cluster, metric, ts);
"""


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(_SCHEMA)
    return conn


def record_sample(cluster: str, metric: str, series: str, value: float, ts: int,
                  labels: dict | None = None) -> None:
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO samples(cluster, metric, labels, value, ts, series) VALUES(?,?,?,?,?,?)",
            (cluster, metric, json.dumps(labels or {}, ensure_ascii=False), float(value), int(ts), series),
        )
        cutoff = int(time.time()) - RETENTION_DAYS * 86400
        conn.execute("DELETE FROM samples WHERE ts < ?", (cutoff,))
        conn.commit()
    finally:
        conn.close()


def record_lag_snapshot(cluster: str, groups: list[dict], ts: int) -> None:
    """Atomically replace one cluster's Lag sample for a given second."""
    rows = [
        (cluster, "group_lag", json.dumps({"consumergroup": g["groupId"]}, ensure_ascii=False),
         float(g["totalLag"]), int(ts), g["groupId"])
        for g in groups
    ]
    # A successful ListGroups with no groups is a real zero, not missing data.
    if not rows:
        rows.append((cluster, "group_lag", "{}", 0.0, int(ts), "__total__"))
    conn = _conn()
    try:
        with conn:
            conn.execute("DELETE FROM samples WHERE cluster=? AND metric='group_lag' AND ts=?",
                         (cluster, int(ts)))
            conn.executemany(
                "INSERT INTO samples(cluster, metric, labels, value, ts, series) VALUES(?,?,?,?,?,?)",
                rows,
            )
            conn.execute("DELETE FROM samples WHERE ts < ?",
                         (int(time.time()) - RETENTION_DAYS * 86400,))
    finally:
        conn.close()


def query_points(cluster: str, metric: str, since_ts: int) -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT series, value, ts, labels FROM samples WHERE cluster=? AND metric=? AND ts>=? ORDER BY ts ASC",
            (cluster, metric, int(since_ts)),
        ).fetchall()
    finally:
        conn.close()
    return [{"series": r[0], "value": r[1], "ts": r[2], "labels": r[3]} for r in rows]


def set_collector(cluster: str, running: bool, last_ok: int | None, last_error: str | None) -> None:
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO collector(cluster, running, last_ok, last_error, updated) VALUES(?,?,?,?,?) "
            "ON CONFLICT(cluster) DO UPDATE SET running=excluded.running, last_ok=excluded.last_ok, "
            "last_error=excluded.last_error, updated=excluded.updated",
            (cluster, 1 if running else 0, last_ok, last_error, int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


def get_collector(cluster: str) -> dict:
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT running, last_ok, last_error FROM collector WHERE cluster=?", (cluster,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {"running": False, "last_ok": None, "last_error": None}
    return {"running": bool(row[0]), "last_ok": row[1], "last_error": row[2]}
