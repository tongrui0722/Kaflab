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
CREATE TABLE IF NOT EXISTS health_samples(cluster TEXT, ts INTEGER, broker_online INTEGER,
 broker_offline INTEGER, topic_total INTEGER, topic_offline INTEGER, topic_under_replicated INTEGER,
 controller_id INTEGER, PRIMARY KEY(cluster, ts));
CREATE TABLE IF NOT EXISTS health_collector(cluster TEXT PRIMARY KEY, last_ok INTEGER,
 last_error TEXT, updated INTEGER);
CREATE TABLE IF NOT EXISTS alert_rules(cluster TEXT PRIMARY KEY, broker_offline INTEGER DEFAULT 1,
 topic_offline INTEGER DEFAULT 1, topic_under_replicated INTEGER DEFAULT 1, lag_threshold INTEGER);
CREATE TABLE IF NOT EXISTS alert_events(id INTEGER PRIMARY KEY AUTOINCREMENT, cluster TEXT, ts INTEGER,
 kind TEXT, message TEXT);
CREATE INDEX IF NOT EXISTS idx_health_samples ON health_samples(cluster, ts);
CREATE INDEX IF NOT EXISTS idx_alert_events ON alert_events(cluster, ts);
"""


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
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
            threshold_row = conn.execute(
                "SELECT lag_threshold FROM alert_rules WHERE cluster=?", (cluster,)
            ).fetchone()
            threshold = threshold_row[0] if threshold_row else None
            if threshold is not None:
                previous_ts = conn.execute(
                    "SELECT MAX(ts) FROM samples WHERE cluster=? AND metric='group_lag' AND ts<?",
                    (cluster, int(ts)),
                ).fetchone()[0]
                previous_total = 0.0
                if previous_ts is not None:
                    previous_total = conn.execute(
                        "SELECT COALESCE(SUM(value),0) FROM samples "
                        "WHERE cluster=? AND metric='group_lag' AND ts=?",
                        (cluster, previous_ts),
                    ).fetchone()[0]
                total = sum(row[3] for row in rows)
                if (previous_total >= threshold) != (total >= threshold):
                    is_alert = total >= threshold
                    conn.execute(
                        "INSERT INTO alert_events(cluster,ts,kind,message) VALUES(?,?,?,?)",
                        (cluster, int(ts), "lag_alert" if is_alert else "lag_recovered",
                         "消费组 Lag 合计：" + str(int(total))),
                    )
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
        prior = conn.execute(
            "SELECT last_error FROM collector WHERE cluster=?", (cluster,)
        ).fetchone()
        was_failed = bool(prior and prior[0] and str(prior[0]).startswith("Lag "))
        is_failed = bool(last_error and last_error.startswith("Lag "))
        if was_failed != is_failed:
            conn.execute(
                "INSERT INTO alert_events(cluster,ts,kind,message) VALUES(?,?,?,?)",
                (cluster, int(time.time()),
                 "lag_collection_failed" if is_failed else "lag_collection_recovered",
                 (last_error or "Lag 采集恢复")[:250]),
            )
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


def record_health_snapshot(cluster: str, data: dict, ts: int) -> list[dict]:
    events: list[dict] = []
    conn = _conn()
    try:
        with conn:
            previous = conn.execute(
                "SELECT broker_offline,topic_offline,topic_under_replicated FROM health_samples WHERE cluster=? ORDER BY ts DESC LIMIT 1",
                (cluster,)).fetchone()
            rules = conn.execute(
                "SELECT broker_offline,topic_offline,topic_under_replicated FROM alert_rules WHERE cluster=?",
                (cluster,)).fetchone()
            thresholds = tuple(rules) if rules else (1, 1, 1)
            for index, key in enumerate(("brokerOffline", "topicOffline", "topicUnderReplicated")):
                old = previous[index] if previous else 0
                new = data[key]
                if (old >= thresholds[index]) != (new >= thresholds[index]):
                    kind = key + ("_alert" if new >= thresholds[index] else "_recovered")
                    message = key + ": " + str(new)
                    conn.execute("INSERT INTO alert_events(cluster,ts,kind,message) VALUES(?,?,?,?)",
                                 (cluster, ts, kind, message))
                    events.append({"cluster": cluster, "ts": ts, "kind": kind, "message": message})
            conn.execute("""INSERT OR REPLACE INTO health_samples
                (cluster,ts,broker_online,broker_offline,topic_total,topic_offline,
                 topic_under_replicated,controller_id) VALUES(?,?,?,?,?,?,?,?)""",
                (cluster, ts, data["brokerOnline"], data["brokerOffline"], data["topicTotal"],
                 data["topicOffline"], data["topicUnderReplicated"], data["controllerId"]))
            prior_collection = conn.execute(
                "SELECT last_error FROM health_collector WHERE cluster=?", (cluster,)
            ).fetchone()
            if prior_collection and prior_collection[0]:
                conn.execute(
                    "INSERT INTO alert_events(cluster,ts,kind,message) VALUES(?,?,?,?)",
                    (cluster, ts, "collection_recovered", "健康采集恢复"),
                )
            conn.execute("""INSERT INTO health_collector(cluster,last_ok,last_error,updated)
                VALUES(?,?,NULL,?) ON CONFLICT(cluster) DO UPDATE SET
                last_ok=excluded.last_ok,last_error=NULL,updated=excluded.updated""",
                (cluster, ts, ts))
            conn.execute("DELETE FROM health_samples WHERE ts < ?",
                         (int(time.time()) - RETENTION_DAYS * 86400,))
    finally:
        conn.close()
    return events


def record_health_error(cluster: str, message: str) -> dict | None:
    now = int(time.time())
    event = None
    conn = _conn()
    try:
        with conn:
            prior = conn.execute("SELECT last_ok,last_error FROM health_collector WHERE cluster=?",
                                 (cluster,)).fetchone()
            conn.execute("""INSERT INTO health_collector(cluster,last_ok,last_error,updated)
                VALUES(?,?,?,?) ON CONFLICT(cluster) DO UPDATE SET
                last_error=excluded.last_error,updated=excluded.updated""",
                (cluster, prior[0] if prior else None, message[:250], now))
            if not prior or prior[1] is None:
                conn.execute("INSERT INTO alert_events(cluster,ts,kind,message) VALUES(?,?,?,?)",
                             (cluster, now, "collection_failed", message[:250]))
                event = {"cluster": cluster, "ts": now, "kind": "collection_failed",
                         "message": message[:250]}
    finally:
        conn.close()
    return event


def health_view(cluster: str, since: int) -> dict:
    conn = _conn()
    try:
        latest = conn.execute("SELECT * FROM health_samples WHERE cluster=? ORDER BY ts DESC LIMIT 1",
                              (cluster,)).fetchone()
        points = conn.execute("SELECT * FROM health_samples WHERE cluster=? AND ts>=? ORDER BY ts",
                              (cluster, since)).fetchall()
        collector = conn.execute("SELECT * FROM health_collector WHERE cluster=?", (cluster,)).fetchone()
        rule = conn.execute("SELECT * FROM alert_rules WHERE cluster=?", (cluster,)).fetchone()
        events = conn.execute("SELECT ts,kind,message FROM alert_events WHERE cluster=? ORDER BY ts DESC LIMIT 30",
                              (cluster,)).fetchall()
    finally:
        conn.close()
    return {"latest": dict(latest) if latest else None,
            "points": [dict(row) for row in points],
            "collector": dict(collector) if collector else None,
            "rules": dict(rule) if rule else {"broker_offline": 1, "topic_offline": 1,
                                               "topic_under_replicated": 1, "lag_threshold": None},
            "events": [dict(row) for row in events]}


def save_alert_rules(cluster: str, rules: dict) -> dict:
    values = [rules.get(key) for key in ("broker_offline", "topic_offline",
                                        "topic_under_replicated", "lag_threshold")]
    conn = _conn()
    try:
        with conn:
            conn.execute("""INSERT INTO alert_rules VALUES(?,?,?,?,?)
                ON CONFLICT(cluster) DO UPDATE SET broker_offline=excluded.broker_offline,
                topic_offline=excluded.topic_offline,
                topic_under_replicated=excluded.topic_under_replicated,
                lag_threshold=excluded.lag_threshold""", (cluster, *values))
    finally:
        conn.close()
    return health_view(cluster, 0)["rules"]
