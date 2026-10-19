"""后台 Lag 采样在没有请求或登录的情况下持续运行。"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import monitoring
from app.cluster.manager import ClusterConfig
from app.lag_collector import LagCollector


class Clusters:
    def __init__(self):
        self.items = [ClusterConfig(id="c1", name="test", bootstrap="broker:9092")]

    def list(self):
        return list(self.items)

    def get(self, cid):
        return next((item for item in self.items if item.id == cid), None)


class Consumer:
    def __init__(self, snapshot=None, delay=0):
        self.snapshot = snapshot or {"groups": [{"groupId": "g1", "totalLag": 12, "uncommitted": 0}]}
        self.delay = delay
        self.requests = []

    async def groups_overview(self, req):
        self.requests.append(req)
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.snapshot


def test_background_collector_writes_history_without_login(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    consumer = Consumer()
    collector = LagCollector(Clusters(), consumer)

    async def run():
        await collector.run_cycle()

    asyncio.run(run())
    points = monitoring.query_points("c1", "group_lag", int(time.time()) - 10)
    assert collector.interval_seconds == 60
    assert len(points) == 1
    assert points[0]["series"] == "g1" and points[0]["value"] == 12
    assert consumer.requests[0].requestTimeoutMs == 30_000
    assert monitoring.get_collector("c1") == {
        "running": True, "last_ok": points[0]["ts"], "last_error": None}


def test_empty_group_list_is_recorded_as_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    collector = LagCollector(Clusters(), Consumer({"groups": []}))
    asyncio.run(collector.run_cycle())
    points = monitoring.query_points("c1", "group_lag", int(time.time()) - 10)
    assert [(point["series"], point["value"]) for point in points] == [("__total__", 0)]


def test_disabled_cluster_is_not_sampled_and_stale_status_is_stopped(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    clusters = Clusters()
    clusters.items[0].monitoringEnabled = False
    monitoring.set_collector("c1", True, 123, None)
    consumer = Consumer()

    asyncio.run(LagCollector(clusters, consumer).run_cycle())

    assert consumer.requests == []
    assert monitoring.get_collector("c1") == {"running": False, "last_ok": 123, "last_error": None}


def test_disabling_during_an_active_sample_discards_its_result(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    clusters = Clusters()
    consumer = Consumer(delay=0.05)

    async def run():
        task = asyncio.create_task(LagCollector(clusters, consumer).run_cycle())
        await asyncio.sleep(0.01)
        clusters.items[0].monitoringEnabled = False
        await task

    asyncio.run(run())
    assert len(consumer.requests) == 1
    assert monitoring.query_points("c1", "group_lag", int(time.time()) - 10) == []


def test_same_second_snapshot_replaces_old_group_values(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    now = int(time.time())
    monitoring.record_lag_snapshot("c1", [{"groupId": "g1", "totalLag": 10}], now)
    monitoring.record_lag_snapshot("c1", [{"groupId": "g1", "totalLag": 15}], now)
    points = monitoring.query_points("c1", "group_lag", now - 1)
    assert len(points) == 1 and points[0]["value"] == 15


def test_trend_total_deduplicates_legacy_same_second_samples(tmp_path, monkeypatch):
    from app.routers import api

    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    monkeypatch.setattr(api, "_auth_mode", lambda _request: "readonly")
    now = int(time.time())
    monitoring.record_sample("c1", "group_lag", "__total__", 10, now,
                             {"consumergroup": "g1"})
    monitoring.record_sample("c1", "group_lag", "__total__", 15, now,
                             {"consumergroup": "g1"})
    request = SimpleNamespace(app=SimpleNamespace(
        state=SimpleNamespace(clusters=SimpleNamespace(get=lambda _cid: object()))))
    trend = asyncio.run(api.cluster_trends("c1", request))
    assert [(p["series"], p["value"]) for p in trend["points"]] == [
        ("g1", 15), ("__total__", 15)]


def test_timeout_preserves_last_good_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    old_ts = int(time.time()) - 5
    monitoring.record_lag_snapshot("c1", [{"groupId": "g1", "totalLag": 9}], old_ts)
    monitoring.set_collector("c1", True, old_ts, None)
    collector = LagCollector(Clusters(), Consumer(delay=0.1), timeout_seconds=0.01)
    asyncio.run(collector.run_cycle())
    assert len(monitoring.query_points("c1", "group_lag", old_ts - 1)) == 1
    status = monitoring.get_collector("c1")
    assert status["last_ok"] == old_ts and "超时" in status["last_error"]


def test_partial_broker_list_does_not_write_false_total(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    consumer = Consumer({"groups": [], "listGroupsErrors": ["broker unavailable"]})
    asyncio.run(LagCollector(Clusters(), consumer).run_cycle())
    assert monitoring.query_points("c1", "group_lag", int(time.time()) - 10) == []
    assert "采集不完整" in monitoring.get_collector("c1")["last_error"]


def test_scheduler_runs_repeatedly_and_stops(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "samples.db"))
    consumer = Consumer()
    collector = LagCollector(Clusters(), consumer, interval_seconds=0.03)

    async def run():
        collector.start()
        deadline = asyncio.get_running_loop().time() + 1
        while len(consumer.requests) < 2 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        await collector.stop()

    asyncio.run(run())
    assert len(consumer.requests) >= 2
    assert monitoring.get_collector("c1")["running"] is False


def test_app_lifecycle_starts_collector_without_login(monkeypatch):
    from app.main import app

    class StubCollector:
        started = False
        stopped = False

        def start(self):
            self.started = True

        async def stop(self):
            self.stopped = True

    stub = StubCollector()
    monkeypatch.setattr(app.state, "lag_collector", stub)

    async def run():
        async with app.router.lifespan_context(app):
            pass

    asyncio.run(run())
    assert stub.started and stub.stopped
