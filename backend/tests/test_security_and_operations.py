import asyncio
import json
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from starlette.requests import Request

from app.cluster import manager, secrets
from app.main import app
from app import monitoring
from app import operations as operation_module
from app.routers import api, group_reset


def test_api_login_boundary_and_redacted_cluster(monkeypatch):
    cluster = manager.ClusterConfig(id="managed", name="managed", bootstrap="broker:9092",
                                    securityProtocol="SASL_PLAINTEXT",
                                    saslUsername="user", saslPassword="test-secret")
    store = SimpleNamespace(list=lambda: [cluster], get=lambda cid: cluster if cid == "managed" else None)
    monkeypatch.setattr(app.state, "clusters", store)
    monkeypatch.setattr(api, "_load_users", lambda: [{"username": "tester", "password": "pass"}])
    with TestClient(app) as client:
        assert client.get("/api/clusters").status_code == 401
        assert client.get("/api/clusters/managed/topics").status_code == 401
        login = client.post("/api/auth/login", json={"username": "tester", "password": "pass"})
        assert login.status_code == 200
        headers = {"Authorization": "Bearer " + login.json()["token"]}
        rows = client.get("/api/clusters", headers=headers).json()
        assert rows[0]["hasSaslPassword"] is True
        assert "saslPassword" not in rows[0]
        assert "test-secret" not in json.dumps(rows)
        token = login.json()["token"]
        api._auth_users[token]["clusters"] = ["managed"]
        assert client.get("/api/clusters/other/topics", headers=headers).status_code == 403
        assert client.get("/api/operations?clusterId=other", headers=headers).status_code == 403
        assert client.get("/api/operations?clusterId=managed", headers=headers).status_code == 200
        assert client.get("/api/traces", headers=headers).status_code == 403
        assert client.get("/api/client-sim/runs", headers=headers).status_code == 403
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/session/s", subprotocols=["kcl", "token." + token]):
                pass
        api._auth_users[token]["operations"] = []
        assert client.post("/api/clusters/managed/topics/t/delete-task", headers=headers).status_code == 403
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/session/s", subprotocols=["kcl", "token.invalid"]):
                pass
        assert client.post("/api/auth/logout", headers=headers).status_code == 200
        assert client.get("/api/clusters", headers=headers).status_code == 401


def test_legacy_cluster_credentials_are_encrypted_on_load(tmp_path, monkeypatch):
    monkeypatch.setenv("KCL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(manager, "_STORE", str(tmp_path / "clusters.json"))
    monkeypatch.setattr(manager, "_BROKER_STORE", str(tmp_path / "broker_nodes.json"))
    (tmp_path / "clusters.json").write_text(json.dumps([{
        "id": "a", "name": "A", "bootstrap": "broker:9092",
        "securityProtocol": "SASL_PLAINTEXT", "saslPassword": "plain-secret"}]), encoding="utf-8")
    store = manager.ClusterManager()
    assert store.get("a").saslPassword == "plain-secret"
    raw = (tmp_path / "clusters.json").read_text(encoding="utf-8")
    assert "plain-secret" not in raw
    assert "saslPasswordEnc" in raw
    assert (tmp_path / ".kaflab.key").exists()
    assert manager.ClusterManager().get("a").saslPassword == "plain-secret"
    assert secrets.decrypt_secret(json.loads(raw)[0]["saslPasswordEnc"]) == "plain-secret"


def test_operation_retries_and_persists(tmp_path, monkeypatch):
    monkeypatch.setattr(operation_module, "DB_PATH", tmp_path / "operations.db")
    monkeypatch.setattr(operation_module, "_RETRY_DELAYS", (0,))
    mgr = operation_module.OperationManager()
    calls = []
    present = [True]

    async def exists(_request, _cluster, _topic):
        return present[0]
    async def delete(_cluster, _topic, _request):
        calls.append(1)
        if len(calls) == 1:
            raise HTTPException(503, "NOT_CONTROLLER")
        present[0] = False
        return {"deleted": "topic"}
    monkeypatch.setattr(mgr, "_exists", exists)
    monkeypatch.setattr(api, "delete_topic_once", delete)

    async def run():
        task = mgr.create_delete(app, "managed", "topic", "tester")
        await asyncio.gather(*list(mgr._running))
        return task["id"]
    task_id = asyncio.run(run())
    saved = operation_module.OperationManager().get(task_id)
    assert saved["status"] == "done"
    assert saved["attempts"] == 2
    assert len(calls) == 2
    assert any(event["type"] == "retrying" for event in saved["events"])


def test_monitoring_records_alert_and_collection_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "monitoring.db"))
    data = {"brokerOnline": 3, "brokerOffline": 0, "topicTotal": 2,
            "topicOffline": 0, "topicUnderReplicated": 0, "controllerId": 1}
    monitoring.record_health_snapshot("a", data, int(time.time()) - 1)
    monitoring.record_health_snapshot("a", {**data, "brokerOnline": 2, "brokerOffline": 1}, int(time.time()))
    monitoring.record_health_error("a", "probe failed")
    view = monitoring.health_view("a", 0)
    assert view["latest"]["broker_offline"] == 1
    assert view["collector"]["last_error"] == "probe failed"
    assert {event["kind"] for event in view["events"]} >= {"brokerOffline_alert", "collection_failed"}


def test_lag_alert_records_threshold_and_collection_recovery(tmp_path, monkeypatch):
    monkeypatch.setattr(monitoring, "DB_PATH", str(tmp_path / "monitoring.db"))
    monitoring.save_alert_rules("a", {"broker_offline": 1, "topic_offline": 1,
                                      "topic_under_replicated": 1, "lag_threshold": 10})
    ts = int(time.time()) - 2
    monitoring.record_lag_snapshot("a", [{"groupId": "g", "totalLag": 15}], ts)
    monitoring.set_collector("a", True, ts, "Lag 采样失败：timeout")
    monitoring.set_collector("a", True, ts + 1, None)
    monitoring.record_lag_snapshot("a", [{"groupId": "g", "totalLag": 2}], ts + 1)
    kinds = {event["kind"] for event in monitoring.health_view("a", 0)["events"]}
    assert {"lag_alert", "lag_recovered", "lag_collection_failed",
            "lag_collection_recovered"} <= kinds


def test_topic_page_server_filter_cache_and_refresh(monkeypatch):
    rows = [{"topic": name, "partitions": 1, "replication": 1,
             "hasOffline": name == "bad", "hasUnderReplicated": False}
            for name in ("bad", "good", "great")]
    calls = []
    async def all_topics(_cid, _request):
        calls.append(1)
        return {"topics": rows, "brokers": [{"id": 1}]}
    monkeypatch.setattr(api, "cluster_topics", all_topics)
    api._topic_cache.clear()
    cluster = SimpleNamespace(id="a")
    store = SimpleNamespace(get=lambda _cid: cluster)
    request = Request({"type": "http", "headers": [], "app": SimpleNamespace(state=SimpleNamespace(clusters=store))})
    async def run():
        first = await api.cluster_topics_page("a", request, 1, 1, "g", "all", "topic", True, False)
        second = await api.cluster_topics_page("a", request, 2, 1, "g", "all", "topic", True, False)
        refreshed = await api.cluster_topics_page("a", request, 1, 1, "g", "all", "topic", True, True)
        return first, second, refreshed
    first, second, refreshed = asyncio.run(run())
    assert [row["topic"] for row in first["topics"]] == ["good"]
    assert [row["topic"] for row in second["topics"]] == ["great"]
    assert first["total"] == 2 and second["cached"] is True and refreshed["cached"] is False
    assert len(calls) == 2


def test_offset_reset_requires_inactive_group_and_confirmation(monkeypatch):
    cluster = manager.ClusterConfig(id="a", bootstrap="broker:9092")
    store = SimpleNamespace(get=lambda _cid: cluster)
    rows = [{"topic": "t", "partition": 0, "leader": 1, "end": 10, "committed": 3}]
    class FakeConsumer:
        calls = 0
        async def describe_group(self, _req):
            return {"group": {"state": "Empty", "members": [], "errorCode": 0}}
        async def group_lag(self, _req, _topics):
            self.calls += 1
            return {"rows": rows if self.calls < 3 else [{**rows[0], "committed": 10}]}
    request = Request({"type": "http", "headers": [],
        "app": SimpleNamespace(state=SimpleNamespace(clusters=store, consumer=FakeConsumer()))})
    monkeypatch.setattr(group_reset, "_need_lab", lambda _request: None)
    monkeypatch.setattr(group_reset, "principal", lambda _request: {"username": "tester"})
    audit = []
    monkeypatch.setattr(group_reset, "_audit", lambda item, actor, results: audit.append(results))
    class FakeClient:
        async def connect(self, *_a, **_kw):
            pass
        async def call(self, key, *_a, **_kw):
            return ({"errorCode": 0, "host": "broker", "port": 9092, "nodeId": 1}
                    if key == 10 else {"topics": [{"partitions": [{"errorCode": 0}]}]}), None, None
        def close(self):
            pass
    monkeypatch.setattr(group_reset, "_cluster_client", lambda *_a, **_kw: (cluster, FakeClient()))
    async def run():
        preview = await group_reset.offset_reset_preview("a", "g",
            group_reset.PreviewBody(topic="t", target="latest"), request)
        with pytest.raises(HTTPException):
            await group_reset.offset_reset_execute("a", "g",
                group_reset.ExecuteBody(previewToken=preview["previewToken"], confirmGroupId="wrong"), request)
        return await group_reset.offset_reset_execute("a", "g",
            group_reset.ExecuteBody(previewToken=preview["previewToken"], confirmGroupId="g"), request)
    result = asyncio.run(run())
    assert result["status"] == "done"
    assert result["results"][0]["target"] == 10
    assert audit
