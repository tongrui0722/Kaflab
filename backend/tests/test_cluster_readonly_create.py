import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.routers import api


class ClusterStore:
    def __init__(self):
        self.saved = []

    def save(self, body):
        self.saved.append(body)
        return SimpleNamespace(id=body.get("id") or "new-cluster",
                               model_dump=lambda: {"id": body.get("id") or "new-cluster"})

    def delete(self, _cid):
        return True


def request_for(store, token=""):
    headers = [(b"authorization", f"Bearer {token}".encode())] if token else []
    app = SimpleNamespace(state=SimpleNamespace(clusters=store))
    return Request({"type": "http", "headers": headers, "app": app})


def test_readonly_can_create_but_cannot_edit_or_remove(monkeypatch):
    store = ClusterStore()
    monkeypatch.setattr(api, "_auth_tokens", {"read-token": "readonly", "full-token": "full"})
    read_request = request_for(store, "read-token")

    assert asyncio.run(api.save_cluster({"bootstrap": "broker:9092"}, read_request)) == {"id": "new-cluster"}
    assert len(store.saved) == 1

    with pytest.raises(HTTPException) as edit_error:
        asyncio.run(api.save_cluster({"id": "new-cluster", "bootstrap": "other:9092"}, read_request))
    assert edit_error.value.status_code == 403
    assert len(store.saved) == 1

    with pytest.raises(HTTPException) as remove_error:
        asyncio.run(api.delete_cluster("new-cluster", read_request))
    assert remove_error.value.status_code == 403

    assert asyncio.run(api.save_cluster({"id": "new-cluster", "bootstrap": "other:9092"},
                                        request_for(store, "full-token"))) == {"id": "new-cluster"}


def test_anonymous_cannot_create_cluster(monkeypatch):
    store = ClusterStore()
    monkeypatch.setattr(api, "_auth_tokens", {})
    with pytest.raises(HTTPException) as error:
        asyncio.run(api.save_cluster({"bootstrap": "broker:9092"}, request_for(store)))
    assert error.value.status_code == 401
    assert store.saved == []


def test_readonly_create_can_disable_lag_collection(monkeypatch):
    store = ClusterStore()
    monkeypatch.setattr(api, "_auth_tokens", {"read-token": "readonly"})
    monkeypatch.setattr(api.monitoring, "get_collector", lambda _cid: {"running": True, "last_ok": 123, "last_error": None})
    stopped = []
    monkeypatch.setattr(api.monitoring, "set_collector", lambda *args: stopped.append(args))

    asyncio.run(api.save_cluster({"bootstrap": "broker:9092", "monitoringEnabled": False},
                                 request_for(store, "read-token")))

    assert store.saved[0]["monitoringEnabled"] is False
    assert stopped == [("new-cluster", False, 123, None)]


def test_login_mode_switch_and_cluster_create_share_the_same_token(monkeypatch):
    store = ClusterStore()
    monkeypatch.setattr(api, "_auth_tokens", {})
    monkeypatch.setattr(api, "_load_users", lambda: [{"username": "admin", "password": "admin"}])

    async def run():
        login = await api.auth_login({"username": "admin", "password": "admin", "mode": "readonly"})
        request = request_for(store, login["token"])
        assert (await api.auth_switch_mode({"mode": "full"}, request))["mode"] == "full"
        return await api.save_cluster({"name": "test", "bootstrap": "broker:9092"}, request)

    assert asyncio.run(run()) == {"id": "new-cluster"}
    assert len(store.saved) == 1
