import asyncio
import json

import pytest
from fastapi import HTTPException

from app.routers import api


def test_first_login_uses_default_or_configured_credentials(tmp_path, monkeypatch):
    monkeypatch.delenv("KCL_AUTH_SOURCE", raising=False)
    users_file = tmp_path / "users.json"
    monkeypatch.setattr(api, "_USERS_FILE", str(users_file))
    monkeypatch.delenv("KCL_USERNAME", raising=False)
    monkeypatch.delenv("KCL_PASSWORD", raising=False)
    assert api._load_users() == [{"username": "admin", "password": "admin"}]
    assert asyncio.run(api.auth_login({"username": "admin", "password": "admin"}))["username"] == "admin"

    users_file.unlink()
    monkeypatch.setenv("KCL_USERNAME", "operator")
    monkeypatch.setenv("KCL_PASSWORD", "secret")
    assert api._load_users() == [{"username": "operator", "password": "secret"}]


def test_existing_users_are_not_replaced_by_environment(tmp_path, monkeypatch):
    monkeypatch.delenv("KCL_AUTH_SOURCE", raising=False)
    users_file = tmp_path / "users.json"
    users_file.write_text(json.dumps({"users": [{"username": "saved", "password": "old"}]}),
                          encoding="utf-8")
    monkeypatch.setattr(api, "_USERS_FILE", str(users_file))
    monkeypatch.setenv("KCL_USERNAME", "new")
    monkeypatch.setenv("KCL_PASSWORD", "new")
    assert api._load_users() == [{"username": "saved", "password": "old"}]
    assert json.loads(users_file.read_text(encoding="utf-8"))["users"][0]["username"] == "saved"


def test_invalid_users_file_never_falls_back_to_default(tmp_path, monkeypatch):
    monkeypatch.delenv("KCL_AUTH_SOURCE", raising=False)
    users_file = tmp_path / "users.json"
    users_file.write_text("invalid JSON", encoding="utf-8")
    monkeypatch.setattr(api, "_USERS_FILE", str(users_file))
    with pytest.raises(HTTPException) as error:
        api._load_users()
    assert error.value.status_code == 503
    assert users_file.read_text(encoding="utf-8") == "invalid JSON"


def test_container_credentials_follow_environment_even_with_existing_data(tmp_path, monkeypatch):
    users_file = tmp_path / "users.json"
    users_file.write_text("invalid JSON", encoding="utf-8")
    monkeypatch.setattr(api, "_USERS_FILE", str(users_file))
    monkeypatch.setenv("KCL_AUTH_SOURCE", "environment")
    monkeypatch.delenv("KCL_USERNAME", raising=False)
    monkeypatch.delenv("KCL_PASSWORD", raising=False)
    assert api._load_users() == [{"username": "admin", "password": "admin"}]
    assert asyncio.run(api.auth_login({"username": "admin", "password": "admin"}))["username"] == "admin"
    monkeypatch.setenv("KCL_USERNAME", "operator")
    assert api._load_users() == [{"username": "operator", "password": "admin"}]
    monkeypatch.setenv("KCL_PASSWORD", "secret")
    assert api._load_users() == [{"username": "operator", "password": "secret"}]
    assert asyncio.run(api.auth_login({"username": "operator", "password": "secret"}))["username"] == "operator"
    with pytest.raises(HTTPException) as error:
        asyncio.run(api.auth_login({"username": "admin", "password": "admin"}))
    assert error.value.status_code == 401
    assert users_file.read_text(encoding="utf-8") == "invalid JSON"
