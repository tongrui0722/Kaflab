from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import pytest
from pydantic import ValidationError
from fastapi import HTTPException
from starlette.requests import Request

from app.client_sim.manager import ClientSimManager
from app.client_sim.models import RunCreate
from app.client_sim.registry import SLOTS, catalog
from app.client_sim.routes import _authorize
from app.client_sim.config_schema import config_schema
from app.routers.api import _auth_tokens
from client_runners.python_client import options as python_options


def test_catalog_never_marks_uninstalled_slot_ready():
    entries = catalog()
    assert {entry["id"] for entry in entries} == set(SLOTS)
    for entry in entries:
        assert {slot["version"] for slot in entry["versions"]} == set(SLOTS[entry["id"]])


def test_consumer_rejects_offset_commit_for_direct_partition():
    with pytest.raises(ValidationError):
        RunCreate(kind="consumer", client="python", version="2.16.0", bootstrap="localhost:9092",
                  topic="test", partition=0, commit=True)


def test_kafkajs_rejects_unsupported_direct_partition():
    with pytest.raises(ValidationError):
        RunCreate(kind="consumer", client="node", version="2.2.4", bootstrap="localhost:9092",
                  topic="test", partition=0)


def test_public_run_config_excludes_message_and_credentials():
    spec = RunCreate(kind="producer", client="python", version="2.16.0", bootstrap="localhost:9092",
                     topic="test", securityProtocol="SASL_SSL", saslUsername="user",
                     saslPassword="private", key="sensitive-key", value="sensitive-value")
    snapshot = spec.public_snapshot()
    assert "private" not in json.dumps(snapshot)
    assert "sensitive-key" not in json.dumps(snapshot)
    assert "sensitive-value" not in json.dumps(snapshot)


def test_simulator_accepts_longer_bounded_runs():
    spec = RunCreate(kind="producer", client="python", version="2.16.0",
                     bootstrap="localhost:9092", topic="test", count=100_000,
                     durationSeconds=6_000, rate=100_000 / 6_000)
    assert spec.count == 100_000
    assert spec.durationSeconds == 6_000
    with pytest.raises(ValidationError):
        RunCreate(kind="producer", client="python", version="2.16.0",
                  bootstrap="localhost:9092", topic="test", count=1_000_001)


def test_advanced_client_parameters_are_validated_and_forwarded():
    fields = {"kind": "producer", "client": "python", "version": "2.16.0",
              "bootstrap": "localhost:9092", "topic": "test"}
    spec = RunCreate(**fields, connectionTimeoutMs=7000, requestTimeoutMs=45000,
                     maxMessageBytes=8192, retries=5)
    config = python_options({"runId": "test-run", **spec.model_dump()})
    assert config["socket.connection.setup.timeout.ms"] == 7000
    assert config["socket.timeout.ms"] == 45000
    assert spec.public_snapshot()["maxMessageBytes"] == 8192
    for invalid in ({"connectionTimeoutMs": 999}, {"requestTimeoutMs": 120001},
                    {"maxMessageBytes": 1023}, {"retries": 0},
                    {"maxFetchBytes": 999999}, {"fetchMaxWaitMs": 10001}):
        with pytest.raises(ValidationError):
            RunCreate(**fields, **invalid)


def test_version_aware_config_templates_and_whitelist():
    java3 = {option["key"]: option for option in config_schema("java", "3.9.1", "producer")}
    java4 = {option["key"]: option for option in config_schema("java", "4.0.1", "producer")}
    assert java3["linger.ms"]["value"] == 0
    assert java4["linger.ms"]["value"] == 5
    node1 = {option["key"]: option for option in config_schema("node", "1.16.0", "producer")}
    node2 = {option["key"]: option for option in config_schema("node", "2.2.4", "producer")}
    assert node1["partitioner"]["value"] == "legacy"
    assert node2["partitioner"]["value"] == "default"
    spec = RunCreate(kind="producer", client="java", version="4.0.1",
                     bootstrap="localhost:9092", topic="test",
                     clientConfig={"linger.ms": 20, "batch.size": 32768, "acks": "1"})
    assert spec.public_snapshot()["clientConfig"]["linger.ms"] == 20
    for config in ({"unknown.option": 1}, {"linger.ms": 10001}, {"acks": "0"},
                   {"request.timeout.ms": 50000, "delivery.timeout.ms": 30000}):
        with pytest.raises(ValidationError):
            RunCreate(kind="producer", client="java", version="4.0.1",
                      bootstrap="localhost:9092", topic="test", clientConfig=config)
    with pytest.raises(ValidationError):
        RunCreate(kind="producer", client="node", version="1.16.0",
                  bootstrap="localhost:9092", topic="test", clientConfig={"partitioner": "default"})


def test_simulator_uses_existing_readonly_and_full_permissions():
    token = "client-sim-test-token"
    request = Request({"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]})
    with pytest.raises(HTTPException) as missing:
        _authorize(request)
    assert missing.value.status_code == 401
    try:
        _auth_tokens[token] = "readonly"
        _authorize(request)
        with pytest.raises(HTTPException) as readonly:
            _authorize(request, write=True)
        assert readonly.value.status_code == 403
        _auth_tokens[token] = "full"
        _authorize(request, write=True)
    finally:
        _auth_tokens.pop(token, None)


def test_raw_process_output_is_preserved_and_redacted(tmp_path: Path):
    manager = ClientSimManager(tmp_path / "runs")
    run = {"id": "raw-test", "logSeq": 0, "logBytes": 0, "logTruncated": False,
           "sent": 0, "received": 0, "errors": 0, "_secret": "private-value"}
    manager.runs[run["id"]] = run
    library_log = json.dumps({"level": "INFO", "logger": "kafkajs", "message": "connected"})

    async def scenario():
        stdout = asyncio.StreamReader()
        original = json.dumps({"event": "sent", "message": "secret=private-value",
                               "offset": 7, "detail": "private-value"})
        stdout.feed_data((original + "\n" + library_log + "\n").encode())
        stdout.feed_eof()
        await manager._read_stream(run, stdout, "stdout")

        stderr = asyncio.StreamReader()
        stderr.feed_data(b"  broker warning private-value  \n")
        stderr.feed_eof()
        await manager._read_stream(run, stderr, "stderr")

    asyncio.run(scenario())
    entries = manager.logs(run["id"])["entries"]
    assert entries[0]["event"] == "sent"
    assert entries[0]["source"] == "adapter"
    assert entries[0]["raw"].startswith('{"event": "sent"')
    assert entries[1]["source"] == "client"
    assert entries[1]["raw"] == library_log
    assert "event" not in entries[1]
    assert entries[2]["raw"].startswith("  broker warning")
    assert "private-value" not in json.dumps(entries)
    text = manager.raw_logs(run["id"])
    assert library_log in text
    assert "  broker warning [REDACTED]  " in text
    assert '"event": "sent"' not in text
    assert "[REDACTED]" in text and "private-value" not in text


def test_run_process_logs_are_persisted_and_secret_is_redacted(tmp_path: Path, monkeypatch):
    child = tmp_path / "child.py"
    child.write_text(
        "import json,sys\n"
        "spec=json.loads(sys.stdin.readline())\n"
        "print('native startup',file=sys.stderr,flush=True)\n"
        "print(json.dumps({'event':'ready','message':'started'}),flush=True)\n"
        "print(json.dumps({'event':'sent','message':'secret='+spec['saslPassword'],'offset':7}),flush=True)\n"
        "print(json.dumps({'event':'done','message':'complete'}),flush=True)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("app.client_sim.manager.command_for", lambda *_: ([sys.executable, str(child)], {}))

    async def scenario():
        manager = ClientSimManager(tmp_path / "runs")
        run = await manager.start(RunCreate(kind="producer", client="python", version="2.16.0",
                                            bootstrap="localhost:9092", topic="test",
                                            securityProtocol="SASL_PLAINTEXT", saslUsername="user",
                                            saslPassword="private-value"))
        for _ in range(100):
            await asyncio.sleep(0.02)
            if manager.get(run["id"])["status"] not in ("starting", "running"):
                break
        result = manager.get(run["id"])
        assert result["status"] == "completed"
        assert result["sent"] == 1
        assert "_secret" not in manager.list()[0]
        logs = manager.logs(run["id"])["entries"]
        assert any(entry.get("event") == "sent" and entry.get("offset") == 7 for entry in logs)
        assert "native startup" in manager.raw_logs(run["id"])
        assert "started" not in manager.raw_logs(run["id"])
        assert "private-value" not in json.dumps(logs)
        assert "private-value" not in (tmp_path / "runs" / f"{run['id']}.json").read_text()
        assert ClientSimManager(tmp_path / "runs").get(run["id"])["status"] == "completed"

    asyncio.run(scenario())


def test_stop_terminates_local_process(tmp_path: Path, monkeypatch):
    child = tmp_path / "waiting.py"
    child.write_text("import sys,time\nsys.stdin.readline()\ntime.sleep(30)\n", encoding="utf-8")
    monkeypatch.setattr("app.client_sim.manager.command_for", lambda *_: ([sys.executable, str(child)], {}))

    async def scenario():
        manager = ClientSimManager(tmp_path / "runs")
        run = await manager.start(RunCreate(kind="consumer", client="python", version="2.16.0",
                                            bootstrap="localhost:9092", topic="test"))
        await manager.stop(run["id"])
        for _ in range(100):
            await asyncio.sleep(0.02)
            if manager.get(run["id"])["status"] == "stopped":
                break
        assert manager.get(run["id"])["status"] == "stopped"
        assert manager.processes.get(run["id"]) is None

    asyncio.run(scenario())
