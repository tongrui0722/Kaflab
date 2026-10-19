from __future__ import annotations

import importlib.util
import asyncio
import io
import json
from pathlib import Path
import sys
import types

import pytest
from pydantic import ValidationError

from app.client_sim.models import RunCreate
from app.client_sim.manager import ClientSimManager
from app.client_sim.topics import TopicConnection


def _admin_module():
    path = Path(__file__).resolve().parents[1] / "client_runners" / "topic_admin.py"
    spec = importlib.util.spec_from_file_location("test_topic_admin_script", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_topic_creation_is_producer_only_and_validated():
    with pytest.raises(ValidationError):
        RunCreate(kind="consumer", client="python", version="2.16.0",
                  bootstrap="localhost:9092", topic="new", autoCreateTopic=True)
    with pytest.raises(ValidationError):
        RunCreate(kind="producer", client="python", version="2.16.0",
                  bootstrap="localhost:9092", topic="bad topic", autoCreateTopic=True)
    with pytest.raises(ValidationError):
        TopicConnection(bootstrap="localhost:9092", securityProtocol="SASL_PLAINTEXT")


@pytest.mark.parametrize("existing,expected_created", [(True, False), (False, True)])
def test_admin_uses_existing_topic_or_creates_missing_topic(monkeypatch, capsys, existing, expected_created):
    created = []

    class FakeAdmin:
        def __init__(self, config):
            assert config["bootstrap.servers"] == "localhost:9092"

        def list_topics(self, timeout):
            assert timeout == 9
            names = {"sample": types.SimpleNamespace(error=None)} if existing else {}
            return types.SimpleNamespace(topics=names, brokers={1: object(), 2: object()})

        def create_topics(self, items, **kwargs):
            created.extend(items)
            return {"sample": types.SimpleNamespace(result=lambda timeout: None)}

    class FakeNewTopic:
        def __init__(self, name, num_partitions, replication_factor):
            self.name = name
            self.partitions = num_partitions
            self.replicas = replication_factor

    monkeypatch.setitem(sys.modules, "confluent_kafka", types.ModuleType("confluent_kafka"))
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", types.SimpleNamespace(AdminClient=FakeAdmin, NewTopic=FakeNewTopic))
    payload = {"action": "ensure", "bootstrap": "localhost:9092", "securityProtocol": "PLAINTEXT",
               "topic": "sample", "topicPartitions": 3, "topicReplicationFactor": 2}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload) + "\n"))
    _admin_module().main()
    result = json.loads(capsys.readouterr().out)
    assert result == {"topic": "sample", "created": expected_created}
    assert len(created) == int(expected_created)
    if created:
        assert (created[0].name, created[0].partitions, created[0].replicas) == ("sample", 3, 2)


def test_admin_rejects_replication_above_broker_count(monkeypatch):
    class FakeAdmin:
        def __init__(self, config):
            pass

        def list_topics(self, timeout):
            return types.SimpleNamespace(topics={}, brokers={1: object()})

    monkeypatch.setitem(sys.modules, "confluent_kafka", types.ModuleType("confluent_kafka"))
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", types.SimpleNamespace(AdminClient=FakeAdmin, NewTopic=object))
    payload = {"action": "ensure", "bootstrap": "localhost:9092", "securityProtocol": "PLAINTEXT",
               "topic": "sample", "topicPartitions": 1, "topicReplicationFactor": 2}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload) + "\n"))
    with pytest.raises(ValueError, match="超过当前集群 Broker 数"):
        _admin_module().main()


def test_creation_error_does_not_start_client_process(monkeypatch, tmp_path):
    async def rejected(*_args):
        raise ValueError("没有创建 Topic 的权限")

    monkeypatch.setattr("app.client_sim.manager.topic_admin", rejected)
    manager = ClientSimManager(tmp_path / "runs")
    spec = RunCreate(kind="producer", client="python", version="2.16.0",
                     bootstrap="localhost:9092", topic="missing", autoCreateTopic=True)
    with pytest.raises(ValueError, match="没有创建 Topic 的权限"):
        asyncio.run(manager.start(spec))
    assert manager.list() == []
    assert manager.processes == {}
