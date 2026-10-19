import asyncio

from app.models import SecurityConfig
from app.network import client as client_module
from app.producer import loadtest


def test_loadtest_tries_next_bootstrap_and_uses_selected_address(monkeypatch):
    attempts = []
    closed = []

    class FakeClient:
        def __init__(self, _traces, _session_id, _trace_id, _client_id,
                     _timeout_ms, host, port, _security, _broker_id=None,
                     tls_ca_path=""):
            self.host, self.port = host, port
            self.tls_ca_path = tls_ca_path

        async def connect(self, _step, label=""):
            attempts.append((self.host, self.port, label))
            if self.host == "unavailable":
                raise OSError("offline")

        async def call(self, *_args, **_kwargs):
            return {}, None, None

        def close(self):
            closed.append(self.host)

    monkeypatch.setattr(loadtest, "KafkaClient", FakeClient)
    monkeypatch.setattr(loadtest, "_metadata_dict", lambda _response: {
        "brokers": [],
        "topics": [{"topic": "demo", "error": 0, "partitions": [
            {"partition": 0, "leader": 1, "error": 0}]}],
    })
    clients, leader, bootstrap, partition = asyncio.run(loadtest._setup_leader(
        {"bootstrap": "unavailable:9092,available:19092", "caPath": "ca.pem"},
        SecurityConfig(), "demo"))

    assert attempts == [
        ("unavailable", 9092, "load-bootstrap"),
        ("available", 19092, "load-bootstrap"),
        ("available", 19092, "load-leader"),
    ]
    assert bootstrap.host == leader.host == "available"
    assert partition == 0
    assert all(client.tls_ca_path == "ca.pem" for client in clients)
    assert closed == ["unavailable"]


def test_tls_loadtest_uses_sasl_only_when_requested(monkeypatch):
    connector_args = []
    sasl_calls = []

    def fake_connect(connector, host, port):
        connector_args.append((connector.use_tls, connector.ca_path, host, port))
        return object()

    async def fake_sasl(self, _step):
        sasl_calls.append(self.security.securityProtocol)

    monkeypatch.setattr(client_module.TcpConnector, "connect", fake_connect)
    monkeypatch.setattr(client_module.KafkaClient, "_sasl_auth", fake_sasl)
    for protocol in ("SSL", "SASL_SSL"):
        client = client_module.KafkaClient(loadtest._NullTraces(), "load", "load",
                                           "test", 1000, "broker", 9093,
                                           SecurityConfig(securityProtocol=protocol,
                                                          saslUsername="user",
                                                          saslPassword="password"),
                                           tls_ca_path="ca.pem")
        asyncio.run(client.connect(0))

    assert connector_args == [(True, "ca.pem", "broker", 9093)] * 2
    assert sasl_calls == ["SASL_SSL"]
