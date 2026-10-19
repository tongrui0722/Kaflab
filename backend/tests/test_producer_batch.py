"""大批量生产应完整发送并准确报告 Offset，而不是静默截到 100 条。"""
import asyncio

from app.models import ProduceRequest
from app.producer import engine


class FakeKafkaClient:
    offset = 500
    produce_calls = []
    fail_at = None

    def __init__(self, *_args, **_kwargs):
        self.host = "broker"
        self.port = 9092

    async def connect(self, *_args, **_kwargs):
        pass

    async def call(self, api_key, _name, _version, _body, *_args, **kwargs):
        if api_key == 18:
            return {"apis": []}, b"", b""
        if api_key == 3:
            return {"brokers": [{"id": 1, "host": "broker", "port": 9092}],
                    "topics": [{"topic": "test", "error": 0,
                                "partitions": [{"partition": 0, "error": 0, "leader": 1,
                                                "replicas": [1], "isr": [1]}]}]}, b"", b""
        assert api_key == 0
        count = kwargs["extra_req"]["records"]
        self.produce_calls.append((count, kwargs["capture_raw"]))
        error = 7 if len(self.produce_calls) == self.fail_at else 0
        base = self.offset
        if not error:
            type(self).offset += count
        return {"topics": [{"partitions": [{"errorCode": error, "baseOffset": base}]}]}, b"", b""

    def close(self):
        pass


class FakeTraces:
    def __init__(self):
        self.events = []

    def append(self, _session_id, event):
        self.events.append(event)


class FakeSessions:
    def __init__(self):
        self.finished = None

    def finish(self, _session_id, ok, **kwargs):
        self.finished = (ok, kwargs)


def test_ten_thousand_records_are_sent_in_100_chunks(monkeypatch):
    monkeypatch.setattr(engine, "KafkaClient", FakeKafkaClient)
    FakeKafkaClient.offset = 500
    FakeKafkaClient.produce_calls = []
    FakeKafkaClient.fail_at = None
    traces, sessions = FakeTraces(), FakeSessions()

    asyncio.run(engine.ProducerEngine(sessions, traces).run(
        ProduceRequest(bootstrap="broker:9092", topic="test", key="", value="", messageCount=10000), "session"))

    assert sessions.finished[0] is True
    assert len(FakeKafkaClient.produce_calls) == 100
    assert sum(count for count, _ in FakeKafkaClient.produce_calls) == 10000
    assert FakeKafkaClient.produce_calls[0] == (100, True)
    assert FakeKafkaClient.produce_calls[1] == (100, False)
    assert FakeKafkaClient.produce_calls[-1] == (100, True)
    done = next(event for event in traces.events if event.step == "session.done")
    assert done.response["offset"] == 500
    assert done.response["lastOffset"] == 10499
    assert done.response["records"] == 10000
    assert done.response["batches"] == 100
    assert done.response["offsets"] is None
    assert done.response["totalBytes"] == 400000
    assert done.response["sampleKey"].startswith("k-")
    assert len(done.response["sampleValue"]) == 32


def test_partial_failure_reports_confirmed_count(monkeypatch):
    monkeypatch.setattr(engine, "KafkaClient", FakeKafkaClient)
    FakeKafkaClient.offset = 500
    FakeKafkaClient.produce_calls = []
    FakeKafkaClient.fail_at = 2
    traces, sessions = FakeTraces(), FakeSessions()

    asyncio.run(engine.ProducerEngine(sessions, traces).run(
        ProduceRequest(bootstrap="broker:9092", topic="test", key="", value="", messageCount=250, retries=1), "session"))

    assert sessions.finished[0] is False
    failed = next(event for event in traces.events if event.step == "session.failed")
    assert failed.response["sent"] == 100
    assert failed.response["requested"] == 250
    assert failed.response["firstOffset"] == 500
    assert failed.response["lastOffset"] == 599
    assert failed.response["confirmedBytes"] == 4000
    assert failed.response["completedBatches"] == 1
    assert failed.response["sampleKey"].startswith("k-")


def test_small_batch_keeps_one_produce_request(monkeypatch):
    monkeypatch.setattr(engine, "KafkaClient", FakeKafkaClient)
    FakeKafkaClient.offset = 700
    FakeKafkaClient.produce_calls = []
    FakeKafkaClient.fail_at = None
    traces, sessions = FakeTraces(), FakeSessions()
    request = ProduceRequest(bootstrap="broker:9092", topic="test",
                             messages=[{"key": f"k-{i}", "value": "v"} for i in range(100)])

    asyncio.run(engine.ProducerEngine(sessions, traces).run(request, "session"))

    assert FakeKafkaClient.produce_calls == [(100, True)]
    done = next(event for event in traces.events if event.step == "session.done")
    assert done.response["offset"] == 700
    assert done.response["lastOffset"] == 799
    assert len(done.response["offsets"]) == 100
    assert done.response["totalBytes"] == sum(len(f"k-{i}".encode()) + 1 for i in range(100))
    assert done.response["sampleKey"] == "k-0"
    assert done.response["sampleValue"] == "v"


def test_record_sample_is_bounded_and_hex_is_identified(monkeypatch):
    monkeypatch.setattr(engine, "KafkaClient", FakeKafkaClient)
    FakeKafkaClient.offset = 900
    FakeKafkaClient.produce_calls = []
    FakeKafkaClient.fail_at = None
    traces, sessions = FakeTraces(), FakeSessions()
    request = ProduceRequest(bootstrap="broker:9092", topic="test", keyHex="00ff",
                             value="x" * 300)

    asyncio.run(engine.ProducerEngine(sessions, traces).run(request, "session"))

    done = next(event for event in traces.events if event.step == "session.done")
    assert done.response["totalBytes"] == 302
    assert done.response["sampleKey"] == "00ff"
    assert done.response["sampleKeyFormat"] == "hex"
    assert len(done.response["sampleValue"]) == 256
    assert done.response["sampleTruncated"] is True
