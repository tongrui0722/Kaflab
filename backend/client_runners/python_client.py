"""JSON-lines adapter for the real confluent-kafka client.

One stdin line contains the run specification. stdout only emits structured events.
"""
from __future__ import annotations

import importlib.metadata
import json
import logging
import os
import signal
import sys
import time


stopping = False


def emit(event: str, message: str, **extra) -> None:
    print(json.dumps({"event": event, "message": message, **extra}, ensure_ascii=False), flush=True)


def request_stop(*_args) -> None:
    global stopping
    stopping = True


def client_logger() -> logging.Logger:
    logger = logging.getLogger("kcl.librdkafka")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [librdkafka] %(message)s"))
        logger.addHandler(handler)
    return logger


def options(spec: dict) -> dict:
    config = {"bootstrap.servers": spec["bootstrap"],
              "client.id": f"kcl-{spec['kind']}-{spec['runId']}",
              "socket.connection.setup.timeout.ms": spec["connectionTimeoutMs"],
              "socket.timeout.ms": spec["requestTimeoutMs"]}
    protocol = spec["securityProtocol"]
    config["security.protocol"] = protocol.lower()
    if protocol.startswith("SASL_"):
        config.update({"sasl.mechanism": spec["saslMechanism"],
                       "sasl.username": spec["saslUsername"],
                       "sasl.password": spec["saslPassword"]})
    if spec.get("caPath"):
        config["ssl.ca.location"] = spec["caPath"]
    config["debug"] = ("generic,broker,metadata,topic,protocol,security,cgrp,fetch,msg"
                       if spec.get("logLevel") == "debug" else "generic,broker,metadata")
    config.update({key: value for key, value in spec.get("clientConfig", {}).items()
                   if key in ("socket.connection.setup.timeout.ms", "socket.timeout.ms", "metadata.max.age.ms")})
    return config


def produce(spec: dict, kafka) -> None:
    config = options(spec)
    config.update({"acks": "all", "enable.idempotence": True,
                   "message.send.max.retries": spec["retries"],
                   "message.timeout.ms": spec["requestTimeoutMs"],
                   "message.max.bytes": spec["maxMessageBytes"] + 1024,
                   "allow.auto.create.topics": False,
                   "error_cb": lambda error: emit("error", str(error))})
    config.update({key: value for key, value in spec.get("clientConfig", {}).items()
                   if key in ("request.timeout.ms", "message.timeout.ms", "message.max.bytes",
                              "message.send.max.retries", "linger.ms", "batch.size", "compression.type")})
    native = spec.get("clientConfig", {})
    client = kafka.Producer(config, logger=client_logger())
    client.poll(0)
    metadata = client.list_topics(topic=spec["topic"], timeout=config["socket.timeout.ms"] / 1000)
    topic_metadata = metadata.topics.get(spec["topic"])
    if topic_metadata is None or topic_metadata.error:
        raise RuntimeError(f"Topic 不可用：{topic_metadata.error if topic_metadata else spec['topic']}")
    emit("ready", "已连接 Broker", broker=",".join(str(b.host) for b in metadata.brokers.values()))
    errors = 0

    def delivered(error, record):
        nonlocal errors
        if error:
            errors += 1
            emit("error", str(error))
        else:
            emit("sent", "消息已确认", topic=record.topic(), partition=record.partition(),
                 offset=record.offset(), latencyMs=round(record.latency() * 1000, 2))

    start = time.monotonic()
    try:
        for index in range(spec["count"]):
            if stopping or time.monotonic() - start >= spec["durationSeconds"]:
                break
            value = spec["value"].replace("{seq}", str(index + 1)).replace("{runId}", spec["runId"])
            payload_bytes = len(spec["key"].encode("utf-8")) + len(value.encode("utf-8"))
            limit = native.get("sim.max.message.bytes", spec["maxMessageBytes"])
            if payload_bytes > limit:
                raise ValueError(f"消息 Key 与内容共 {payload_bytes} 字节，超过设定上限 {limit} 字节")
            client.produce(spec["topic"], key=spec["key"] or None, value=value.encode("utf-8"),
                           partition=spec["partition"] if spec["partition"] is not None else -1,
                           callback=delivered)
            client.poll(0)
            target = start + (index + 1) / spec["rate"]
            while not stopping and time.monotonic() < target:
                client.poll(min(0.1, target - time.monotonic()))
    finally:
        remaining = client.flush(config["message.timeout.ms"] / 1000)
        if remaining:
            raise RuntimeError(f"仍有 {remaining} 条消息未确认")
    if errors:
        raise RuntimeError(f"{errors} 条消息投递失败")
    emit("done", "生产任务完成")


def consume(spec: dict, kafka) -> None:
    config = options(spec)
    config.update({"group.id": spec["groupId"] or f"kcl-{spec['runId']}",
                   "fetch.max.bytes": spec["maxFetchBytes"],
                   "fetch.message.max.bytes": spec["maxFetchBytes"],
                   "fetch.wait.max.ms": spec["fetchMaxWaitMs"],
                   "enable.auto.commit": spec["commit"],
                   "auto.offset.reset": spec["offsetStart"],
                   "error_cb": lambda error: emit("error", str(error))})
    config.update({key: value for key, value in spec.get("clientConfig", {}).items()
                   if key in ("fetch.max.bytes", "fetch.message.max.bytes", "fetch.wait.max.ms",
                              "fetch.min.bytes", "session.timeout.ms", "max.poll.interval.ms", "group.protocol")})
    client = kafka.Consumer(config, logger=client_logger())
    try:
        client.poll(0)
        metadata = client.list_topics(topic=spec["topic"], timeout=config["socket.timeout.ms"] / 1000)
        topic_metadata = metadata.topics.get(spec["topic"])
        if topic_metadata is None or topic_metadata.error:
            raise RuntimeError(f"Topic 不可用：{topic_metadata.error if topic_metadata else spec['topic']}")
        if spec["partition"] is None:
            client.subscribe([spec["topic"]],
                             on_assign=lambda _client, parts: emit("assigned", "分区已分配", count=len(parts)))
        else:
            offset = kafka.OFFSET_BEGINNING if spec["offsetStart"] == "earliest" else kafka.OFFSET_END
            client.assign([kafka.TopicPartition(spec["topic"], spec["partition"], offset)])
        emit("ready", "消费者已启动")
        deadline = time.monotonic() + spec["durationSeconds"]
        received = 0
        while not stopping and received < spec["count"] and time.monotonic() < deadline:
            record = client.poll(min(1.0, max(0.05, deadline - time.monotonic())))
            if record is None:
                continue
            if record.error():
                emit("error", str(record.error()))
                continue
            received += 1
            emit("received", "收到消息", topic=record.topic(), partition=record.partition(), offset=record.offset())
        emit("done", f"消费任务完成，共读取 {received} 条", count=received)
    finally:
        client.close()


def main() -> None:
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    spec = json.loads(sys.stdin.readline())
    installed = importlib.metadata.version("confluent-kafka")
    if installed != spec["version"]:
        raise RuntimeError(f"客户端版本不匹配：要求 {spec['version']}，实际 {installed}")
    import confluent_kafka as kafka
    print(f"[runner] PID {os.getpid()} · confluent-kafka {installed} · {spec['kind']} starting", file=sys.stderr, flush=True)
    emit("client", f"confluent-kafka {installed} 已加载")
    if spec["kind"] == "producer":
        produce(spec, kafka)
    else:
        consume(spec, kafka)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        emit("error", f"{type(exc).__name__}: {exc}")
        raise SystemExit(1)
