"""Short-lived Kafka admin process for the standalone client simulator.

The request is read from stdin so credentials never appear in process arguments.
Only a single JSON result is written to stdout.
"""
from __future__ import annotations

import json
import sys


def main() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    spec = json.loads(sys.stdin.readline())
    config = {
        "bootstrap.servers": spec["bootstrap"],
        "security.protocol": spec["securityProtocol"].lower(),
        "socket.timeout.ms": 8000,
        "request.timeout.ms": 8000,
        "client.id": "kcl-topic-admin",
    }
    if spec["securityProtocol"].startswith("SASL_"):
        config.update({
            "sasl.mechanism": spec["saslMechanism"],
            "sasl.username": spec["saslUsername"],
            "sasl.password": spec["saslPassword"],
        })
    if spec.get("caPath"):
        config["ssl.ca.location"] = spec["caPath"]
    admin = AdminClient(config)
    metadata = admin.list_topics(timeout=9)
    topics = sorted(name for name, item in metadata.topics.items() if not item.error)
    if spec["action"] == "list":
        print(json.dumps({"topics": topics, "brokers": len(metadata.brokers)}))
        return

    name = spec["topic"]
    if name in topics:
        print(json.dumps({"topic": name, "created": False}))
        return
    partitions = spec["topicPartitions"]
    replicas = spec["topicReplicationFactor"]
    if replicas > len(metadata.brokers):
        raise ValueError(f"副本数 {replicas} 超过当前集群 Broker 数 {len(metadata.brokers)}")
    future = admin.create_topics(
        [NewTopic(name, num_partitions=partitions, replication_factor=replicas)],
        operation_timeout=12, request_timeout=15,
    )[name]
    try:
        future.result(timeout=18)
        print(json.dumps({"topic": name, "created": True}))
    except Exception as exc:
        # A concurrent creator may win between metadata lookup and CreateTopics.
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            print(json.dumps({"topic": name, "created": False}))
        else:
            raise


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"error": str(exc)[:500]}))
        sys.exit(1)
