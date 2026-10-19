from __future__ import annotations


def broker_nodes_from_metadata(brokers: list[dict], topics: list[dict],
                               known_brokers: list[dict] | None = None) -> list[dict]:
    """Compare current broker metadata with assigned and previously observed IDs.

    Kafka's broker list contains the currently registered nodes, while partition
    replica assignments can still name a node that has gone offline. A node
    without current replica assignments remains visible via observation history.
    """
    online: dict[int, dict] = {}
    for broker in brokers:
        broker_id = broker.get("id")
        if isinstance(broker_id, int) and broker_id >= 0:
            online[broker_id] = broker

    assigned = set(online)
    for topic in topics:
        for partition in topic.get("partitions", []) or []:
            for broker_id in partition.get("replicas", []) or []:
                if isinstance(broker_id, int) and broker_id >= 0:
                    assigned.add(broker_id)

    previous = {
        broker["id"]: broker for broker in known_brokers or []
        if isinstance(broker.get("id"), int) and broker["id"] >= 0
    }
    assigned.update(previous)

    return [
        {"id": broker_id, "status": "online" if broker_id in online else "offline",
         "host": (online[broker_id] if broker_id in online else previous.get(broker_id, {})).get("host"),
         "port": (online[broker_id] if broker_id in online else previous.get(broker_id, {})).get("port")}
        for broker_id in sorted(assigned)
    ]
