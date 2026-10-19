from app.cluster.nodes import broker_nodes_from_metadata
from app.cluster import manager as cluster_manager


def test_broker_missing_from_live_metadata_is_offline_when_still_assigned():
    brokers = [{"id": 1, "host": "one", "port": 9092},
               {"id": 2, "host": "two", "port": 9092}]
    topics = [{"partitions": [{"replicas": [1, 2, 3], "isr": [1, 2]}]}]

    assert broker_nodes_from_metadata(brokers, topics) == [
        {"id": 1, "status": "online", "host": "one", "port": 9092},
        {"id": 2, "status": "online", "host": "two", "port": 9092},
        {"id": 3, "status": "offline", "host": None, "port": None},
    ]


def test_unassigned_nodes_are_not_invented():
    assert broker_nodes_from_metadata([{"id": 1, "host": "one", "port": 9092}], []) == [
        {"id": 1, "status": "online", "host": "one", "port": 9092},
    ]


def test_observed_node_stays_offline_without_replica_assignment_after_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(cluster_manager, "_STORE", str(tmp_path / "clusters.json"))
    monkeypatch.setattr(cluster_manager, "_BROKER_STORE", str(tmp_path / "broker_nodes.json"))
    manager = cluster_manager.ClusterManager()
    cid = manager.list()[0].id
    three = [{"id": number, "host": f"broker-{number}", "port": 9092} for number in (1, 2, 3)]
    assert len(manager.observe_brokers(cid, three, [])) == 3

    restarted = cluster_manager.ClusterManager()
    nodes = restarted.observe_brokers(cid, three[:2], [])
    assert nodes == [
        {"id": 1, "status": "online", "host": "broker-1", "port": 9092},
        {"id": 2, "status": "online", "host": "broker-2", "port": 9092},
        {"id": 3, "status": "offline", "host": "broker-3", "port": 9092},
    ]
    assert restarted.observe_brokers(cid, three, [])[-1]["status"] == "online"
