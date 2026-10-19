"""Lag 自动采集开关与集群配置一同持久化。"""
from app.cluster import manager


def test_monitoring_switch_persists_and_old_configs_default_to_on(tmp_path, monkeypatch):
    monkeypatch.setenv("KCL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(manager, "_STORE", str(tmp_path / "clusters.json"))
    monkeypatch.setattr(manager, "_BROKER_STORE", str(tmp_path / "broker_nodes.json"))

    clusters = manager.ClusterManager()
    saved = clusters.save({"name": "test", "bootstrap": "broker:9092", "monitoringEnabled": False})
    assert saved.monitoringEnabled is False
    assert manager.ClusterManager().get(saved.id).monitoringEnabled is False

    clusters.save({"id": saved.id, "name": "updated", "bootstrap": "broker:9092"})
    assert manager.ClusterManager().get(saved.id).monitoringEnabled is False
    assert manager.ClusterConfig(id="old", bootstrap="broker:9092").monitoringEnabled is True
