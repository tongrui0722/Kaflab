from __future__ import annotations
import json
import os
import time
import uuid
from typing import Dict, List, Optional

from pydantic import BaseModel

from .nodes import broker_nodes_from_metadata
from .secrets import encrypt_secret, decrypt_secret

_DATA_DIR = os.environ.get("KCL_DATA_DIR") or os.path.join(os.path.dirname(__file__), "..", "..")
_STORE = os.path.join(_DATA_DIR, "clusters.json")
_BROKER_STORE = os.path.join(_DATA_DIR, "broker_nodes.json")


class ClusterConfig(BaseModel):
    id: str = ""
    name: str = ""
    bootstrap: str = "192.168.127.129:19092"
    securityProtocol: str = "PLAINTEXT"
    saslMechanism: str = "PLAIN"
    saslUsername: str = ""
    saslPassword: str = ""
    monitoringEnabled: bool = True
    createdAt: Optional[int] = None


_DEFAULTS = [
    {"id": "local-plaintext", "name": "本地 PLAINTEXT",
     "bootstrap": "192.168.127.129:19092",
     "securityProtocol": "PLAINTEXT", "saslMechanism": "PLAIN",
     "saslUsername": "", "saslPassword": ""},
    {"id": "local-sasl", "name": "SASL_PLAINTEXT",
     "bootstrap": "192.168.127.129:19094",
     "securityProtocol": "SASL_PLAINTEXT", "saslMechanism": "PLAIN",
     "saslUsername": "admin", "saslPassword": ""},
    {"id": "local-scram", "name": "SCRAM",
     "bootstrap": "192.168.127.129:19095",
     "securityProtocol": "SASL_PLAINTEXT", "saslMechanism": "SCRAM-SHA-512",
     "saslUsername": "scramuser", "saslPassword": ""},
]


class ClusterManager:
    """多集群纳管：CRUD + 加密凭据落盘，重启保留。"""

    def __init__(self):
        self._items: Dict[str, ClusterConfig] = {}
        self._load()
        self._known_brokers: Dict[str, list[dict]] = {}
        try:
            with open(_BROKER_STORE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self._known_brokers = {cid: [node for node in nodes if isinstance(node, dict)]
                                       for cid, nodes in data.items()
                                       if cid in self._items and isinstance(nodes, list)}
        except (OSError, ValueError):
            pass

    def _save_broker_nodes(self) -> None:
        try:
            temporary = f"{_BROKER_STORE}.tmp"
            with open(temporary, "w", encoding="utf-8") as f:
                json.dump(self._known_brokers, f, ensure_ascii=False)
            os.replace(temporary, _BROKER_STORE)
        except OSError:
            pass

    def observe_brokers(self, cid: str, brokers: list[dict], topics: list[dict]) -> list[dict]:
        """Keep previously registered broker IDs visible after they disappear from metadata."""
        nodes = broker_nodes_from_metadata(brokers, topics, self._known_brokers.get(cid))
        known = [{"id": node["id"], "host": node["host"], "port": node["port"]}
                 for node in nodes]
        if known != self._known_brokers.get(cid):
            self._known_brokers[cid] = known
            self._save_broker_nodes()
        return nodes

    def _load(self) -> None:
        try:
            with open(_STORE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            data = None
        except (OSError, ValueError) as exc:
            raise RuntimeError("集群配置文件无法读取，请检查 clusters.json") from exc
        if data is not None:
            if not isinstance(data, list):
                raise RuntimeError("集群配置文件格式错误")
            migrated = False
            for item in data:
                if not isinstance(item, dict):
                    continue
                row = dict(item)
                encrypted = row.pop("saslPasswordEnc", None)
                if encrypted is not None:
                    row["saslPassword"] = decrypt_secret(encrypted)
                elif row.get("saslPassword"):
                    migrated = True
                try:
                    config = ClusterConfig(**row)
                except Exception:
                    continue
                if config.id:
                    self._items[config.id] = config
            if migrated:
                self._save()
            if self._items or os.environ.get("KCL_DATA_DIR"):
                return
        if os.environ.get("KCL_DATA_DIR"):
            self._save()
            return
        for row in _DEFAULTS:
            config = ClusterConfig(**row, createdAt=int(time.time() * 1000))
            self._items[config.id] = config
        self._save()

    def _save(self) -> None:
        os.makedirs(_DATA_DIR, exist_ok=True)
        rows = []
        for config in self._items.values():
            row = config.model_dump(exclude={"saslPassword"})
            if config.saslPassword:
                row["saslPasswordEnc"] = encrypt_secret(config.saslPassword)
            rows.append(row)
        temporary = f"{_STORE}.tmp"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(rows, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, _STORE)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @staticmethod
    def public(config: ClusterConfig) -> dict:
        data = config.model_dump()
        data["hasSaslPassword"] = bool(data.pop("saslPassword", ""))
        return data

    def list(self) -> List[ClusterConfig]:
        return list(self._items.values())

    def get(self, cid: str) -> Optional[ClusterConfig]:
        return self._items.get(cid)

    def save(self, data: dict) -> ClusterConfig:
        cid = (data.get("id") or "").strip() or f"c-{uuid.uuid4().hex[:8]}"
        c = ClusterConfig(
            id=cid,
            name=(data.get("name") or cid).strip(),
            bootstrap=(data.get("bootstrap") or "").strip(),
            securityProtocol=data.get("securityProtocol") or "PLAINTEXT",
            saslMechanism=data.get("saslMechanism") or "PLAIN",
            saslUsername=data.get("saslUsername") or "",
            saslPassword=("" if data.get("clearSaslPassword") else
                          data.get("saslPassword") or self._items.get(cid, ClusterConfig()).saslPassword),
            monitoringEnabled=data.get("monitoringEnabled", self._items[cid].monitoringEnabled if cid in self._items else True),
            createdAt=(self._items[cid].createdAt if cid in self._items else None)
            or int(time.time() * 1000),
        )
        if not c.bootstrap:
            raise ValueError("bootstrap 不可为空")
        previous = self._items.get(cid)
        if previous and (previous.bootstrap, previous.securityProtocol, previous.saslMechanism) != (c.bootstrap, c.securityProtocol, c.saslMechanism):
            self._known_brokers.pop(cid, None)
            self._save_broker_nodes()
        self._items[cid] = c
        self._save()
        return c

    def delete(self, cid: str) -> bool:
        if cid in self._items:
            del self._items[cid]
            self._known_brokers.pop(cid, None)
            self._save_broker_nodes()
            self._save()
            return True
        return False
