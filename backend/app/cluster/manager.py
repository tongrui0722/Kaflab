from __future__ import annotations
import json
import os
import time
import uuid
from typing import Dict, List, Optional

from pydantic import BaseModel

_STORE = os.path.join(os.path.dirname(__file__), "..", "..", "clusters.json")


class ClusterConfig(BaseModel):
    id: str = ""
    name: str = ""
    bootstrap: str = "192.168.127.129:19092"
    securityProtocol: str = "PLAINTEXT"
    saslMechanism: str = "PLAIN"
    saslUsername: str = ""
    saslPassword: str = ""
    createdAt: Optional[int] = None


_DEFAULTS = [
    {"id": "local-plaintext", "name": "本地 PLAINTEXT",
     "bootstrap": "192.168.127.129:19092",
     "securityProtocol": "PLAINTEXT", "saslMechanism": "PLAIN",
     "saslUsername": "", "saslPassword": ""},
    {"id": "local-sasl", "name": "SASL_PLAINTEXT",
     "bootstrap": "192.168.127.129:19094",
     "securityProtocol": "SASL_PLAINTEXT", "saslMechanism": "PLAIN",
     "saslUsername": "admin", "saslPassword": "admin123"},
    {"id": "local-scram", "name": "SCRAM",
     "bootstrap": "192.168.127.129:19095",
     "securityProtocol": "SASL_PLAINTEXT", "saslMechanism": "SCRAM-SHA-512",
     "saslUsername": "scramuser", "saslPassword": ""},
]


class ClusterManager:
    """多集群纳管：CRUD + 落盘，重启保留。口令明文存本地文件（实验工具定位）."""

    def __init__(self):
        self._items: Dict[str, ClusterConfig] = {}
        self._load()

    def _load(self) -> None:
        try:
            if not os.path.exists(_STORE):
                raise FileNotFoundError
            with open(_STORE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for d in data:
                try:
                    c = ClusterConfig(**d)
                except Exception:
                    continue
                if c.id:
                    self._items[c.id] = c
            if self._items:
                return
        except Exception:
            pass
        for d in _DEFAULTS:
            c = ClusterConfig(**d, createdAt=int(time.time() * 1000))
            self._items[c.id] = c
        self._save()

    def _save(self) -> None:
        try:
            with open(_STORE, "w", encoding="utf-8") as f:
                json.dump([c.model_dump() for c in self._items.values()],
                          f, ensure_ascii=False)
        except OSError:
            pass

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
            saslPassword=data.get("saslPassword") or "",
            createdAt=(self._items[cid].createdAt if cid in self._items else None)
            or int(time.time() * 1000),
        )
        if not c.bootstrap:
            raise ValueError("bootstrap 不可为空")
        self._items[cid] = c
        self._save()
        return c

    def delete(self, cid: str) -> bool:
        if cid in self._items:
            del self._items[cid]
            self._save()
            return True
        return False
