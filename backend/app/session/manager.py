from __future__ import annotations
import json
import os
import time
from typing import Dict, List

from ..models import Session

_STORE = os.path.join(os.path.dirname(__file__), "..", "..", "sessions.json")


class SessionManager:
    def __init__(self, limit: int = 100):
        self._items: Dict[str, Session] = {}
        self._order: List[str] = []
        self._limit = limit
        self._seq = 0
        self._load()

    def _load(self) -> None:
        try:
            if not os.path.exists(_STORE):
                return
            with open(_STORE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for d in data[: self._limit]:
                try:
                    s = Session(**d)
                except Exception:
                    continue
                self._items[s.id] = s
                self._order.append(s.id)
            # restore seq from max suffix
            for sid in self._order:
                try:
                    n = int(sid.rsplit("-", 1)[1])
                    self._seq = max(self._seq, n)
                except Exception:
                    pass
        except Exception:
            pass

    def _save(self) -> None:
        try:
            with open(_STORE, "w", encoding="utf-8") as f:
                json.dump([self._items[i].model_dump() for i in self._order
                           if i in self._items], f, ensure_ascii=False)
        except Exception:
            pass

    def create(self, *, client_id: str, operation: str, topic: str, bootstrap: str) -> Session:
        self._seq += 1
        sid = f"sess-{time.strftime('%Y%m%d-%H%M%S')}-{self._seq:03d}"
        s = Session(id=sid, clientId=client_id, operation=operation,
                    topic=topic, bootstrap=bootstrap)
        self._items[sid] = s
        self._order.insert(0, sid)
        for old in self._order[self._limit:]:
            self._items.pop(old, None)
        self._order = self._order[:self._limit]
        self._save()
        return s

    def get(self, sid: str) -> Session | None:
        return self._items.get(sid)

    def list(self) -> List[Session]:
        return [self._items[i] for i in self._order if i in self._items]

    def finish(self, sid: str, ok: bool, **kw) -> Session | None:
        s = self._items.get(sid)
        if not s:
            return None
        s.status = "success" if ok else "error"
        s.endTime = int(time.time() * 1000)
        s.durationMs = s.endTime - s.startTime
        for k, v in kw.items():
            if hasattr(s, k):
                setattr(s, k, v)
        self._save()
        return s
