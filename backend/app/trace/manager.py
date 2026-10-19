from __future__ import annotations
import asyncio
import json
import os
import time
from collections import defaultdict
from typing import Dict, List

from ..models import KafkaTraceEvent, WsMessage

_DATA_DIR = os.environ.get("KCL_DATA_DIR") or os.path.join(os.path.dirname(__file__), "..", "..")
_TRACE_DIR = os.path.join(_DATA_DIR, "traces")


class TraceManager:
    def __init__(self):
        self._events: Dict[str, List[KafkaTraceEvent]] = defaultdict(list)
        self._by_session: Dict[str, List[WsMessage]] = defaultdict(list)
        self._subs: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        try:
            os.makedirs(_TRACE_DIR, exist_ok=True)
        except OSError:
            pass

    def _path(self, trace_id: str) -> str:
        safe = "".join(c if (c.isalnum() or c in "-_") else "_" for c in trace_id)
        return os.path.join(_TRACE_DIR, f"{safe}.json")

    def append(self, session_id: str, event: KafkaTraceEvent) -> None:
        self._events[event.traceId].append(event)
        msg = WsMessage(type=_ws_type(event), sessionId=session_id, event=event)
        self._by_session[session_id].append(msg)
        # cap per-session replay buffer
        if len(self._by_session[session_id]) > 500:
            self._by_session[session_id] = self._by_session[session_id][-500:]
        for q in list(self._subs.get(session_id, [])):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass
        # terminal event → persist whole trace for Protocol Inspector replay
        if event.step in ("session.done", "session.failed"):
            self._persist(event.traceId)

    def _persist(self, trace_id: str) -> None:
        try:
            evs = self._events.get(trace_id, [])
            if not evs:
                return
            with open(self._path(trace_id), "w", encoding="utf-8") as f:
                json.dump([e.model_dump() for e in evs], f, ensure_ascii=False)
            self._prune()
        except OSError:
            pass

    def _prune(self, keep: int = 200) -> None:
        try:
            files = sorted(
                (os.path.join(_TRACE_DIR, n) for n in os.listdir(_TRACE_DIR)
                 if n.endswith(".json")),
                key=lambda p: os.path.getmtime(p))
            for p in files[:-keep]:
                try:
                    os.remove(p)
                except OSError:
                    pass
        except OSError:
            pass

    def history(self, trace_id: str) -> List[KafkaTraceEvent]:
        evs = self._events.get(trace_id)
        if evs:
            return list(evs)
        # fall back to disk (survives backend restart)
        try:
            with open(self._path(trace_id), "r", encoding="utf-8") as f:
                return [KafkaTraceEvent(**d) for d in json.load(f)]
        except (OSError, ValueError):
            return []

    def list_recent(self, limit: int = 50) -> List[dict]:
        """落盘 trace 摘要，供 Protocol Inspector 直接选择回放（无需手抄 traceId）。"""
        out: List[dict] = []
        seen: set = set()
        items: List[tuple] = []
        try:
            for n in os.listdir(_TRACE_DIR):
                if n.endswith(".json"):
                    p = os.path.join(_TRACE_DIR, n)
                    items.append((os.path.getmtime(p), n[:-5]))
        except OSError:
            items = []
        for _, tid in sorted(items, reverse=True):
            evs = self._events.get(tid)
            if not evs:
                evs = self.history(tid)
            if not evs:
                continue
            seen.add(tid)
            last = evs[-1]
            out.append({
                "traceId": tid,
                "events": len(evs),
                "startTime": evs[0].timestamp,
                "lastStep": last.step,
                "status": self._status_of(last),
                "topic": next((e.topic for e in evs if e.topic), None),
                "api": next((e.apiName for e in evs if e.apiKey >= 0), None),
            })
        # 内存中有但尚未落盘的（进行中）置顶
        for tid, evs in list(self._events.items()):
            if tid in seen or not evs:
                continue
            last = evs[-1]
            out.insert(0, {
                "traceId": tid,
                "events": len(evs),
                "startTime": evs[0].timestamp,
                "lastStep": last.step,
                "status": self._status_of(last, running_ok=True),
                "topic": next((e.topic for e in evs if e.topic), None),
                "api": next((e.apiName for e in evs if e.apiKey >= 0), None),
            })
        return out[:limit]

    @staticmethod
    def _status_of(last: KafkaTraceEvent, running_ok: bool = False) -> str:
        if last.step == "session.failed" or last.status == "error":
            return "error"
        if last.step == "session.done":
            return "success"
        return "running" if running_ok else last.status

    def subscribe(self, session_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        # 回放 WS 连接前已产生的事件：POST 后 engine 后台任务可能先跑，
        # 否则前面的 TCP/ApiVersions/Metadata 步骤在前端永远缺失（表现为大量 pending 闪烁）
        for m in self._by_session.get(session_id, []):
            try:
                q.put_nowait(m)
            except asyncio.QueueFull:
                break
        self._subs[session_id].append(q)
        return q

    def unsubscribe(self, session_id: str, q: asyncio.Queue) -> None:
        try:
            self._subs[session_id].remove(q)
        except ValueError:
            pass


def _ws_type(e: KafkaTraceEvent) -> str:
    s = e.step
    if e.direction == "request":
        if e.apiName == "ApiVersions":
            return "trace.request.sent"
        if e.apiName == "Metadata":
            return "trace.request.sent"
        if e.apiName == "Produce":
            return "trace.produce.sent"
        return "trace.request.sent"
    if e.direction == "response":
        if e.apiName == "Produce":
            return "trace.produce.success" if e.status == "success" else "trace.failed"
        return "trace.response.received"
    mapping = {
        "session.start": "trace.started",
        "tcp.connect": "trace.tcp.connected",
        "metadata.updated": "trace.metadata.updated",
        "partition.selected": "trace.partition.selected",
        "session.done": "trace.completed",
        "session.failed": "trace.failed",
    }
    return mapping.get(s, "trace.response.received")


def now_ms() -> int:
    return int(time.time() * 1000)
