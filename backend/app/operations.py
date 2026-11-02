"""Durable, verified Kafka administrative operations."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

from fastapi import HTTPException
from starlette.requests import Request

DB_PATH = Path(os.environ.get("KCL_DATA_DIR") or str(Path(__file__).resolve().parents[1])) / "operations.db"
_RETRY_DELAYS = (1, 2, 4, 8, 15, 20, 30, 30)


class OperationManager:
    def __init__(self):
        self._running: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(2)
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS operations(
                id TEXT PRIMARY KEY, cluster TEXT NOT NULL, topic TEXT NOT NULL,
                actor TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL,
                next_at INTEGER, error TEXT, events TEXT NOT NULL,
                created INTEGER NOT NULL, updated INTEGER NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_operations_cluster ON operations(cluster, created DESC)")

    def _db(self):
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _row(row) -> dict:
        out = dict(row)
        out["events"] = json.loads(out["events"])
        return out

    def get(self, task_id: str) -> dict | None:
        with self._db() as db:
            row = db.execute("SELECT * FROM operations WHERE id=?", (task_id,)).fetchone()
        return self._row(row) if row else None

    def list(self, cluster: str | None = None, limit: int = 100) -> list[dict]:
        with self._db() as db:
            if cluster:
                rows = db.execute("SELECT * FROM operations WHERE cluster=? ORDER BY created DESC LIMIT ?",
                                  (cluster, limit)).fetchall()
            else:
                rows = db.execute("SELECT * FROM operations ORDER BY created DESC LIMIT ?", (limit,)).fetchall()
        return [self._row(row) for row in rows]

    def create_delete(self, app, cluster: str, topic: str, actor: str) -> dict:
        with self._db() as db:
            existing = db.execute(
                "SELECT * FROM operations WHERE cluster=? AND topic=? "
                "AND status IN ('queued','running','retrying','verifying') "
                "ORDER BY created DESC LIMIT 1", (cluster, topic)
            ).fetchone()
        if existing:
            return self._row(existing)
        now = int(time.time())
        task_id = "op-" + uuid.uuid4().hex[:12]
        events = [{"at": now, "type": "queued", "message": "Topic delete queued"}]
        with self._db() as db:
            db.execute("INSERT INTO operations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       (task_id, cluster, topic, actor, "queued", 0, None, None,
                        json.dumps(events), now, now))
        self._schedule(app, task_id)
        return self.get(task_id)

    def _update(self, task_id: str, status: str, message: str, *,
                attempts: int | None = None, next_at: int | None = None,
                error: str | None = None):
        current = self.get(task_id)
        if not current:
            return
        events = (current["events"] + [{"at": int(time.time()), "type": status,
                                         "message": message}])[-100:]
        with self._db() as db:
            db.execute("""UPDATE operations SET status=?, attempts=?, next_at=?, error=?,
                          events=?, updated=? WHERE id=?""",
                       (status, current["attempts"] if attempts is None else attempts,
                        next_at, error, json.dumps(events, ensure_ascii=False),
                        int(time.time()), task_id))

    def _schedule(self, app, task_id: str):
        task = asyncio.create_task(self._run(app, task_id), name=task_id)
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    def start(self, app):
        with self._db() as db:
            ids = [row[0] for row in db.execute(
                "SELECT id FROM operations WHERE status IN ('queued','running','retrying','verifying')")]
        for task_id in ids:
            self._schedule(app, task_id)

    async def stop(self):
        tasks = list(self._running)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _exists(self, request: Request, cluster: str, topic: str) -> bool:
        from .routers.api import _cluster_client
        from .protocol import codec
        _, client = _cluster_client(cluster, request, "topic-delete-verify")
        try:
            await client.connect(1, label="topic-delete-verify")
            decoded, _, _ = await client.call(
                3, "Metadata", 0, codec.encode_metadata_v0([]), 2, "metadata",
                extra_req={"topics": "ALL"}, decode=codec.decode_metadata_v0)
            return any(item.topic == topic and item.error == 0 for item in decoded.topics)
        finally:
            client.close()

    async def _run(self, app, task_id: str):
        from .routers.api import delete_topic_once
        async with self._semaphore:
            task = self.get(task_id)
            if not task:
                return
            request = Request({"type": "http", "app": app, "headers": []})
            for attempt in range(task["attempts"] + 1, len(_RETRY_DELAYS) + 2):
                current = self.get(task_id)
                if not current:
                    return
                if current["next_at"]:
                    await asyncio.sleep(max(0, current["next_at"] - time.time()))
                try:
                    if not await self._exists(request, current["cluster"], current["topic"]):
                        self._update(task_id, "done", "Confirmed absent from Kafka metadata")
                        return
                except Exception as exc:
                    self._update(task_id, "retrying", "Preflight metadata check failed: " + type(exc).__name__,
                                 attempts=attempt - 1, error=str(exc)[:250])
                    if attempt > len(_RETRY_DELAYS):
                        self._update(task_id, "failed", "Unable to confirm Topic state", error=str(exc)[:250])
                        return
                    await asyncio.sleep(_RETRY_DELAYS[attempt - 1])
                    continue
                self._update(task_id, "running", "Sending DeleteTopics to current Controller",
                             attempts=attempt)
                try:
                    await delete_topic_once(current["cluster"], current["topic"], request)
                    self._update(task_id, "verifying", "DeleteTopics accepted; checking metadata",
                                 attempts=attempt)
                except HTTPException as exc:
                    if exc.status_code in {400, 403, 409}:
                        self._update(task_id, "failed", str(exc.detail), attempts=attempt,
                                     error=str(exc.detail))
                        return
                    self._update(task_id, "retrying", str(exc.detail), attempts=attempt,
                                 error=str(exc.detail)[:250])
                except Exception as exc:
                    self._update(task_id, "retrying", "Request failed: " + type(exc).__name__,
                                 attempts=attempt, error=str(exc)[:250])
                for _ in range(3):
                    try:
                        if not await self._exists(request, current["cluster"], current["topic"]):
                            self._update(task_id, "done", "Confirmed absent from Kafka metadata",
                                         attempts=attempt)
                            return
                    except Exception:
                        pass
                    await asyncio.sleep(2)
                if attempt > len(_RETRY_DELAYS):
                    self._update(task_id, "failed", "Topic remains in metadata after retry limit",
                                 attempts=attempt, error="verification failed")
                    return
                delay = _RETRY_DELAYS[attempt - 1]
                self._update(task_id, "retrying", "Topic still present; retry scheduled",
                             attempts=attempt, next_at=int(time.time() + delay))
