from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys
import uuid

from .models import RunCreate
from .registry import command_for
from .topics import topic_admin


ACTIVE = {"starting", "running", "stopping"}
MAX_LOG_BYTES = 16 * 1024 * 1024
ADAPTER_EVENTS = {"client", "ready", "sent", "received", "assigned", "done", "error"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ClientSimManager:
    def __init__(self, root: Path | None = None):
        default_root = Path(os.environ.get("KCL_DATA_DIR", str(Path(__file__).resolve().parents[2]))) / "client_sim_data"
        self.root = root or Path(os.environ.get("KCL_CLIENT_SIM_DATA", str(default_root)))
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs: dict[str, dict] = {}
        self.processes: dict[str, asyncio.subprocess.Process] = {}
        self._lock = asyncio.Lock()
        for path in sorted(self.root.glob("*.json")):
            try:
                run = json.loads(path.read_text(encoding="utf-8"))
                if run.get("status") in ACTIVE:
                    run["status"] = "interrupted"
                    run["endedAt"] = _now()
                    run["error"] = "KAFLAB 服务重启，运行状态已中断"
                    path.write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
                self.runs[run["id"]] = run
            except (OSError, ValueError, KeyError):
                continue

    def _meta_path(self, run_id: str) -> Path:
        return self.root / f"{run_id}.json"

    def _log_path(self, run_id: str) -> Path:
        return self.root / f"{run_id}.jsonl"

    def _save(self, run: dict) -> None:
        target = self._meta_path(run["id"])
        temp = target.with_suffix(".tmp")
        temp.write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
        temp.replace(target)

    def list(self) -> list[dict]:
        return [{key: value for key, value in run.items() if not key.startswith("_")}
                for run in sorted(self.runs.values(), key=lambda item: item["createdAt"], reverse=True)[:200]]

    def get(self, run_id: str) -> dict | None:
        return self.runs.get(run_id)

    def logs(self, run_id: str, after: int = 0) -> dict:
        if run_id not in self.runs:
            raise KeyError(run_id)
        path = self._log_path(run_id)
        entries: list[dict] = []
        if path.exists():
            with path.open(encoding="utf-8") as source:
                for line in source:
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if event.get("seq", 0) > after:
                        entries.append(event)
                        if len(entries) >= 500:
                            break
        return {"entries": entries, "next": entries[-1]["seq"] if entries else after}

    def raw_logs(self, run_id: str) -> str:
        if run_id not in self.runs:
            raise KeyError(run_id)
        path = self._log_path(run_id)
        if not path.exists():
            return ""
        lines: list[str] = []
        with path.open(encoding="utf-8") as source:
            for line in source:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                channel = entry.get("channel", "system")
                if entry.get("source") != "client" and not (
                    "source" not in entry and channel in ("stdout", "stderr") and not entry.get("event") and "raw" in entry
                ):
                    continue
                output = entry.get("raw", entry.get("message", ""))
                suffix = " [单行已截断]" if entry.get("rawTruncated") else ""
                lines.append(f"{output}{suffix}\n")
        return "".join(lines)

    def _append(self, run: dict, channel: str, message: str, payload: dict | None = None,
                raw: str | None = None) -> None:
        if payload:
            name = payload.get("event")
            if name == "sent":
                run["sent"] += 1
            elif name == "received":
                run["received"] += 1
            elif name == "error":
                run["errors"] += 1
        if run.get("logTruncated"):
            return
        secret = run.get("_secret", "")
        if secret:
            message = message.replace(secret, "[REDACTED]")
            if raw is not None:
                raw = raw.replace(secret, "[REDACTED]")
        event = {"seq": run["logSeq"] + 1, "time": _now(), "channel": channel,
                 "source": "adapter" if payload else "client" if raw is not None else "system",
                 "message": message[:3000]}
        if raw is not None:
            event["raw"] = raw
        if payload:
            event["event"] = payload.get("event", "")
            for name in ("topic", "partition", "offset", "latencyMs", "broker", "count"):
                if name in payload and isinstance(payload[name], (str, int, float)):
                    value = payload[name]
                    event[name] = value.replace(secret, "[REDACTED]") if secret and isinstance(value, str) else value
        line = json.dumps(event, ensure_ascii=False) + "\n"
        encoded_size = len(line.encode("utf-8"))
        if run["logBytes"] + encoded_size > MAX_LOG_BYTES:
            run["logTruncated"] = True
            self._save({key: value for key, value in run.items() if not key.startswith("_")})
            return
        with self._log_path(run["id"]).open("a", encoding="utf-8") as target:
            target.write(line)
        run["logSeq"] = event["seq"]
        run["logBytes"] += encoded_size

    async def start(self, spec: RunCreate) -> dict:
        async with self._lock:
            command = command_for(spec.client, spec.version)
            if command is None:
                raise ValueError("该客户端版本尚未安装；请先准备对应运行包")
            if sum(run["status"] in ACTIVE for run in self.runs.values()) >= 3:
                raise ValueError("本地最多同时运行 3 个模拟进程")
            topic_result = None
            if spec.kind == "producer" and spec.autoCreateTopic:
                topic_result = await topic_admin("ensure", spec.model_dump())
            run_id = uuid.uuid4().hex[:12]
            run = {"id": run_id, "kind": spec.kind, "client": spec.client, "version": spec.version,
                   "status": "starting", "createdAt": _now(), "startedAt": None, "endedAt": None,
                   "pid": None, "exitCode": None, "error": "", "config": spec.public_snapshot(),
                   "sent": 0, "received": 0, "errors": 0, "logSeq": 0, "logBytes": 0,
                   "logTruncated": False, "_secret": spec.saslPassword}
            self.runs[run_id] = run
            self._save({key: value for key, value in run.items() if not key.startswith("_")})
            args, extra_env = command
            flags = 0
            if sys.platform == "win32":
                import subprocess
                flags = subprocess.CREATE_NEW_PROCESS_GROUP
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE, env={**os.environ, **extra_env},
                    creationflags=flags, start_new_session=sys.platform != "win32")
                self.processes[run_id] = proc
                run["pid"] = proc.pid
                run["status"] = "running"
                run["startedAt"] = _now()
                self._append(run, "system", f"已启动 {spec.client} {spec.version} 客户端进程")
                if topic_result:
                    message = (f"已创建 Topic {spec.topic}（{spec.topicPartitions} 分区，{spec.topicReplicationFactor} 副本）"
                               if topic_result.get("created") else f"已确认 Topic {spec.topic} 已存在")
                    self._append(run, "system", message)
                assert proc.stdin is not None
                proc.stdin.write((json.dumps({"runId": run_id, **spec.model_dump()}, ensure_ascii=False) + "\n").encode("utf-8"))
                await proc.stdin.drain()
                proc.stdin.close()
                self._save({key: value for key, value in run.items() if not key.startswith("_")})
                asyncio.create_task(self._watch(run_id, proc))
            except Exception as exc:
                proc = self.processes.pop(run_id, None)
                if proc and proc.returncode is None:
                    proc.kill()
                    await proc.wait()
                run["status"] = "failed"
                run["error"] = str(exc)
                run["endedAt"] = _now()
                self._save({key: value for key, value in run.items() if not key.startswith("_")})
            return {key: value for key, value in run.items() if not key.startswith("_")}

    async def _read_stream(self, run: dict, stream: asyncio.StreamReader, channel: str) -> None:
        while line := await stream.readline():
            raw = line.decode("utf-8", errors="replace").rstrip("\r\n")
            if not raw.strip():
                continue
            message = raw
            payload = None
            if channel == "stdout":
                try:
                    candidate = json.loads(raw)
                    if (isinstance(candidate, dict) and isinstance(candidate.get("event"), str)
                            and candidate["event"] in ADAPTER_EVENTS
                            and isinstance(candidate.get("message"), str)):
                        payload = candidate
                        message = str(candidate.get("message") or candidate.get("event") or "事件")
                except ValueError:
                    pass
            self._append(run, channel, message, payload, raw=raw)
            if payload and payload.get("event") in ("sent", "received", "error"):
                self._save({key: value for key, value in run.items() if not key.startswith("_")})

    async def _watch(self, run_id: str, proc: asyncio.subprocess.Process) -> None:
        run = self.runs[run_id]
        assert proc.stdout and proc.stderr
        await asyncio.gather(self._read_stream(run, proc.stdout, "stdout"),
                             self._read_stream(run, proc.stderr, "stderr"))
        code = await proc.wait()
        run["exitCode"] = code
        run["endedAt"] = _now()
        run["status"] = "stopped" if run["status"] == "stopping" else "completed" if code == 0 else "failed"
        self._append(run, "system", f"进程退出，退出码 {code}")
        run.pop("_secret", None)
        self.processes.pop(run_id, None)
        self._save(run)

    async def stop(self, run_id: str) -> dict:
        run = self.runs.get(run_id)
        if run is None:
            raise KeyError(run_id)
        proc = self.processes.get(run_id)
        if proc and proc.returncode is None and run["status"] in ACTIVE:
            run["status"] = "stopping"
            self._save({key: value for key, value in run.items() if not key.startswith("_")})
            if sys.platform == "win32":
                proc.terminate()
            else:
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=4)
            except asyncio.TimeoutError:
                if sys.platform == "win32":
                    proc.kill()
                else:
                    os.killpg(proc.pid, signal.SIGKILL)
        return {key: value for key, value in run.items() if not key.startswith("_")}

    async def shutdown(self) -> None:
        for run_id in list(self.processes):
            await self.stop(run_id)
