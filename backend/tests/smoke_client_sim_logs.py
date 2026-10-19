"""Run the four installed clients against a closed loopback port to inspect real startup logs."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.client_sim.models import RunCreate
from app.client_sim.registry import command_for


SLOTS = [
    ("python", "2.13.2"), ("python", "2.16.0"),
    ("java", "3.9.1"), ("java", "4.0.1"),
    ("node", "1.16.0"), ("node", "2.2.4"),
    ("go", "1.20.5"), ("go", "1.21.0"),
]


def probe(client: str, version: str, kind: str) -> tuple[str, str, str, int, bool, bool, int, str]:
    command = command_for(client, version)
    if command is None:
        raise RuntimeError(f"{client} {version} is not installed")
    args, extra_env = command
    spec = RunCreate(kind=kind, client=client, version=version,
                     bootstrap="127.0.0.1:1", topic="smoke", count=1,
                     durationSeconds=2, logLevel="debug").model_dump()
    spec["runId"] = "smoke"
    completed = subprocess.run(args, input=json.dumps(spec) + "\n", text=True,
                               encoding="utf-8", errors="replace", capture_output=True,
                               timeout=22, env={**os.environ, **extra_env}, check=False)
    combined = completed.stdout + completed.stderr
    native_markers = {
        "python": "[librdkafka]", "java": "org.apache.kafka",
        "node": '"level":', "go": "[DEBUG]",
    }
    return (client, version, kind, completed.returncode, "[runner] PID" in completed.stderr,
            native_markers[client] in combined, len(combined.splitlines()),
            "\n".join(combined.splitlines()[:8]))


if __name__ == "__main__":
    cases = [(client, version, kind) for client, version in SLOTS
             for kind in ("producer", "consumer")]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda item: probe(*item), cases))
    for client, version, kind, code, started, native, lines, sample in results:
        print(f"{client} {version}/{kind}: exit={code}, startup={started}, native={native}, lines={lines}")
        if not native:
            print(sample)
    if not all(started and native for _, _, _, _, started, native, _, _ in results):
        raise SystemExit(1)
