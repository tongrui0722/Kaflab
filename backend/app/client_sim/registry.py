from __future__ import annotations

import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import sys


RUNNERS = Path(__file__).resolve().parents[2] / "client_runners"
INSTALLED = RUNNERS / "installed"

# Each slot is intentionally pinned. A run never downloads or silently changes a library.
SLOTS = {
    "python": ("2.13.2", "2.16.0"),
    "java": ("3.9.1", "4.0.1"),
    "node": ("1.16.0", "2.2.4"),
    "go": ("1.20.5", "1.21.0"),
}
LABELS = {"python": "Python · confluent-kafka", "java": "Java · kafka-clients",
          "node": "Node.js · KafkaJS", "go": "Go · franz-go"}


def _python_executable(version: str) -> Path | None:
    root = INSTALLED / "python" / version
    version_marker = f"confluent_kafka-{version}.dist-info"
    has_package = any(path.is_dir() for path in root.rglob(version_marker)) if root.exists() else False
    for candidate in (root / "Scripts" / "python.exe", root / "bin" / "python"):
        if candidate.is_file() and has_package:
            return candidate
    try:
        if importlib.metadata.version("confluent-kafka") == version:
            return Path(sys.executable)
    except importlib.metadata.PackageNotFoundError:
        pass
    return None


def command_for(client: str, version: str) -> tuple[list[str], dict[str, str]] | None:
    """Only return commands from pinned, locally verified artifacts; never use a shell."""
    if version not in SLOTS.get(client, ()):
        return None
    env: dict[str, str] = {}
    if client == "python":
        exe = _python_executable(version)
        script = RUNNERS / "python_client.py"
        env["PYTHONIOENCODING"] = "utf-8"
        return ([str(exe), "-u", str(script)], env) if exe and script.is_file() else None
    if client == "node":
        exe = shutil.which("node")
        root = INSTALLED / "node" / version
        package = root / "node_modules" / "kafkajs" / "package.json"
        script = RUNNERS / "node_client.cjs"
        try:
            installed_version = json.loads(package.read_text(encoding="utf-8"))["version"]
        except (OSError, ValueError, KeyError):
            installed_version = ""
        if exe and installed_version == version and script.is_file():
            env["KCL_KAFKAJS_ROOT"] = str(root)
            return [exe, str(script)], env
    if client == "java":
        exe = shutil.which("java")
        jar = INSTALLED / "java" / version / "client-sim.jar"
        if exe and jar.is_file():
            return [exe, "-Dfile.encoding=UTF-8", "-jar", str(jar)], env
    if client == "go":
        binary = INSTALLED / "go" / version / ("client-sim.exe" if os.name == "nt" else "client-sim")
        if binary.is_file():
            return [str(binary)], env
    return None


def catalog() -> list[dict]:
    return [
        {"id": client, "label": LABELS[client], "versions": [
            {"version": version, "available": command_for(client, version) is not None}
            for version in versions]}
        for client, versions in SLOTS.items()
    ]
