"""Prepare one pinned local client slot; dependencies are fetched only on explicit CLI use.

Examples from the project root:
  python backend/client_runners/setup.py python 2.16.0
  python backend/client_runners/setup.py node 2.2.4
  python backend/client_runners/setup.py java 3.9.1
  python backend/client_runners/setup.py go 1.21.0
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "installed"
SLOTS = {"python": ("2.13.2", "2.16.0"), "java": ("3.9.1", "4.0.1"),
         "node": ("1.16.0", "2.2.4"), "go": ("1.20.5", "1.21.0")}


def execute(*args: str, cwd: Path | None = None) -> None:
    print("运行:", " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd, check=True)


def setup(client: str, version: str) -> Path:
    if version not in SLOTS[client]:
        raise ValueError("版本不在已验证档位中")
    target = OUTPUT / client / version
    target.mkdir(parents=True, exist_ok=True)
    if client == "python":
        if not (target / "pyvenv.cfg").exists():
            execute(sys.executable, "-m", "venv", str(target))
        exe = target / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        execute(str(exe), "-m", "pip", "install", f"confluent-kafka=={version}")
    elif client == "node":
        npm = shutil.which("npm.cmd" if os.name == "nt" else "npm")
        if not npm:
            raise RuntimeError("未找到 npm")
        execute(npm, "install", "--prefix", str(target), "--no-save", "--ignore-scripts", f"kafkajs@{version}")
    elif client == "java":
        mvn = shutil.which("mvn.cmd" if os.name == "nt" else "mvn")
        if not mvn:
            raise RuntimeError("未找到 Maven")
        execute(mvn, "-q", "-f", str(ROOT / "java" / "pom.xml"), f"-Dkafka.version={version}",
                "-DskipTests", "clean", "package")
        shutil.copy2(ROOT / "java" / "target" / "client-sim-1.0.0.jar", target / "client-sim.jar")
    else:
        bundled_go = ROOT / "tools" / "go" / "bin" / ("go.exe" if os.name == "nt" else "go")
        go = shutil.which("go") or (str(bundled_go) if bundled_go.is_file() else None)
        if not go:
            raise RuntimeError("未找到 Go 工具链；构建好的 Go 程序可直接复制到该版本目录")
        with tempfile.TemporaryDirectory(prefix="kcl-go-") as temp:
            working = Path(temp)
            shutil.copy2(ROOT / "go" / "main.go", working / "main.go")
            shutil.copy2(ROOT / "go" / "go.mod", working / "go.mod")
            execute(go, "mod", "edit", f"-require=github.com/twmb/franz-go@v{version}", cwd=working)
            execute(go, "mod", "tidy", cwd=working)
            binary = target / ("client-sim.exe" if os.name == "nt" else "client-sim")
            execute(go, "build", f"-ldflags=-X main.clientVersion={version}", "-o", str(binary), ".", cwd=working)
    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("client", choices=SLOTS)
    parser.add_argument("version")
    parsed = parser.parse_args()
    print("已准备:", setup(parsed.client, parsed.version))
