"""Create a source-only archive for building the Docker image on Linux."""
from __future__ import annotations

import tarfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "release" / "kaflab-docker-source.tar.gz"

FILES = [
    "Dockerfile", ".dockerignore", "docker/entrypoint.sh",
    "README.md", "docs/docker.md", "docs/architecture.md",
    "scripts/package-docker-source.py",
    "backend/requirements.txt",
    "backend/client_runners/setup.py", "backend/client_runners/python_client.py",
    "backend/client_runners/node_client.cjs", "backend/client_runners/topic_admin.py",
    "backend/client_runners/java/pom.xml", "backend/client_runners/go/go.mod",
    "backend/client_runners/go/main.go", "frontend/package.json",
    "frontend/package-lock.json", "frontend/index.html", "frontend/postcss.config.js",
    "frontend/tailwind.config.js", "frontend/tsconfig.json", "frontend/vite.config.ts",
]
DIRECTORIES = [
    "backend/app", "backend/client_runners/java/src", "frontend/src", "frontend/public",
]


def source_files() -> list[Path]:
    paths = [ROOT / name for name in FILES]
    for name in DIRECTORIES:
        paths.extend(path for path in (ROOT / name).rglob("*") if path.is_file())
    return sorted(path for path in paths if path.is_file()
                  and "__pycache__" not in path.parts
                  and path.suffix not in {".pyc", ".pyo"}
                  and path.name != "users.json")


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(OUTPUT, "w:gz") as archive:
        for path in source_files():
            archive.add(path, arcname=path.relative_to(ROOT).as_posix())
    print(OUTPUT)


if __name__ == "__main__":
    main()
