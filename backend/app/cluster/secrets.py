"""Encrypt saved Kafka credentials with a persistent Fernet key."""
from __future__ import annotations

import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken


def _key_path() -> Path:
    data_dir = os.environ.get("KCL_DATA_DIR") or str(Path(__file__).resolve().parents[2])
    return Path(data_dir) / ".kaflab.key"


def _cipher() -> Fernet:
    configured = os.environ.get("KCL_SECRET_KEY")
    if configured:
        try:
            return Fernet(configured.encode("ascii"))
        except (ValueError, TypeError) as exc:
            raise RuntimeError("KCL_SECRET_KEY 不是有效的 Fernet 密钥") from exc
    path = _key_path()
    try:
        key = path.read_bytes().strip()
    except FileNotFoundError:
        path.parent.mkdir(parents=True, exist_ok=True)
        generated = Fernet.generate_key()
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            key = path.read_bytes().strip()
        else:
            with os.fdopen(fd, "wb") as handle:
                handle.write(generated + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            key = generated
    try:
        return Fernet(key)
    except (ValueError, TypeError) as exc:
        raise RuntimeError("KAFLAB 凭据密钥无效，请恢复原有 .kaflab.key") from exc


def encrypt_secret(value: str) -> str:
    return _cipher().encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_secret(value: str) -> str:
    try:
        return _cipher().decrypt(value.encode("ascii")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("无法解密集群凭据，请恢复原有 KCL_SECRET_KEY 或 .kaflab.key") from exc

