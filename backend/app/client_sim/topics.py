"""Topic discovery and guarded creation for manually configured simulator targets."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from .registry import RUNNERS, _python_executable


class TopicConnection(BaseModel):
    bootstrap: str = Field(min_length=3, max_length=1024)
    securityProtocol: str = Field(default="PLAINTEXT", max_length=32)
    saslMechanism: str = Field(default="PLAIN", max_length=32)
    saslUsername: str = Field(default="", max_length=512)
    saslPassword: str = Field(default="", max_length=4096)
    caPath: str = Field(default="", max_length=1024)

    @model_validator(mode="after")
    def validate_security(self):
        if self.securityProtocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ValueError("不支持的安全协议")
        if self.securityProtocol.startswith("SASL_") and (not self.saslUsername or not self.saslPassword):
            raise ValueError("SASL 连接需要用户名和密码")
        return self


def _runtime() -> Path:
    for version in ("2.16.0", "2.13.2"):
        executable = _python_executable(version)
        if executable:
            return executable
    raise ValueError("Topic 查询需要已安装的 Python 客户端运行包")


async def topic_admin(action: str, config: dict) -> dict:
    script = RUNNERS / "topic_admin.py"
    proc = await asyncio.create_subprocess_exec(
        str(_runtime()), "-u", str(script),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    payload = json.dumps({"action": action, **config}, ensure_ascii=False).encode("utf-8") + b"\n"
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(payload), timeout=32)
    except asyncio.TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise ValueError("Topic 查询或创建超时，请确认连接地址和集群状态") from exc
    try:
        result = json.loads(stdout.decode("utf-8"))
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Topic 管理进程未返回有效结果") from exc
    if proc.returncode or result.get("error"):
        message = str(result.get("error") or "Topic 管理进程失败")
        secret = str(config.get("saslPassword") or "")
        raise ValueError(message.replace(secret, "[REDACTED]") if secret else message)
    return result
