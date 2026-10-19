from __future__ import annotations

from typing import Literal
import re
from pydantic import BaseModel, Field, field_validator, model_validator
from .config_schema import validate_config


class RunCreate(BaseModel):
    kind: Literal["producer", "consumer"]
    client: Literal["python", "java", "node", "go"]
    version: str
    bootstrap: str = Field(min_length=3, max_length=1024)
    topic: str = Field(min_length=1, max_length=249)
    securityProtocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    saslMechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = "PLAIN"
    saslUsername: str = Field(default="", max_length=512)
    saslPassword: str = Field(default="", max_length=4096)
    # PEM material is referenced by files already present on the KAFLAB host.
    caPath: str = Field(default="", max_length=1024)
    key: str = Field(default="", max_length=1024)
    value: str = Field(default="kaflab", max_length=1_048_576)
    count: int = Field(default=1, ge=1, le=1_000_000)
    rate: float = Field(default=10, gt=0, le=1000)
    durationSeconds: int = Field(default=60, ge=1, le=86_400)
    connectionTimeoutMs: int = Field(default=10_000, ge=1_000, le=60_000)
    requestTimeoutMs: int = Field(default=30_000, ge=1_000, le=120_000)
    maxMessageBytes: int = Field(default=1_048_576, ge=1_024, le=10_485_760)
    retries: int = Field(default=3, ge=1, le=10)
    maxFetchBytes: int = Field(default=10_485_760, ge=1_048_576, le=52_428_800)
    fetchMaxWaitMs: int = Field(default=500, ge=10, le=10_000)
    clientConfig: dict[str, int | str] = Field(default_factory=dict)
    logLevel: Literal["info", "debug"] = "info"
    groupId: str = Field(default="", max_length=249)
    offsetStart: Literal["earliest", "latest"] = "latest"
    commit: bool = False
    partition: int | None = Field(default=None, ge=0, le=2_147_483_647)
    autoCreateTopic: bool = False
    topicPartitions: int = Field(default=1, ge=1, le=1000)
    topicReplicationFactor: int = Field(default=1, ge=1, le=100)

    @field_validator("bootstrap", "topic", "groupId")
    @classmethod
    def strip_text(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def check_security(self):
        if self.securityProtocol.startswith("SASL_") and (not self.saslUsername or not self.saslPassword):
            raise ValueError("SASL 连接需要用户名和密码")
        if not self.bootstrap or not self.topic:
            raise ValueError("连接地址和 Topic 不能为空")
        if self.autoCreateTopic and self.kind != "producer":
            raise ValueError("仅生产端模拟可以自动创建 Topic")
        if self.autoCreateTopic and not re.fullmatch(r"[a-zA-Z0-9._-]{1,249}", self.topic):
            raise ValueError("Topic 名称只能包含字母、数字、点、下划线和连字符")
        if self.client == "node" and self.kind == "consumer" and self.partition is not None:
            raise ValueError("KafkaJS 消费模拟仅支持消费组订阅")
        if self.kind == "consumer" and self.partition is not None and self.commit:
            raise ValueError("指定分区直读时不支持提交 Offset")
        validate_config(self.client, self.version, self.kind, self.clientConfig)
        return self

    def public_snapshot(self) -> dict:
        data = self.model_dump(exclude={"saslPassword", "key", "value"})
        data["hasSaslPassword"] = bool(self.saslPassword)
        data["keyBytes"] = len(self.key.encode("utf-8"))
        data["valueBytes"] = len(self.value.encode("utf-8"))
        return data
