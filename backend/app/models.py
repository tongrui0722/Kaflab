from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
import time


class BrokerRef(BaseModel):
    id: Optional[int] = None
    host: str
    port: int


class KafkaTraceEvent(BaseModel):
    traceId: str
    timestamp: int = Field(default_factory=lambda: int(time.time() * 1000))
    apiKey: int = -1
    apiName: str = "LOCAL"
    apiVersion: int = 0
    correlationId: int = 0
    direction: Literal["request", "response", "local"] = "local"
    step: str = ""
    stepIndex: int = 0
    status: Literal["pending", "success", "error"] = "success"
    broker: Optional[BrokerRef] = None
    topic: Optional[str] = None
    partition: Optional[int] = None
    latencyMs: Optional[int] = None
    errorCode: Optional[int] = None
    errorName: Optional[str] = None
    request: Optional[Any] = None
    response: Optional[Any] = None
    rawRequest: Optional[str] = None
    rawResponse: Optional[str] = None


class WsMessage(BaseModel):
    type: str
    sessionId: str
    event: KafkaTraceEvent


SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
SaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]


class SecurityConfig(BaseModel):
    securityProtocol: SecurityProtocol = "PLAINTEXT"
    saslMechanism: SaslMechanism = "PLAIN"
    saslUsername: str = ""
    saslPassword: str = ""


class Session(BaseModel):
    id: str
    clientId: str = "kafka-sim-01"
    operation: str = "Producer"
    topic: str = "test-topic"
    status: Literal["running", "success", "error"] = "running"
    startTime: int = Field(default_factory=lambda: int(time.time() * 1000))
    endTime: Optional[int] = None
    durationMs: Optional[int] = None
    bootstrap: str = ""
    partition: Optional[int] = None
    offset: Optional[int] = None
    mode: Literal["live"] = "live"
    error: Optional[str] = None


class ProduceMessage(BaseModel):
    key: str = ""
    value: str = ""
    keyHex: str = ""
    valueHex: str = ""


class ProduceRequest(BaseModel):
    bootstrap: str = "192.168.127.129:19092"
    topic: str = "test-topic"
    partition: str = "auto"
    key: str = "test001"
    value: str = "hello kafka"
    keyHex: str = ""
    valueHex: str = ""
    messages: List[ProduceMessage] = Field(default_factory=list)
    messageCount: int = Field(default=0, ge=0, le=10000)
    acks: str = "all"
    retries: int = 3
    requestTimeoutMs: int = 10000
    deliveryTimeoutMs: int = 30000
    clientId: str = "kafka-sim-01"
    sessionId: Optional[str] = None
    securityProtocol: SecurityProtocol = "PLAINTEXT"
    saslMechanism: SaslMechanism = "PLAIN"
    saslUsername: str = ""
    saslPassword: str = ""


class ConsumerFetchRequest(BaseModel):
    bootstrap: str = "192.168.127.129:19092"
    topic: str = "test-topic"
    partition: int = 0
    offset: Optional[int] = None
    maxMessages: int = 20
    groupId: str = ""
    clientId: str = "kafka-sim-01"
    requestTimeoutMs: int = 10000
    sessionId: Optional[str] = None
    securityProtocol: SecurityProtocol = "PLAINTEXT"
    saslMechanism: SaslMechanism = "PLAIN"
    saslUsername: str = ""
    saslPassword: str = ""


class GroupDescribeRequest(BaseModel):
    bootstrap: str = "192.168.127.129:19092"
    groupId: str = "test-group"
    clientId: str = "kafka-sim-01"
    requestTimeoutMs: int = 10000
    securityProtocol: SecurityProtocol = "PLAINTEXT"
    saslMechanism: SaslMechanism = "PLAIN"
    saslUsername: str = ""
    saslPassword: str = ""


class FaultToggle(BaseModel):
    name: str
    enabled: bool = True


class TopicCreationRequest(BaseModel):
    """唯一的 Topic 创建请求契约；与 Kafbat TopicCreation 字段对齐。"""
    model_config = ConfigDict(extra="forbid")

    name: StrictStr = Field(min_length=1, max_length=249)
    partitions: StrictInt = Field(ge=1, le=1000)
    replicationFactor: StrictInt = Field(ge=1, le=32767)
    configs: Dict[StrictStr, StrictStr] = Field(default_factory=dict)


KAFKA_ERRORS: Dict[int, str] = {
    0: "NONE", 1: "OFFSET_OUT_OF_RANGE", 2: "CORRUPT_MESSAGE",
    5: "LEADER_NOT_AVAILABLE", 6: "NOT_LEADER_FOR_PARTITION",
    7: "REQUEST_TIMED_OUT", 8: "BROKER_NOT_AVAILABLE", 9: "REPLICA_NOT_AVAILABLE",
    13: "NETWORK_EXCEPTION", 14: "COORDINATOR_LOAD_IN_PROGRESS",
    15: "COORDINATOR_NOT_AVAILABLE", 16: "NOT_COORDINATOR",
    17: "INVALID_TOPIC_EXCEPTION", 19: "NOT_ENOUGH_REPLICAS",
    20: "NOT_ENOUGH_REPLICAS_AFTER_APPEND", 22: "ILLEGAL_GENERATION",
    25: "UNKNOWN_MEMBER_ID", 26: "INVALID_SESSION_TIMEOUT",
    27: "REBALANCE_IN_PROGRESS", 29: "TOPIC_AUTHORIZATION_FAILED",
    31: "CLUSTER_AUTHORIZATION_FAILED", 35: "UNSUPPORTED_VERSION",
    36: "TOPIC_ALREADY_EXISTS", 37: "INVALID_PARTITIONS",
    38: "INVALID_REPLICATION_FACTOR", 39: "INVALID_REPLICA_ASSIGNMENT",
    40: "INVALID_CONFIG", 41: "NOT_CONTROLLER",
    60: "SASL_AUTHENTICATION_FAILED", 3: "UNKNOWN_TOPIC_OR_PARTITION",
}


def error_name(code: Optional[int]) -> str:
    if code is None:
        return ""
    if code == -1:
        return "UNKNOWN_TOPIC_OR_PARTITION"
    return KAFKA_ERRORS.get(code, f"ERROR_{code}")
