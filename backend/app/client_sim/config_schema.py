"""Editable, version-aware options exposed by the client simulator.

Only these keys may reach a child client. Connection credentials and run controls
stay in their dedicated fields rather than in the free-form editor.
"""
from __future__ import annotations

from .registry import SLOTS


def number(key: str, default: int, minimum: int, maximum: int, label: str) -> dict:
    return {"key": key, "value": default, "type": "integer", "min": minimum,
            "max": maximum, "label": label}


def choice(key: str, default: str, values: tuple[str, ...], label: str) -> dict:
    return {"key": key, "value": default, "type": "choice", "choices": list(values), "label": label}


def config_schema(client: str, version: str, kind: str) -> list[dict]:
    if version not in SLOTS.get(client, ()) or kind not in ("producer", "consumer"):
        raise ValueError("未验证的客户端、版本或模拟类型")
    kib = 1024
    mib = 1024 * kib
    message = number("sim.max.message.bytes", mib, kib, 10 * mib, "模拟器单条 Key + 内容上限（UTF-8 字节）")
    if client == "python":
        common = [number("socket.connection.setup.timeout.ms", 10000, 1000, 60000, "连接超时 · ms"),
                  number("socket.timeout.ms", 30000, 1000, 120000, "Socket 请求超时 · ms"),
                  number("metadata.max.age.ms", 300000, 10000, 86400000, "元数据刷新间隔 · ms")]
        specific = ([message, number("request.timeout.ms", 30000, 1000, 120000, "Broker 确认超时 · ms"),
                     number("message.timeout.ms", 60000, 1000, 300000, "消息投递总超时 · ms"),
                     number("message.max.bytes", mib + kib, 2048, 11 * mib, "客户端请求上限 · 字节"),
                     number("message.send.max.retries", 3, 1, 10, "失败重试次数"),
                     number("linger.ms", 0, 0, 10000, "发送聚合等待 · ms"),
                     number("batch.size", 16384, 1, 10 * mib, "单批次大小 · 字节"),
                     choice("compression.type", "none", ("none", "gzip"), "压缩方式")]
                    if kind == "producer" else
                    [number("fetch.max.bytes", 10 * mib, mib, 50 * mib, "单次拉取目标上限 · 字节"),
                     number("fetch.message.max.bytes", mib, kib, 50 * mib, "单分区拉取目标上限 · 字节"),
                     number("fetch.wait.max.ms", 500, 10, 10000, "拉取最长等待 · ms"),
                     number("fetch.min.bytes", 1, 1, mib, "拉取最少字节"),
                     number("session.timeout.ms", 45000, 6000, 300000, "消费组会话超时 · ms"),
                     number("max.poll.interval.ms", 300000, 10000, 1800000, "处理最长间隔 · ms"),
                     choice("group.protocol", "classic", ("classic", "consumer"), "消费组协议 · consumer 需 Kafka 4.0+")])
        return common + specific
    if client == "java":
        common = [number("socket.connection.setup.timeout.ms", 10000, 1000, 60000, "连接超时 · ms"),
                  number("socket.connection.setup.timeout.max.ms", 30000, 1000, 120000, "连接超时上限 · ms"),
                  number("request.timeout.ms", 30000, 1000, 120000, "请求超时 · ms"),
                  number("default.api.timeout.ms", 35000, 1000, 180000, "API 调用超时 · ms"),
                  number("metadata.max.age.ms", 300000, 10000, 86400000, "元数据刷新间隔 · ms")]
        specific = ([message, number("max.request.size", mib + kib, 2048, 11 * mib, "请求大小上限 · 字节"),
                     number("max.block.ms", 35000, 1000, 180000, "发送阻塞上限 · ms"),
                     number("delivery.timeout.ms", 60000, 1000, 300000, "投递总超时 · ms"),
                     number("retries", 3, 1, 10, "失败重试次数"),
                     choice("acks", "all", ("all", "1"), "确认级别"),
                     number("linger.ms", 5 if version.startswith("4.") else 0, 0, 10000, "聚合等待 · Kafka 4.0 默认 5ms"),
                     number("batch.size", 16384, 1, 10 * mib, "批次大小 · 字节"),
                     number("buffer.memory", 32 * mib, mib, 512 * mib, "生产缓冲区 · 字节"),
                     choice("compression.type", "none", ("none", "gzip"), "压缩方式")]
                    if kind == "producer" else
                    [number("fetch.max.bytes", 10 * mib, mib, 50 * mib, "单次拉取目标上限 · 字节"),
                     number("max.partition.fetch.bytes", mib, kib, 50 * mib, "单分区拉取目标上限 · 字节"),
                     number("fetch.max.wait.ms", 500, 10, 10000, "拉取最长等待 · ms"),
                     number("fetch.min.bytes", 1, 1, mib, "拉取最少字节"),
                     number("session.timeout.ms", 45000, 6000, 300000, "消费组会话超时 · ms"),
                     number("heartbeat.interval.ms", 3000, 1000, 100000, "心跳间隔 · ms"),
                     number("max.poll.records", 500, 1, 10000, "单次 poll 最多记录"),
                     choice("group.protocol", "classic", ("classic", "consumer"), "消费组协议 · consumer 需 Kafka 4.0+")])
        return common + specific
    if client == "node":
        common = [number("connectionTimeout", 10000, 1000, 60000, "连接超时 · ms"),
                  number("requestTimeout", 30000, 1000, 120000, "请求超时 · ms"),
                  number("retry.retries", 3, 1, 10, "失败重试次数"),
                  number("metadataMaxAge", 300000, 10000, 86400000, "元数据刷新间隔 · ms"),
                  number("maxInFlightRequests", 5, 1, 50, "并行请求上限")]
        specific = ([message, choice("acks", "all", ("all", "1"), "确认级别"),
                     choice("compression", "none", ("none", "gzip"), "压缩方式"),
                     choice("partitioner", "default" if version.startswith("2.") else "legacy",
                            ("default", "legacy") if version.startswith("2.") else ("legacy", "java-compatible"),
                            "分区器 · 2.x 默认算法与 1.x 不同")]
                    if kind == "producer" else
                    [number("maxBytes", 10 * mib, mib, 50 * mib, "单次拉取目标上限 · 字节"),
                     number("maxBytesPerPartition", mib, kib, 50 * mib, "单分区拉取目标上限 · 字节"),
                     number("maxWaitTimeInMs", 500, 10, 10000, "拉取最长等待 · ms"),
                     number("minBytes", 1, 1, mib, "拉取最少字节"),
                     number("sessionTimeout", 30000, 6000, 300000, "消费组会话超时 · ms"),
                     number("heartbeatInterval", 3000, 1000, 100000, "心跳间隔 · ms")])
        return common + specific
    common = [number("dialTimeoutMs", 10000, 1000, 60000, "连接超时 · ms"),
              number("requestTimeoutMs", 30000, 1000, 120000, "请求超时 · ms"),
              number("metadataMaxAgeMs", 300000, 10000, 86400000, "元数据刷新间隔 · ms")]
    specific = ([message, number("produceRequestTimeoutMs", 30000, 1000, 120000, "生产请求超时 · ms"),
                 number("deliveryTimeoutMs", 60000, 1000, 300000, "投递总超时 · ms"),
                 number("recordRetries", 4, 2, 11, "最多尝试次数（含首次）"),
                 number("batchMaxBytes", mib + kib, 2048, 11 * mib, "批次大小上限 · 字节"),
                 number("lingerMs", 0, 0, 10000, "聚合等待 · ms")]
                if kind == "producer" else
                [number("fetchMaxBytes", 10 * mib, mib, 50 * mib, "单次拉取目标上限 · 字节"),
                 number("fetchMaxPartitionBytes", mib, kib, 50 * mib, "单分区拉取目标上限 · 字节"),
                 number("fetchMaxWaitMs", 500, 10, 10000, "拉取最长等待 · ms"),
                 number("fetchMinBytes", 1, 1, mib, "拉取最少字节")])
    return common + specific


def validate_config(client: str, version: str, kind: str, config: dict) -> None:
    allowed = {option["key"]: option for option in config_schema(client, version, kind)}
    for key, value in config.items():
        option = allowed.get(key)
        if option is None:
            raise ValueError(f"当前客户端版本不支持参数 {key}")
        if option["type"] == "integer":
            if type(value) is not int or not option["min"] <= value <= option["max"]:
                raise ValueError(f"参数 {key} 需要 {option['min']}–{option['max']} 的整数")
        elif not isinstance(value, str) or value not in option["choices"]:
            raise ValueError(f"参数 {key} 只支持 {', '.join(option['choices'])}")
    if client == "java":
        values = {option["key"]: config.get(option["key"], option["value"]) for option in allowed.values()}
        if values["socket.connection.setup.timeout.max.ms"] < values["socket.connection.setup.timeout.ms"]:
            raise ValueError("连接超时上限不能小于连接超时")
        if kind == "producer":
            if values["delivery.timeout.ms"] < values["request.timeout.ms"] + values["linger.ms"]:
                raise ValueError("delivery.timeout.ms 不能小于 request.timeout.ms + linger.ms")
            if values["batch.size"] > values["buffer.memory"]:
                raise ValueError("batch.size 不能大于 buffer.memory")
    if kind == "consumer" and client in ("java", "node"):
        keys = ("heartbeat.interval.ms", "session.timeout.ms") if client == "java" else ("heartbeatInterval", "sessionTimeout")
        values = {option["key"]: config.get(option["key"], option["value"]) for option in allowed.values()}
        if values[keys[0]] >= values[keys[1]]:
            raise ValueError("心跳间隔必须小于消费组会话超时")
