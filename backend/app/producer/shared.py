from __future__ import annotations


class KafkaFailure(RuntimeError):
    def __init__(self, msg: str, name: str, step: int, code: int | None = None):
        super().__init__(msg)
        self.kafka_name = name
        self.kafka_code = code
        self.kafka_step = step


def _fail(msg: str, name: str, step: int, code: int | None = None) -> KafkaFailure:
    return KafkaFailure(msg, name, step, code)


def _metadata_dict(dec) -> dict:
    if isinstance(dec, dict):
        return dec
    return {"brokers": dec.brokers,
            "topics": [{"topic": t.topic, "error": t.error,
                        "partitions": [{"partition": p.partition, "leader": p.leader,
                                        "replicas": p.replicas, "isr": p.isr,
                                        "error": p.error} for p in t.partitions]}
                       for t in dec.topics]}


def _pick(supported: dict, key: int, want: int) -> int:
    if key not in supported:
        return want
    lo, hi = supported[key]
    return max(lo, min(hi, want))
