"""Scheduled broker and topic health snapshots independent of browser sessions."""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

from . import monitoring
from .cluster.manager import ClusterManager
from .lag_collector import DiscardTraces
from .models import SecurityConfig
from .network.client import KafkaClient
from .network.tcp import parse_hostport
from .protocol import codec

logger = logging.getLogger(__name__)


class HealthCollector:
    def __init__(self, clusters: ClusterManager, interval: int = 60, timeout: int = 30):
        self.clusters = clusters
        self.interval = interval
        self.timeout = timeout
        self._task: asyncio.Task | None = None

    def start(self):
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._run(), name="health-collector")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        while True:
            began = time.monotonic()
            await asyncio.gather(*(self.collect(c) for c in self.clusters.list()
                                   if c.monitoringEnabled), return_exceptions=True)
            await asyncio.sleep(max(0, self.interval - (time.monotonic() - began)))

    async def collect(self, cluster):
        client = None
        try:
            async def probe():
                nonlocal client
                host, port = parse_hostport(cluster.bootstrap)
                sec = SecurityConfig(securityProtocol=cluster.securityProtocol,
                                     saslMechanism=cluster.saslMechanism,
                                     saslUsername=cluster.saslUsername,
                                     saslPassword=cluster.saslPassword)
                client = KafkaClient(DiscardTraces(), "sess-health", "health-" + uuid.uuid4().hex[:8],
                                     "kaflab-health", self.timeout * 1000, host, port, sec)
                await client.connect(0, label="health")
                all_topics, _, _ = await client.call(
                    3, "Metadata", 0, codec.encode_metadata_v0([]), 1, "metadata",
                    extra_req={"topics": "ALL"}, decode=codec.decode_metadata_v0)
                controller_id = None
                try:
                    controller, _, _ = await client.call(
                        3, "Metadata", 1, codec.encode_metadata_v0([]), 2, "metadata",
                        extra_req={"controllerLookup": True}, decode=codec.decode_metadata_v1)
                    controller_id = controller.controller_id
                except Exception:
                    pass
                return all_topics, controller_id
            meta, controller_id = await asyncio.wait_for(probe(), timeout=self.timeout)
            known = self.clusters.observe_brokers(
                cluster.id, meta.brokers,
                [{"topic": item.topic, "partitions": [
                    {"leader": part.leader, "replicas": part.replicas, "isr": part.isr}
                    for part in item.partitions]} for item in meta.topics])
            valid = [topic for topic in meta.topics if topic.error == 0]
            offline = sum(1 for topic in valid if any(p.leader < 0 for p in topic.partitions))
            under_rep = sum(1 for topic in valid if any(len(p.isr) < len(p.replicas)
                                                       for p in topic.partitions))
            data = {"brokerOnline": sum(1 for b in known if b.get("status") == "online"),
                    "brokerOffline": sum(1 for b in known if b.get("status") != "online"),
                    "topicTotal": len(valid), "topicOffline": offline,
                    "topicUnderReplicated": under_rep, "controllerId": controller_id}
            monitoring.record_health_snapshot(cluster.id, data, int(time.time()))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Health sampling failed for %s: %s", cluster.id, exc)
            monitoring.record_health_error(cluster.id, str(exc))
        finally:
            if client:
                client.close()
