"""后台消费组 Lag 采样，不依赖登录会话或浏览器页面。"""
from __future__ import annotations

import asyncio
import logging
import math
import time

from . import monitoring
from .cluster.manager import ClusterConfig, ClusterManager
from .consumer.engine import ConsumerEngine
from .models import GroupDescribeRequest

logger = logging.getLogger(__name__)


class DiscardTraces:
    """定时探测不进入交互式协议追踪缓存，避免长期运行占用内存。"""

    def append(self, *_args, **_kwargs) -> None:
        pass


class LagCollector:
    def __init__(self, clusters: ClusterManager, consumer: ConsumerEngine,
                 interval_seconds: float = 60, timeout_seconds: float = 30):
        self.clusters = clusters
        self.consumer = consumer
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="lag-collector")

    async def stop(self) -> None:
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        try:
            while True:
                started = time.monotonic()
                try:
                    await self.run_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    logger.exception("Lag sampling cycle failed")
                await asyncio.sleep(max(0, self.interval_seconds - (time.monotonic() - started)))
        except asyncio.CancelledError:
            raise
        finally:
            for cluster in self.clusters.list():
                try:
                    status = await asyncio.to_thread(monitoring.get_collector, cluster.id)
                    await asyncio.to_thread(monitoring.set_collector, cluster.id, False,
                                            status["last_ok"], status["last_error"])
                except Exception:  # noqa: BLE001
                    logger.exception("Could not stop Lag collector status for %s", cluster.id)

    async def run_cycle(self) -> None:
        # Read the current list every cycle so newly saved clusters are included.
        clusters = self.clusters.list()
        for cluster in clusters:
            if not cluster.monitoringEnabled:
                status = await asyncio.to_thread(monitoring.get_collector, cluster.id)
                if status["running"]:
                    await asyncio.to_thread(monitoring.set_collector, cluster.id, False,
                                            status["last_ok"], status["last_error"])
        results = await asyncio.gather(
            *(self.collect_cluster(cluster) for cluster in clusters if cluster.monitoringEnabled),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.error("Lag sampling task failed unexpectedly: %s", result)

    async def collect_cluster(self, cluster: ClusterConfig) -> None:
        try:
            req = GroupDescribeRequest(
                bootstrap=cluster.bootstrap, groupId="probe", clientId="kafka-lag-collector",
                requestTimeoutMs=int(self.timeout_seconds * 1000),
                securityProtocol=cluster.securityProtocol, saslMechanism=cluster.saslMechanism,
                saslUsername=cluster.saslUsername, saslPassword=cluster.saslPassword,
            )
            snapshot = await asyncio.wait_for(
                self.consumer.groups_overview(req), timeout=self.timeout_seconds)
            groups = snapshot.get("groups", [])
            list_errors = snapshot.get("listGroupsErrors", [])
            group_errors = [g for g in groups if g.get("error")]
            if list_errors or group_errors:
                raise RuntimeError(
                    f"消费组采集不完整：{len(list_errors)} 个 Broker、{len(group_errors)} 个消费组失败")
            valid = [g for g in groups if isinstance(g.get("totalLag"), (int, float))
                     and math.isfinite(g["totalLag"]) and g["totalLag"] >= 0
                     and isinstance(g.get("groupId"), str) and g["groupId"]]
            if len(valid) != len(groups):
                raise RuntimeError("消费组 Lag 数据不完整")
            current = self.clusters.get(cluster.id)
            if current is None or not current.monitoringEnabled:
                return
            partial = sum(int(g.get("uncommitted") or 0) for g in valid)
            ts = int(time.time())
            await asyncio.to_thread(monitoring.record_lag_snapshot, cluster.id, valid, ts)
            await asyncio.to_thread(monitoring.set_collector, cluster.id, True, ts,
                                    f"{partial} 个分区位点未知，Lag 为近似值" if partial else None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            current = self.clusters.get(cluster.id)
            if current is None or not current.monitoringEnabled:
                return
            message = (f"Lag 采样超时（{self.timeout_seconds:g} 秒）" if isinstance(exc, TimeoutError)
                       else f"Lag 采样失败：{exc}")[:250]
            status = await asyncio.to_thread(monitoring.get_collector, cluster.id)
            await asyncio.to_thread(monitoring.set_collector, cluster.id, True,
                                    status["last_ok"], message)
            logger.warning("Lag sampling failed for cluster %s: %s", cluster.id, message)
