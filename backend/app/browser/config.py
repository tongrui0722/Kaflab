"""消息浏览集中配置：所有上限收敛在这里，不散落各处。"""
from __future__ import annotations


class BrowserLimits:
    default_limit: int = 100
    max_limit: int = 1000
    max_scan_records: int = 10000
    max_scan_seconds: float = 30.0
    max_result_records: int = 1000
    fetch_max_bytes: int = 1048576
    live_poll_interval_ms: int = 2000
    live_max_cache: int = 1000


LIMITS = BrowserLimits()
