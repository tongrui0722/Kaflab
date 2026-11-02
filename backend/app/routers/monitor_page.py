"""Monitoring view and alert thresholds."""
from __future__ import annotations

import time
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from .. import monitoring
from .permissions import _need_lab

router = APIRouter()


class AlertRules(BaseModel):
    broker_offline: int = Field(default=1, ge=1, le=1000)
    topic_offline: int = Field(default=1, ge=1, le=1000000)
    topic_under_replicated: int = Field(default=1, ge=1, le=1000000)
    lag_threshold: int | None = Field(default=None, ge=0)


@router.get("/api/clusters/{cid}/monitoring")
async def monitoring_page(cid: str, request: Request):
    if request.app.state.clusters.get(cid) is None:
        raise HTTPException(404, "cluster not found")
    now = int(time.time())
    view = monitoring.health_view(cid, now - 86400)
    lag = monitoring.get_collector(cid)
    lag_points = monitoring.query_points(cid, "group_lag", now - 86400)
    last_lag = [p for p in lag_points if p["ts"] == lag["last_ok"]]
    lag_total = sum(p["value"] for p in last_lag) if last_lag else None
    latest = view["latest"]
    rules = view["rules"]
    alerts = []
    if (latest and now - latest["ts"] <= 150 and view["collector"]
            and not view["collector"]["last_error"]):
        for metric, key in (("Broker offline", "broker_offline"),
                            ("Topic offline", "topic_offline"),
                            ("Under replicated Topic", "topic_under_replicated")):
            column = {"broker_offline": "broker_offline", "topic_offline": "topic_offline",
                      "topic_under_replicated": "topic_under_replicated"}[key]
            if latest[column] >= rules[key]:
                alerts.append({"type": key, "message": metric, "value": latest[column]})
    if (lag_total is not None and not lag["last_error"] and lag["last_ok"]
            and now - lag["last_ok"] <= 150 and rules["lag_threshold"] is not None
            and lag_total >= rules["lag_threshold"]):
        alerts.append({"type": "lag", "message": "Consumer Lag", "value": lag_total})
    lag_state = ("disabled" if not request.app.state.clusters.get(cid).monitoringEnabled
                 else "collection_failed" if lag["last_error"] and lag["last_error"].startswith("Lag ")
                 else "stale" if not lag["last_ok"] or now - lag["last_ok"] > 150
                 else "partial" if lag["last_error"] else "healthy")
    return {**view, "lag": {**lag, "total": lag_total, "points": lag_points,
                             "state": lag_state},
            "alerts": alerts, "serverTime": now,
            "healthState": ("collection_failed" if view["collector"] and view["collector"]["last_error"]
                            else "stale" if not latest or now - latest["ts"] > 150
                            else "abnormal" if alerts else "healthy")}


@router.put("/api/clusters/{cid}/monitoring/rules")
async def update_alert_rules(cid: str, body: AlertRules, request: Request):
    _need_lab(request)
    if request.app.state.clusters.get(cid) is None:
        raise HTTPException(404, "cluster not found")
    return monitoring.save_alert_rules(cid, body.model_dump())
