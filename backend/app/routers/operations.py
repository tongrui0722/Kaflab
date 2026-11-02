"""Authenticated operation task API."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from .permissions import _need_lab, principal, require_cluster

router = APIRouter()


@router.post("/api/clusters/{cid}/topics/{topic}/delete-task", status_code=202)
async def submit_delete(cid: str, topic: str, request: Request):
    _need_lab(request)
    require_cluster(request, cid)
    if request.app.state.clusters.get(cid) is None:
        raise HTTPException(404, "cluster not found")
    return request.app.state.operations.create_delete(
        request.app, cid, topic, principal(request)["username"])


@router.get("/api/operations")
async def list_operations(request: Request, clusterId: str = Query("")):
    user = principal(request)
    if clusterId:
        require_cluster(request, clusterId)
    allowed = user["clusters"]
    if clusterId:
        rows = request.app.state.operations.list(clusterId)
    elif allowed is None:
        rows = request.app.state.operations.list()
    else:
        rows = [row for cid in allowed for row in request.app.state.operations.list(cid)]
        rows.sort(key=lambda row: (row["created"], row["id"]), reverse=True)
        rows = rows[:100]
    return {"operations": [row for row in rows if allowed is None or row["cluster"] in allowed]}


@router.get("/api/operations/{task_id}")
async def get_operation(task_id: str, request: Request):
    task = request.app.state.operations.get(task_id)
    if task is None:
        raise HTTPException(404, "operation not found")
    require_cluster(request, task["cluster"])
    return task
