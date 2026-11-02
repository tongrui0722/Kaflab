from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from .models import RunCreate
from .topics import TopicConnection, topic_admin
from .registry import catalog
from .config_schema import config_schema
from ..routers.connection import managed_fields

router = APIRouter(prefix="/api/client-sim")


def _authorize(request: Request, write: bool = False) -> None:
    # Reuse KAFLAB's existing login/mode model.
    from ..routers.api import _auth_mode
    mode = _auth_mode(request)
    if mode is None:
        raise HTTPException(401, "登录已失效，请重新登录")
    if write and mode != "full":
        raise HTTPException(403, "只读模式下不能启动或停止客户端模拟")


@router.get("/catalog")
async def get_catalog(request: Request):
    _authorize(request)
    return {"clients": catalog()}


@router.get("/config-schema")
async def get_config_schema(request: Request, client: str, version: str, kind: str):
    _authorize(request)
    try:
        return {"options": config_schema(client, version, kind)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs")
async def list_runs(request: Request):
    _authorize(request)
    return {"runs": request.app.state.client_sim.list()}


@router.post("/topics/list")
async def list_topics(connection: TopicConnection, request: Request):
    _authorize(request)
    if connection.clusterId:
        connection = connection.model_copy(update=managed_fields(request, connection.clusterId))
    try:
        return await topic_admin("list", connection.model_dump())
    except ValueError as exc:
        raise HTTPException(502, str(exc)) from exc


@router.post("/runs", status_code=201)
async def create_run(spec: RunCreate, request: Request):
    _authorize(request, write=True)
    if spec.clusterId:
        spec = spec.model_copy(update=managed_fields(request, spec.clusterId))
    try:
        return await request.app.state.client_sim.start(spec)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/runs/{run_id}")
async def get_run(run_id: str, request: Request):
    _authorize(request)
    run = request.app.state.client_sim.get(run_id)
    if run is None:
        raise HTTPException(404, "运行记录不存在")
    return {key: value for key, value in run.items() if not key.startswith("_")}


@router.get("/runs/{run_id}/logs")
async def get_logs(run_id: str, request: Request, after: int = Query(0, ge=0)):
    _authorize(request)
    try:
        return request.app.state.client_sim.logs(run_id, after)
    except KeyError as exc:
        raise HTTPException(404, "运行记录不存在") from exc


@router.get("/runs/{run_id}/logs/raw")
async def get_raw_logs(run_id: str, request: Request):
    _authorize(request)
    try:
        content = request.app.state.client_sim.raw_logs(run_id)
    except KeyError as exc:
        raise HTTPException(404, "运行记录不存在") from exc
    return PlainTextResponse(content, headers={"Content-Disposition": f'attachment; filename="{run_id}-raw.log"'})


@router.post("/runs/{run_id}/stop")
async def stop_run(run_id: str, request: Request):
    _authorize(request, write=True)
    try:
        return await request.app.state.client_sim.stop(run_id)
    except KeyError as exc:
        raise HTTPException(404, "运行记录不存在") from exc
