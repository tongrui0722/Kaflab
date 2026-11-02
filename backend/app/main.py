from contextlib import asynccontextmanager
import os
import json
import time
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from .routers.permissions import principal, require_cluster, require_operation, operation_scope

from .cluster.manager import ClusterManager
from .consumer.engine import ConsumerEngine
from .producer.engine import ProducerEngine
from .routers.api import router as cluster_router
from .routers.workbench import router as workbench_router
from .routers.loadtest import router as loadtest_router
from .routers.stream import router as stream_router
from .session.manager import SessionManager
from .trace.manager import TraceManager
from .client_sim.manager import ClientSimManager
from .client_sim.routes import router as client_sim_router
from .lag_collector import DiscardTraces, LagCollector
from .health_collector import HealthCollector
from .routers.monitor_page import router as monitor_page_router
from .routers.group_reset import router as group_reset_router
from .runtime_config import seconds_from_env
from .operations import OperationManager
from .routers.operations import router as operations_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.lag_collector.start()
    app.state.health_collector.start()
    app.state.operations.start(app)
    try:
        yield
    finally:
        await app.state.operations.stop()
        await app.state.health_collector.stop()
        await app.state.lag_collector.stop()
        await app.state.client_sim.shutdown()


app = FastAPI(title="KAFLAB", lifespan=lifespan)

@app.middleware("http")
async def security_boundary(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS" or path in {"/api/health", "/api/auth/login"} or not path.startswith("/api/"):
        return await call_next(request)
    try:
        current = principal(request)
        parts = path.split("/")
        if len(parts) > 3 and parts[2] == "clusters":
            require_cluster(request, parts[3])
        if current["clusters"] is not None and not (
            path == "/api/clusters" or path.startswith("/api/clusters/")
            or path == "/api/operations" or path.startswith("/api/operations/")
            or path.startswith("/api/auth/")
            or path in {"/api/client-sim/catalog", "/api/client-sim/config-schema"}
        ):
            raise HTTPException(403, "受限账号不能访问未绑定集群的记录或操作")
        require_operation(current, operation_scope(request.method, path))
        request.state.principal = current
    except HTTPException as exc:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
    response = await call_next(request)
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and path not in {"/api/auth/login", "/api/auth/mode"}:
        audit_path = Path(os.environ.get("KCL_DATA_DIR") or str(Path(__file__).resolve().parents[1])) / "audit.jsonl"
        event = {"at": int(time.time() * 1000), "user": current["username"],
                 "mode": current["mode"], "method": request.method,
                 "path": path, "status": response.status_code}
        try:
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(audit_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")
        except OSError:
            import logging
            logging.getLogger("kaflab.audit").exception("Could not persist operation audit")
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.operations = OperationManager()
app.state.sessions = SessionManager()
app.state.traces = TraceManager()
app.state.clusters = ClusterManager()
app.state.producer = ProducerEngine(app.state.sessions, app.state.traces)
app.state.consumer = ConsumerEngine(app.state.sessions, app.state.traces)
app.state.client_sim = ClientSimManager()
app.state.health_collector = HealthCollector(app.state.clusters)
app.state.lag_collector = LagCollector(
    app.state.clusters, ConsumerEngine(app.state.sessions, DiscardTraces()),
    interval_seconds=seconds_from_env("KCL_LAG_INTERVAL_SECONDS", 60, 10, 3600),
    timeout_seconds=seconds_from_env("KCL_LAG_TIMEOUT_SECONDS", 30, 1, 300))

app.include_router(cluster_router)
app.include_router(operations_router)
app.include_router(monitor_page_router)
app.include_router(group_reset_router)
app.include_router(workbench_router)
app.include_router(loadtest_router)
app.include_router(stream_router)
app.include_router(client_sim_router)

static_dir = os.environ.get("KCL_STATIC_DIR")
if static_dir:
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="frontend")
