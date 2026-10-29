from contextlib import asynccontextmanager
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

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
from .runtime_config import seconds_from_env

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.lag_collector.start()
    try:
        yield
    finally:
        await app.state.lag_collector.stop()
        await app.state.client_sim.shutdown()


app = FastAPI(title="KAFLAB", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.sessions = SessionManager()
app.state.traces = TraceManager()
app.state.clusters = ClusterManager()
app.state.producer = ProducerEngine(app.state.sessions, app.state.traces)
app.state.consumer = ConsumerEngine(app.state.sessions, app.state.traces)
app.state.client_sim = ClientSimManager()
app.state.lag_collector = LagCollector(
    app.state.clusters, ConsumerEngine(app.state.sessions, DiscardTraces()),
    interval_seconds=seconds_from_env("KCL_LAG_INTERVAL_SECONDS", 60, 10, 3600),
    timeout_seconds=seconds_from_env("KCL_LAG_TIMEOUT_SECONDS", 30, 1, 300))

app.include_router(cluster_router)
app.include_router(workbench_router)
app.include_router(loadtest_router)
app.include_router(stream_router)
app.include_router(client_sim_router)

static_dir = os.environ.get("KCL_STATIC_DIR")
if static_dir:
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="frontend")
