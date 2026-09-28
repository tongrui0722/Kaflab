from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .cluster.manager import ClusterManager
from .consumer.engine import ConsumerEngine
from .fault.engine import FaultEngine
from .producer.engine import ProducerEngine
from .routers.api import router
from .session.manager import SessionManager
from .trace.manager import TraceManager

app = FastAPI(title="Kafka Client Lab")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.sessions = SessionManager()
app.state.traces = TraceManager()
app.state.faults = FaultEngine()
app.state.clusters = ClusterManager()
app.state.producer = ProducerEngine(app.state.sessions, app.state.traces, app.state.faults)
app.state.consumer = ConsumerEngine(app.state.sessions, app.state.traces)

app.include_router(router)
