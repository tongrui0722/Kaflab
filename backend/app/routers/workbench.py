from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request

from ..consumer.engine import ConsumerEngine
from ..models import ConsumerFetchRequest, GroupDescribeRequest, ProduceRequest, SecurityConfig
from ..network.client import KafkaClient
from ..network.tcp import parse_hostport
from ..protocol import codec
from .permissions import _need_lab
from .connection import managed_fields

router = APIRouter()


@router.get("/api/cluster/status")
async def cluster_status(request: Request, bootstrap: str = Query(""),
                         clusterId: str = Query(""),
                         topic: str = Query("test-topic"),
                         securityProtocol: str = Query("PLAINTEXT"),
                         saslMechanism: str = Query("PLAIN"),
                         saslUsername: str = Query(""),
                         saslPassword: str = Query("")):
    """Real probe: TCP + ApiVersions + Metadata. No mock — failures return 502."""
    import uuid as _u
    from ..models import KafkaTraceEvent
    if clusterId:
        fields = managed_fields(request, clusterId)
        bootstrap = fields["bootstrap"]
        securityProtocol = fields["securityProtocol"]
        saslMechanism = fields["saslMechanism"]
        saslUsername = fields["saslUsername"]
        saslPassword = fields["saslPassword"]
    host, port = parse_hostport(bootstrap or "localhost:19092")
    sec = SecurityConfig(securityProtocol=securityProtocol,  # type: ignore[arg-type]
                         saslMechanism=saslMechanism,  # type: ignore[arg-type]
                         saslUsername=saslUsername, saslPassword=saslPassword)
    tid, sid = f"tr-{_u.uuid4().hex[:8]}", "sess-probe"
    c = KafkaClient(request.app.state.traces, sid, tid, "kafka-sim-01",
                    8000, host, port, sec)
    try:
        await c.connect(1, label="probe")
        api_dec, _, _ = await c.call(18, "ApiVersions", 0,
                                     codec.encode_apiversions_v0(), 2,
                                     "apiversions", extra_req={"probe": True},
                                     decode=codec.decode_apiversions_v0)
        meta_dec, _, _ = await c.call(3, "Metadata", 0,
                                      codec.encode_metadata_v0([topic]),
                                      3, "metadata",
                                      extra_req={"topics": [topic]},
                                      topic=topic, decode=codec.decode_metadata_v0)
        from ..producer.shared import _metadata_dict
        meta = _metadata_dict(meta_dec)
        tmeta = next((x for x in meta["topics"] if x["topic"] == topic), None)
        parts = tmeta["partitions"] if tmeta else []
        return {"connected": True, "brokers": meta["brokers"],
                "brokerCount": len(meta["brokers"]),
                "topicCount": len(meta["topics"]),
                "partitionCount": len(parts),
                "topic": topic, "partitions": parts,
                "events": request.app.state.traces.history(tid)}
    except Exception as e:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        raise HTTPException(502, f"cluster unreachable: {type(e).__name__}: {e}")
    finally:
        c.close()


@router.post("/api/sessions")
async def create_session(body: dict, request: Request):
    s = request.app.state.sessions.create(
        client_id=body.get("clientId", "kafka-sim-01"),
        operation=body.get("operation", "Producer"),
        topic=body.get("topic", "test-topic"),
        bootstrap=body.get("bootstrap", ""),
    )
    return s


@router.get("/api/sessions")
async def list_sessions(request: Request):
    return request.app.state.sessions.list()


@router.get("/api/sessions/{sid}")
async def get_session(sid: str, request: Request):
    s = request.app.state.sessions.get(sid)
    if not s:
        raise HTTPException(404, "session not found")
    return s


@router.post("/api/producer/send")
async def producer_send(req: ProduceRequest, background: BackgroundTasks, request: Request):
    _need_lab(request)
    if req.clusterId:
        req = req.model_copy(update=managed_fields(request, req.clusterId))
    sessions = request.app.state.sessions
    engine = request.app.state.producer
    if req.sessionId and sessions.get(req.sessionId):
        sess = sessions.get(req.sessionId)
        assert sess is not None
    else:
        sess = sessions.create(client_id=req.clientId, operation="Producer",
                               topic=req.topic, bootstrap=req.bootstrap)
    background.add_task(engine.run, req, sess.id)
    return {"sessionId": sess.id, "status": "running", "mode": "live"}


@router.get("/api/metadata")
async def get_metadata(request: Request, bootstrap: str = Query(""),
                       clusterId: str = Query(""),
                       topic: str = Query("test-topic"),
                       securityProtocol: str = Query("PLAINTEXT"),
                       saslMechanism: str = Query("PLAIN"),
                       saslUsername: str = Query(""),
                       saslPassword: str = Query("")):
    import uuid as _u
    if clusterId:
        fields = managed_fields(request, clusterId)
        bootstrap = fields["bootstrap"]
        securityProtocol = fields["securityProtocol"]
        saslMechanism = fields["saslMechanism"]
        saslUsername = fields["saslUsername"]
        saslPassword = fields["saslPassword"]
    host, port = parse_hostport(bootstrap or "localhost:19092")
    sec = SecurityConfig(securityProtocol=securityProtocol,  # type: ignore[arg-type]
                         saslMechanism=saslMechanism,  # type: ignore[arg-type]
                         saslUsername=saslUsername, saslPassword=saslPassword)
    tid = f"tr-{_u.uuid4().hex[:8]}"
    c = KafkaClient(request.app.state.traces, "sess-meta", tid, "kafka-sim-01",
                    8000, host, port, sec)
    try:
        await c.connect(1, label="metadata")
        meta_dec, _, _ = await c.call(3, "Metadata", 0,
                                      codec.encode_metadata_v0([topic]),
                                      2, "metadata",
                                      extra_req={"topics": [topic]},
                                      topic=topic, decode=None)
        from ..producer.shared import _metadata_dict
        meta = _metadata_dict(meta_dec)
        return {"brokers": meta["brokers"],
                "topics": [{"topic": t["topic"], "error": t["error"],
                            "partitions": [{"partition": p["partition"],
                                            "leader": p["leader"],
                                            "replicas": p["replicas"],
                                            "isr": p["isr"]}
                                           for p in t["partitions"]]}
                           for t in meta["topics"]]}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"metadata failed: {e}")
    finally:
        c.close()


@router.get("/api/traces")
async def list_traces(request: Request, limit: int = 50):
    """最近落盘 trace 摘要（供 Protocol Inspector 选择回放，无需手抄 traceId）。"""
    return request.app.state.traces.list_recent(limit=min(max(limit, 1), 200))


@router.get("/api/traces/{trace_id}")
async def get_trace(trace_id: str, request: Request):
    return request.app.state.traces.history(trace_id)


@router.post("/api/consumer/fetch")
async def consumer_fetch(req: ConsumerFetchRequest, background: BackgroundTasks,
                         request: Request):
    if (req.groupId or "").strip():
        _need_lab(request)  # 带 Group 会提交位点，视为写入
    if req.clusterId:
        req = req.model_copy(update=managed_fields(request, req.clusterId))
    sessions = request.app.state.sessions
    engine: ConsumerEngine = request.app.state.consumer
    sess = sessions.create(client_id=req.clientId, operation="Consumer",
                           topic=req.topic, bootstrap=req.bootstrap)
    background.add_task(engine.fetch, req, sess.id)
    return {"sessionId": sess.id, "status": "running"}


@router.post("/api/groups/describe")
async def group_describe(req: GroupDescribeRequest, request: Request):
    if req.clusterId:
        req = req.model_copy(update=managed_fields(request, req.clusterId))
    engine: ConsumerEngine = request.app.state.consumer
    try:
        return await engine.describe_group(req)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"describe group failed: {e}")


@router.get("/api/groups")
async def list_groups(request: Request, bootstrap: str = Query(""),
                      securityProtocol: str = Query("PLAINTEXT"),
                      saslMechanism: str = Query("PLAIN"),
                      saslUsername: str = Query(""),
                      saslPassword: str = Query("")):
    req = GroupDescribeRequest(bootstrap=bootstrap or "localhost:19092",
                               groupId="probe",
                               securityProtocol=securityProtocol,  # type: ignore[arg-type]
                               saslMechanism=saslMechanism,  # type: ignore[arg-type]
                               saslUsername=saslUsername, saslPassword=saslPassword)
    engine: ConsumerEngine = request.app.state.consumer
    try:
        out = await engine.describe_group(req)
        return out.get("groups", [])
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"list groups failed: {e}")


