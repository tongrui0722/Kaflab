from __future__ import annotations
import asyncio
import json
import os
import secrets
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, WebSocket, WebSocketDisconnect

from ..consumer.engine import ConsumerEngine
from ..models import ConsumerFetchRequest, GroupDescribeRequest, ProduceRequest, TopicCreationRequest, error_name
from ..network.client import KafkaClient
from ..models import SecurityConfig
from ..network.tcp import TcpConnector, parse_hostport
from ..protocol import codec

router = APIRouter()

_sec_of = lambda r: SecurityConfig(securityProtocol=r.securityProtocol,
                                   saslMechanism=r.saslMechanism,
                                   saslUsername=r.saslUsername,
                                   saslPassword=r.saslPassword)


# ---- 登录与模式鉴权 ----
# mode=readonly：只读巡检；mode=full：全部功能。token 内存保存，重启后端失效。
_auth_tokens: dict[str, str] = {}
_USERS_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "users.json")

def _load_users() -> list[dict]:
    """每次登录都读文件：运行时改 users.json 即时生效，无需重启。"""
    try:
        with open(_USERS_FILE, encoding="utf-8") as f:
            data = json.load(f)
            users = data.get("users", [])
            if isinstance(users, list):
                return [u for u in users if u.get("username") and u.get("password")]
    except FileNotFoundError:
        pass
    except Exception:
        pass
    # 文件缺失或非法：落盘默认账号后返回
    default = {"users": [{"username": "admin", "password": os.environ.get("KCL_PASSWORD", "kafka-lab")}]}
    try:
        with open(_USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return default["users"]

def _password() -> str:
    return os.environ.get("KCL_PASSWORD", "kafka-lab")


@router.post("/api/auth/login")
async def auth_login(body: dict):
    mode = body.get("mode", "readonly")
    if mode not in ("readonly", "full"):
        raise HTTPException(400, "mode must be readonly|full")
    username = (body.get("username", "") or "").strip()
    ok = any(u.get("username") == username and u.get("password") == body.get("password", "")
             for u in _load_users())
    if not username or not ok:
        raise HTTPException(401, "用户名或密码错误")
    tok = secrets.token_hex(16)
    _auth_tokens[tok] = mode
    return {"token": tok, "mode": mode, "username": username}


def _auth_mode(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return _auth_tokens.get(auth[7:])
    return None


def _need_lab(request: Request) -> None:
    mode = _auth_mode(request)
    if mode is None:
        raise HTTPException(401, "登录已失效（后端重启或 token 过期），请退出后重新登录")
    if mode != "full":
        raise HTTPException(403, "只读模式：该操作需要【全部功能】模式登录")


@router.get("/api/auth/me")
async def auth_me(request: Request):
    mode = _auth_mode(request)
    if mode is None:
        raise HTTPException(401, "unauthorized")
    return {"mode": mode}


@router.get("/api/health")
async def health():
    return {"ok": True, "service": "kafka-client-lab"}


@router.get("/api/cluster/status")
async def cluster_status(request: Request, bootstrap: str = Query(""),
                         topic: str = Query("test-topic"),
                         securityProtocol: str = Query("PLAINTEXT"),
                         saslMechanism: str = Query("PLAIN"),
                         saslUsername: str = Query(""),
                         saslPassword: str = Query("")):
    """Real probe: TCP + ApiVersions + Metadata. No mock — failures return 502."""
    import uuid as _u
    from ..models import KafkaTraceEvent
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
                       topic: str = Query("test-topic"),
                       securityProtocol: str = Query("PLAINTEXT"),
                       saslMechanism: str = Query("PLAIN"),
                       saslUsername: str = Query(""),
                       saslPassword: str = Query("")):
    import uuid as _u
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


@router.get("/api/clusters")
async def list_clusters(request: Request):
    return [c.model_dump() for c in request.app.state.clusters.list()]


@router.post("/api/clusters")
async def save_cluster(body: dict, request: Request):
    try:
        return request.app.state.clusters.save(body or {}).model_dump()
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.delete("/api/clusters/{cid}")
async def delete_cluster(cid: str, request: Request):
    if not request.app.state.clusters.delete(cid):
        raise HTTPException(404, "cluster not found")
    return {"deleted": cid}


@router.post("/api/clusters/{cid}/test")
async def test_cluster(cid: str, request: Request):
    """真实探测指定纳管集群：TCP + ApiVersions + Metadata（全 topics 计数）。"""
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    import uuid as _u
    host, port = parse_hostport(c.bootstrap)
    sec = SecurityConfig(securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
                         saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
                         saslUsername=c.saslUsername, saslPassword=c.saslPassword)
    tid = f"tr-{_u.uuid4().hex[:8]}"
    kc = KafkaClient(request.app.state.traces, "sess-probe", tid, "kafka-sim-01",
                     8000, host, port, sec)
    try:
        await kc.connect(1, label="probe")
        await kc.call(18, "ApiVersions", 0, codec.encode_apiversions_v0(), 2,
                      "apiversions", extra_req={"probe": True},
                      decode=codec.decode_apiversions_v0)
        meta_dec, _, _ = await kc.call(3, "Metadata", 0,
                                       codec.encode_metadata_v0([]),
                                       3, "metadata", extra_req={"topics": "ALL"},
                                       decode=codec.decode_metadata_v0)
        from ..producer.shared import _metadata_dict
        meta = _metadata_dict(meta_dec)
        return {"connected": True, "brokers": meta["brokers"],
                "brokerCount": len(meta["brokers"]),
                "topicCount": len(meta["topics"])}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"cluster unreachable: {type(e).__name__}: {e}")
    finally:
        kc.close()


@router.get("/api/clusters/{cid}/topics")
async def cluster_topics(cid: str, request: Request):
    """该集群全部 topic 概览（kafka-ui Topics 页对应功能）。"""
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    import uuid as _u
    host, port = parse_hostport(c.bootstrap)
    sec = SecurityConfig(securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
                         saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
                         saslUsername=c.saslUsername, saslPassword=c.saslPassword)
    tid = f"tr-{_u.uuid4().hex[:8]}"
    kc = KafkaClient(request.app.state.traces, "sess-topics", tid, "kafka-sim-01",
                     8000, host, port, sec)
    try:
        await kc.connect(1, label="topics")
        meta_dec, _, _ = await kc.call(3, "Metadata", 0,
                                       codec.encode_metadata_v0([]),
                                       3, "metadata", extra_req={"topics": "ALL"},
                                       decode=codec.decode_metadata_v0)
        from ..producer.shared import _metadata_dict
        meta = _metadata_dict(meta_dec)
        out = []
        for t in meta["topics"]:
            ps = t.get("partitions", [])
            out.append({
                "topic": t["topic"],
                "partitions": len(ps),
                "replication": max((len(p.get("replicas", [])) for p in ps), default=0),
                "leaders": sorted({p.get("leader") for p in ps if p.get("leader", -1) >= 0}),
                "hasOffline": any(p.get("leader", -1) < 0 for p in ps),
            })
        out.sort(key=lambda x: x["topic"])
        return {"clusterId": cid, "brokers": meta["brokers"], "topics": out}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"topics failed: {type(e).__name__}: {e}")
    finally:
        kc.close()


@router.get("/api/clusters/{cid}/topics/{topic}/messages")
async def cluster_topic_messages(cid: str, topic: str, request: Request):
    """Topic 消息浏览（只读）：临时 Consumer 查完即关，不建 Group、不提交位点。

    参数：partition=all|N · mode=latest|earliest|offset|timestamp ·
    offset · startTime/endTime(ISO 或 ms) · limit(≤1000) · key/value(contains) ·
    from={partition: offset} 断点续查（Load More / 实时轮询）。
    上限集中在 browser/config.py。
    """
    from ..browser.service import BrowseError, browse
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    q = dict(request.query_params)
    try:
        return await browse(c, topic, q)
    except BrowseError as e:
        raise HTTPException(502, str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"查询失败：{type(e).__name__}")


@router.get("/api/clusters/{cid}/topics/{topic}")
async def cluster_topic_detail(cid: str, topic: str, request: Request):
    """单个 topic 分区详情（leader/replicas/isr，kafka-ui Topic Overview 对应）。"""
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    import uuid as _u
    host, port = parse_hostport(c.bootstrap)
    sec = SecurityConfig(securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
                         saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
                         saslUsername=c.saslUsername, saslPassword=c.saslPassword)
    tid = f"tr-{_u.uuid4().hex[:8]}"
    kc = KafkaClient(request.app.state.traces, "sess-topic", tid, "kafka-sim-01",
                     8000, host, port, sec)
    try:
        await kc.connect(1, label="topic-detail")
        meta_dec, _, _ = await kc.call(3, "Metadata", 0,
                                       codec.encode_metadata_v0([topic]),
                                       3, "metadata",
                                       extra_req={"topics": [topic]},
                                       topic=topic, decode=codec.decode_metadata_v0)
        from ..producer.shared import _metadata_dict
        meta = _metadata_dict(meta_dec)
        t = next((x for x in meta["topics"] if x["topic"] == topic), None)
        if not t:
            raise HTTPException(404, "topic not found in metadata")
        return {"clusterId": cid, "topic": topic,
                "brokers": meta["brokers"], "partitions": t.get("partitions", [])}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"topic detail failed: {type(e).__name__}: {e}")
    finally:
        kc.close()


def _cluster_client(cid: str, request: Request, label: str,
                    broker: dict | None = None):
    """Topic 管理类操作的公用连接：返回 (cluster, KafkaClient)。"""
    import uuid as _u
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    host, port = ((str(broker["host"]), int(broker["port"]))
                  if broker else parse_hostport(c.bootstrap))
    sec = SecurityConfig(securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
                         saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
                         saslUsername=c.saslUsername, saslPassword=c.saslPassword)
    tid = f"tr-{_u.uuid4().hex[:8]}"
    kc = KafkaClient(request.app.state.traces, f"sess-{label}", tid, "kafka-sim-01",
                     8000, host, port, sec,
                     broker_id=(broker.get("id") if broker else None))
    return c, kc


async def _ensure_admin_api(kc: KafkaClient, api_key: int, api_name: str) -> int:
    """ApiVersions 协商：确认 broker 支持该管理 API，返回 broker 支持的最大版本。"""
    api_dec, _, _ = await kc.call(18, "ApiVersions", 0, codec.encode_apiversions_v0(), 2,
                                  "apiversions", extra_req={"probe": api_name},
                                  decode=codec.decode_apiversions_v0)
    supported = {a["apiKey"]: (a["minVersion"], a["maxVersion"])
                 for a in api_dec.get("apis", [])}
    if api_key not in supported or supported[api_key][1] < 0:
        raise HTTPException(501, f"broker 不支持 {api_name}（API key {api_key}）")
    return supported[api_key][1]


async def _topic_cluster_metadata(cid: str, request: Request, label: str):
    """通过纳管 Cluster 的认证连接读取 Broker 列表及当前 Controller。"""
    _, probe = _cluster_client(cid, request, label)
    try:
        await probe.connect(0, label=label)
        metadata, _, _ = await probe.call(
            3, "Metadata", 1, codec.encode_metadata_v0([]), 1, "metadata",
            extra_req={"topics": "ALL", "controllerLookup": True},
            decode=codec.decode_metadata_v1)
        return metadata
    finally:
        probe.close()


async def _send_create_topic(cid: str, request: Request, broker: dict,
                             body: TopicCreationRequest, attempt: int):
    """一次性向指定 Broker 发送创建请求；不会重试传输异常或模糊结果。"""
    _, kc = _cluster_client(cid, request, f"topic-create-{attempt}", broker)
    try:
        await kc.connect(1, label=f"topic-create-controller-{attempt}")
        return await kc.call(
            19, "CreateTopics", 0,
            codec.encode_create_topics_v0([(
                body.name, body.partitions, body.replicationFactor, body.configs)]),
            3, "create-topics",
            extra_req={"topic": body.name, "partitions": body.partitions,
                       "replicationFactor": body.replicationFactor,
                       "configs": body.configs, "version": 0,
                       "controllerBrokerId": broker["id"], "attempt": attempt},
            topic=body.name, decode=codec.decode_create_topics_v0)
    finally:
        kc.close()


def _controller_broker(metadata) -> dict:
    controller_id = metadata.controller_id
    broker = next((b for b in metadata.brokers if b.get("id") == controller_id), None)
    if controller_id is None or controller_id < 0 or broker is None:
        raise HTTPException(503, "Kafka 集群暂时无法确定当前 Controller，请稍后重试")
    return broker


@router.post("/api/clusters/{cid}/topics", status_code=201)
async def cluster_topic_create(cid: str, body: TopicCreationRequest, request: Request):
    """复用当前 Cluster 认证，路由至 Kafka Controller 并等待 CreateTopics 响应。"""
    _need_lab(request)
    import logging
    import re

    name = body.name
    if (not name or name != name.strip() or len(name) > 249
            or not re.fullmatch(r"[a-zA-Z0-9._-]+", name)):
        raise HTTPException(400, "Topic 名称非法：只能包含字母、数字、点、下划线和连字符，长度为 1–249")

    log = logging.getLogger("kcl.topic")
    try:
        metadata = await _topic_cluster_metadata(cid, request, "topic-create-metadata")
        broker_count = len(metadata.brokers)
        if broker_count < 1:
            raise HTTPException(503, "无法从当前 Kafka 集群读取 Broker 信息，未发送创建请求")
        if body.replicationFactor > broker_count:
            raise HTTPException(400, f"副本数超过当前 Kafka 集群可用 Broker 数量（{broker_count}）")

        broker = _controller_broker(metadata)
        decoded, frame, raw = await _send_create_topic(cid, request, broker, body, 1)
        result = next((t for t in decoded.get("topics", [])
                       if t.get("topic") == name), None)

        # Kafka 明确返回 NOT_CONTROLLER (41) 表示该请求没有被执行；
        # 刷新 Controller 后只重发一次。其他错误/超时不重试，避免重复创建。
        if result and result.get("errorCode") == 41:
            log.warning("Kafka Controller changed while creating topic %s; refreshing metadata", name)
            metadata = await _topic_cluster_metadata(cid, request, "topic-create-controller-refresh")
            broker = _controller_broker(metadata)
            decoded, frame, raw = await _send_create_topic(cid, request, broker, body, 2)
            result = next((t for t in decoded.get("topics", [])
                           if t.get("topic") == name), None)

        if result is None or decoded.get("_end") != len(raw) - 4:
            log.error("CreateTopics response did not match request: topic=%s response=%s request=%s",
                      name, codec.hexdump(raw), codec.hexdump(frame))
            raise HTTPException(502, "Kafka 创建结果响应无法确认，请刷新 Topic 列表确认是否已创建")

        code = result.get("errorCode")
        if code != 0:
            log.error("Kafka CreateTopics rejected request: topic=%s errorCode=%s errorName=%s message=%s",
                      name, code, error_name(code), result.get("errorMessage"))
            status = (409 if code == 36 else 403 if code in (29, 31)
                      else 400 if code in (38, 39, 40) else 504 if code == 7
                      else 503 if code == 41 else 502)
            raise HTTPException(status, _topic_err_hint(code))

        return {"name": name, "partitions": body.partitions,
                "replicationFactor": body.replicationFactor,
                "configs": body.configs, "apiVersion": 0,
                "controllerBrokerId": broker["id"]}
    except HTTPException:
        raise
    except (TimeoutError, asyncio.TimeoutError) as e:
        log.exception("Kafka CreateTopics request timed out for %s", name)
        raise HTTPException(504, "Kafka Topic 创建请求超时，请刷新 Topic 列表确认是否已创建") from e
    except Exception as e:  # noqa: BLE001
        log.exception("Kafka CreateTopics failed for %s", name)
        raise HTTPException(502, f"Kafka Topic 创建请求失败：{type(e).__name__}；请刷新列表确认结果") from e


@router.delete("/api/clusters/{cid}/topics/{topic}")
async def cluster_topic_delete(cid: str, topic: str, request: Request):
    """删除 Topic（DeleteTopics v0 真实链路）。写入操作，需全部功能模式。"""
    _need_lab(request)
    from ..models import error_name
    try:
        for attempt in (1, 2):
            metadata = await _topic_cluster_metadata(cid, request, f"topic-delete-metadata-{attempt}")
            broker = _controller_broker(metadata)
            _, kc = _cluster_client(cid, request, f"topic-delete-{attempt}", broker)
            try:
                await kc.connect(1, label=f"topic-delete-controller-{attempt}")
                await _ensure_admin_api(kc, 20, "DeleteTopics")
                dec, _, _ = await kc.call(
                    20, "DeleteTopics", 0, codec.encode_delete_topics_v0([topic]),
                    3, "delete-topics",
                    extra_req={"topic": topic, "controllerBrokerId": broker["id"],
                               "attempt": attempt},
                    topic=topic, decode=codec.decode_delete_topics_v0)
            finally:
                kc.close()
            t = next((item for item in dec.get("topics", [])
                      if item.get("topic") == topic), None)
            code = (t or {}).get("errorCode", -1)
            if code != 41 or attempt == 2:
                break
        if code != 0:
            raise HTTPException(502, f"删除失败：{error_name(code)}（{code}）")
        return {"deleted": topic}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"delete topic failed: {type(e).__name__}: {e}")


def _topic_err_hint(code: int) -> str:
    """将 Kafka CreateTopics 常见错误映射成用户可读信息。"""
    from ..models import error_name
    hints = {
        17: "Topic 名称或格式无效，请检查 Topic 名称。",
        20: "副本未能达到要求的同步状态（ISR），请检查 Kafka Broker 和副本健康状态。",
        29: "当前 Kafka 用户没有创建 Topic 的权限。",
        31: "当前 Kafka 用户没有在该集群创建 Topic 的权限。",
        38: "副本数超过当前 Kafka 集群可用 Broker 数量。",
        39: "Kafka 拒绝了无效的副本分配。",
        40: "Topic 配置无效，请检查配置项和值。",
        7: "Kafka Topic 创建请求超时，请检查集群状态并刷新 Topic 列表确认结果。",
        36: "Topic 已存在，请修改 Topic 名称。",
        41: "Kafka Controller 正在切换，刷新后重试仍未完成；请稍后重试。",
    }
    return hints.get(code, f"Kafka 拒绝创建 Topic：{error_name(code)}（{code}）")


@router.post("/api/clusters/{cid}/topics/{topic}/partitions")
async def cluster_topic_expand(cid: str, topic: str, body: dict, request: Request):
    """分区扩容，只能增加（CreatePartitions v0 真实链路）。写入操作，需全部功能模式。"""
    _need_lab(request)
    from ..models import error_name
    count = int(body.get("count", 0))
    if not 1 <= count <= 1000:
        raise HTTPException(400, "目标分区数范围 1–1000（且只能比当前多）")
    try:
        for attempt in (1, 2):
            metadata = await _topic_cluster_metadata(cid, request, f"topic-expand-metadata-{attempt}")
            broker = _controller_broker(metadata)
            _, kc = _cluster_client(cid, request, f"topic-expand-{attempt}", broker)
            try:
                await kc.connect(1, label=f"topic-expand-controller-{attempt}")
                await _ensure_admin_api(kc, 37, "CreatePartitions")
                dec, _, _ = await kc.call(
                    37, "CreatePartitions", 0,
                    codec.encode_create_partitions_v0(topic, count), 3, "create-partitions",
                    extra_req={"topic": topic, "count": count,
                               "controllerBrokerId": broker["id"], "attempt": attempt},
                    topic=topic, decode=codec.decode_create_partitions_v0)
            finally:
                kc.close()
            r = next((item for item in dec.get("results", [])
                      if item.get("topic") == topic), None)
            code = (r or {}).get("errorCode", -1)
            if code != 41 or attempt == 2:
                break
        if code != 0:
            raise HTTPException(502, f"扩容失败：{error_name(code)}（{code}）{(r or {}).get('errorMessage') or ''}")
        return {"topic": topic, "partitions": count}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"expand partitions failed: {type(e).__name__}: {e}")


@router.post("/api/clusters/{cid}/topics/{topic}/reassignment")
async def cluster_topic_reassign(cid: str, topic: str, body: dict, request: Request):
    """调整副本数：按轮询生成分配方案并提交（AlterPartitionReassignments v0）。
    副本搬迁是异步过程，提交成功后需等待 ISR 追齐。写入操作，需全部功能模式。"""
    _need_lab(request)
    from ..models import error_name
    rf = int(body.get("replication", 0))
    if not 1 <= rf <= 10:
        raise HTTPException(400, "副本数范围 1–10")
    try:
        from ..producer.shared import _metadata_dict
        for attempt in (1, 2):
            _, meta_client = _cluster_client(cid, request, f"topic-reassign-metadata-{attempt}")
            try:
                await meta_client.connect(1, label=f"topic-reassign-metadata-{attempt}")
                meta_dec, _, _ = await meta_client.call(
                    3, "Metadata", 1, codec.encode_metadata_v0([topic]), 2, "metadata",
                    extra_req={"topics": [topic], "controllerLookup": True},
                    topic=topic, decode=codec.decode_metadata_v1)
            finally:
                meta_client.close()
            meta = _metadata_dict(meta_dec)
            brokers = [b.get("id") for b in meta["brokers"] if b.get("id") is not None]
            t = next((x for x in meta["topics"] if x["topic"] == topic), None)
            if not t or not t.get("partitions"):
                raise HTTPException(404, "topic 不存在")
            if rf > len(brokers):
                raise HTTPException(400, f"副本数 {rf} 超过 broker 数 {len(brokers)}")
            assignment = {}
            for i, p in enumerate(sorted(t["partitions"], key=lambda x: x["partition"])):
                assignment[p["partition"]] = [brokers[(i + k) % len(brokers)] for k in range(rf)]
            controller = _controller_broker(meta_dec)
            _, kc = _cluster_client(cid, request, f"topic-reassign-{attempt}", controller)
            try:
                await kc.connect(1, label=f"topic-reassign-controller-{attempt}")
                await _ensure_admin_api(kc, 45, "AlterPartitionReassignments")
                dec, _, raw = await kc.call(
                    45, "AlterPartitionReassignments", 0,
                    codec.encode_alter_reassignments_v0(topic, assignment),
                    3, "alter-reassignments",
                    extra_req={"topic": topic, "replication": rf,
                               "assignment": {str(k): v for k, v in assignment.items()},
                               "controllerBrokerId": controller["id"], "attempt": attempt},
                    topic=topic, decode=codec.decode_alter_reassignments_v0, flexible=True)
            finally:
                kc.close()
            partition_not_controller = any(
                p.get("errorCode") == 41
                for tp in dec.get("topics", [])
                for p in tp.get("partitions", []))
            if (dec.get("errorCode", 0) != 41 and not partition_not_controller) or attempt == 2:
                break
        if dec.get("errorCode", 0) != 0:
            raise HTTPException(502, f"提交失败：{error_name(dec['errorCode'])}（{dec['errorCode']}）{dec.get('errorMessage') or ''}")
        bad = [(p["partition"], p["errorCode"]) for tp in dec.get("topics", [])
               for p in tp.get("partitions", []) if p.get("errorCode", 0) != 0]
        if bad:
            raise HTTPException(502, f"部分分区失败：{bad}")
        return {"topic": topic, "replication": rf,
                "assignment": {str(k): v for k, v in assignment.items()},
                "note": "已提交，后台异步搬迁，待 ISR 追齐后生效"}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"reassign failed: {type(e).__name__}: {e}")


@router.get("/api/clusters/{cid}/groups")
async def cluster_groups(cid: str, request: Request):
    """该集群消费组概览（首页统计用）：逐 broker ListGroups 取并集 + 每组 LAG。
    注意不能只问单个 broker——各 broker 只认自己协调的组。"""
    from ..models import GroupDescribeRequest as _G
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    engine: ConsumerEngine = request.app.state.consumer
    try:
        out = await engine.groups_overview(_G(
            bootstrap=c.bootstrap, groupId="probe",
            clientId="kafka-sim-01", requestTimeoutMs=8000,
            securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
            saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
            saslUsername=c.saslUsername, saslPassword=c.saslPassword))
        groups = out.get("groups", [])
        return {"clusterId": cid, "count": len(groups),
                "groups": [{"groupId": g.get("groupId"), "state": g.get("state"),
                            "protocol": g.get("protocol") or g.get("protocolType"),
                            "members": g.get("memberCount", 0),
                            "totalLag": g.get("totalLag", 0)}
                           for g in groups]}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"groups failed: {type(e).__name__}: {e}")


@router.get("/api/clusters/{cid}/groups/overview")
async def cluster_groups_overview(cid: str, request: Request):
    """全量消费组一览：成员数/连接情况/消费者ID/LAG，点击某组再用 describe/lag 看明细。"""
    from ..models import GroupDescribeRequest as _G
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    engine: ConsumerEngine = request.app.state.consumer
    try:
        return await engine.groups_overview(_G(
            bootstrap=c.bootstrap, groupId="probe",
            clientId="kafka-sim-01", requestTimeoutMs=10000,
            securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
            saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
            saslUsername=c.saslUsername, saslPassword=c.saslPassword))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"groups overview failed: {type(e).__name__}: {e}")


@router.get("/api/clusters/{cid}/groups/{group_id}/lag")
async def cluster_group_lag(cid: str, group_id: str, request: Request,
                            topics: str = Query("")):
    """消费 LAG：每分区 log-end − 已提交，无提交则 lag 未知（N/A）。"""
    from ..models import GroupDescribeRequest as _G
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    engine: ConsumerEngine = request.app.state.consumer
    want = [t.strip() for t in topics.split(",") if t.strip()]
    try:
        return await engine.group_lag(_G(
            bootstrap=c.bootstrap, groupId=group_id,
            clientId="kafka-sim-01", requestTimeoutMs=10000,
            securityProtocol=c.securityProtocol,  # type: ignore[arg-type]
            saslMechanism=c.saslMechanism,  # type: ignore[arg-type]
            saslUsername=c.saslUsername, saslPassword=c.saslPassword), want)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"lag failed: {type(e).__name__}: {e}")


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
    sessions = request.app.state.sessions
    engine: ConsumerEngine = request.app.state.consumer
    sess = sessions.create(client_id=req.clientId, operation="Consumer",
                           topic=req.topic, bootstrap=req.bootstrap)
    background.add_task(engine.fetch, req, sess.id)
    return {"sessionId": sess.id, "status": "running"}


@router.post("/api/groups/describe")
async def group_describe(req: GroupDescribeRequest, request: Request):
    engine: ConsumerEngine = request.app.state.consumer
    try:
        return await engine.describe_group(req)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"describe group failed: {e}")


@router.post("/api/groups/rebalance")
async def group_rebalance(body: dict, request: Request):
    _need_lab(request)
    engine: ConsumerEngine = request.app.state.consumer
    req = GroupDescribeRequest(**{k: v for k, v in body.items()
                                  if k in GroupDescribeRequest.model_fields})
    topics = body.get("topics", ["test-topic"])
    try:
        return await engine.rebalance_demo(req, topics)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"rebalance demo failed: {e}")


@router.post("/api/groups/join")
async def group_join(body: dict, request: Request):
    _need_lab(request)
    engine: ConsumerEngine = request.app.state.consumer
    req = GroupDescribeRequest(**{k: v for k, v in body.items()
                                  if k in GroupDescribeRequest.model_fields})
    topics = body.get("topics", ["test-topic"])
    try:
        return await engine.join_sync_demo(req, topics)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"join group failed: {e}")


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


@router.get("/api/faults")
async def get_faults(request: Request):
    return {"faults": request.app.state.faults.faults}


@router.post("/api/faults")
async def set_fault(body: dict, request: Request):
    _need_lab(request)
    request.app.state.faults.set(body.get("name", "unknown"), bool(body.get("enabled", True)))
    return {"faults": request.app.state.faults.faults}


@router.delete("/api/faults")
async def clear_faults(request: Request):
    _need_lab(request)
    request.app.state.faults.clear()
    return {"faults": {}}


# ---- 压测（独立工具：轻量 Produce 循环 + 聚合统计 + Markdown 报告） ----
_load_jobs: dict = {}

async def _load_worker(job_id: str, request: Request, body: dict):
    from ..producer.loadtest import run_load_job
    job = _load_jobs[job_id]
    try:
        await run_load_job(job, body)
    except Exception as e:  # noqa: BLE001
        job.update({"status": "failed", "error": str(e)[:300]})


@router.post("/api/loadtest/run")
async def loadtest_run(body: dict, background: BackgroundTasks, request: Request):
    _need_lab(request)
    import time as _t
    import uuid as _u
    mode = body.get("mode", "count") if body.get("mode") in ("count", "duration") else "count"
    total = max(1, min(1_000_000, int(body.get("total", 100))))
    duration_sec = max(5, min(3600, float(body.get("durationSec", 60))))
    conc = max(1, min(64, int(body.get("concurrency", 4))))
    value_size = max(1, min(65536, int(body.get("valueSize", 128))))
    job_id = f"load-{_u.uuid4().hex[:8]}"
    _load_jobs[job_id] = {
        "id": job_id, "status": "running", "mode": mode,
        "total": total if mode == "count" else None,
        "durationTarget": duration_sec if mode == "duration" else None,
        "concurrency": conc, "valueSize": value_size, "done": 0,
        "topic": body.get("topic", "test-topic"),
        "bootstrap": body.get("bootstrap", ""),
        "startedAt": int(_t.time() * 1000),
    }
    payload = {
        "bootstrap": body.get("bootstrap", ""),
        "topic": body.get("topic", "test-topic"),
        "partition": body.get("partition", "auto"),
        "acks": body.get("acks", "all"),
        "retries": int(body.get("retries", 1)),
        "requestTimeoutMs": int(body.get("requestTimeoutMs", 10000)),
        "deliveryTimeoutMs": int(body.get("deliveryTimeoutMs", 30000)),
        "securityProtocol": body.get("securityProtocol", "PLAINTEXT"),
        "saslMechanism": body.get("saslMechanism", "PLAIN"),
        "saslUsername": body.get("saslUsername", ""),
        "saslPassword": body.get("saslPassword", ""),
        "mode": mode, "total": total, "durationSec": duration_sec,
        "concurrency": conc, "valueSize": value_size,
    }
    background.add_task(_load_worker, job_id, request, payload)
    return {"jobId": job_id, "total": total if mode == "count" else None}


@router.get("/api/loadtest/{job_id}")
async def loadtest_status(job_id: str):
    job = _load_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job


@router.get("/api/loadtest/{job_id}/report")
async def loadtest_report(job_id: str):
    from ..producer.loadtest import build_report
    job = _load_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job.get("status") != "done":
        raise HTTPException(409, "job not finished")
    return {"markdown": build_report(job)}


@router.websocket("/ws/session/{session_id}")
async def ws_session(ws: WebSocket, session_id: str):
    await ws.accept()
    traces = ws.app.state.traces
    q = traces.subscribe(session_id)
    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=30)
            except asyncio.TimeoutError:
                await ws.send_json({"type": "ping", "sessionId": session_id})
                continue
            await ws.send_json(msg.model_dump())
    except WebSocketDisconnect:
        pass
    finally:
        traces.unsubscribe(session_id, q)
