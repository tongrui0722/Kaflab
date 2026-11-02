from __future__ import annotations
import asyncio
import base64
import hashlib
import hmac
from collections import Counter
import json
import os
import secrets
import time
from fastapi import APIRouter, HTTPException, Query, Request

from .. import monitoring
from ..cluster.manager import ClusterManager
from ..consumer.engine import ConsumerEngine
from ..models import TopicCreationRequest, error_name
from ..network.client import KafkaClient
from ..models import SecurityConfig
from ..network.tcp import TcpConnector, parse_hostport
from ..protocol import codec
from .permissions import _auth_tokens, _auth_users, auth_mode, require_full, principal, require_cluster

router = APIRouter()

_sec_of = lambda r: SecurityConfig(securityProtocol=r.securityProtocol,
                                   saslMechanism=r.saslMechanism,
                                   saslUsername=r.saslUsername,
                                   saslPassword=r.saslPassword)


# ---- 登录与模式鉴权 ----
# mode=readonly：只读巡检；mode=full：全部功能。token 内存保存，重启后端失效。
_DATA_DIR = os.environ.get("KCL_DATA_DIR") or os.path.dirname(os.path.dirname(__file__))
_USERS_FILE = os.path.join(_DATA_DIR, "users.json")

def _password_hash(password: str) -> str:
    salt = os.urandom(16)
    rounds = 210000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return "pbkdf2_sha256$%d$%s$%s" % (
        rounds, base64.b64encode(salt).decode(), base64.b64encode(digest).decode())


def _password_matches(saved: str, supplied: str) -> bool:
    if not saved.startswith("pbkdf2_sha256$"):
        return hmac.compare_digest(saved, supplied)
    try:
        _, rounds, salt, expected = saved.split("$")
        actual = hashlib.pbkdf2_hmac("sha256", supplied.encode("utf-8"),
                                    base64.b64decode(salt), int(rounds))
        return hmac.compare_digest(actual, base64.b64decode(expected))
    except (ValueError, TypeError):
        return False


def _write_users(users: list[dict]) -> None:
    temporary = _USERS_FILE + ".tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"users": users}, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, _USERS_FILE)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _load_users() -> list[dict]:
    """Docker uses environment credentials; local users are hashed on disk."""
    if os.environ.get("KCL_AUTH_SOURCE") == "environment":
        return [{"username": (os.environ.get("KCL_USERNAME") or "admin").strip() or "admin",
                 "password": os.environ.get("KCL_PASSWORD") or "admin",
                 "role": "admin"}]
    try:
        with open(_USERS_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        initial = [{"username": os.environ.get("KCL_USERNAME") or "admin",
                    "password": _password_hash(os.environ.get("KCL_PASSWORD") or "admin"),
                    "role": "admin"}]
        try:
            fd = os.open(_USERS_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"users": initial}, handle, ensure_ascii=False, indent=2)
            return initial
        except FileExistsError:
            return _load_users()
        except OSError:
            raise HTTPException(503, "用户配置文件写入失败，请检查数据目录权限") from None
    except (OSError, ValueError, TypeError):
        raise HTTPException(503, "用户配置文件读取失败，请检查 users.json") from None
    users = data.get("users") if isinstance(data, dict) else None
    if not isinstance(users, list) or not users:
        raise HTTPException(503, "用户配置文件无有效账号，请检查 users.json")
    valid = [dict(user) for user in users if isinstance(user, dict)
             and user.get("username") and user.get("password")]
    if not valid:
        raise HTTPException(503, "用户配置文件无有效账号，请检查 users.json")
    if any(not str(user["password"]).startswith("pbkdf2_sha256$") for user in valid):
        for user in valid:
            if not str(user["password"]).startswith("pbkdf2_sha256$"):
                user["password"] = _password_hash(str(user["password"]))
        try:
            _write_users(valid)
        except OSError:
            raise HTTPException(503, "用户凭据迁移失败，请检查 users.json 权限") from None
    return valid


@router.post("/api/auth/login")
async def auth_login(body: dict):
    mode = body.get("mode", "readonly")
    if mode not in ("readonly", "full"):
        raise HTTPException(400, "mode must be readonly|full")
    username = (body.get("username", "") or "").strip()
    user = next((u for u in _load_users() if u.get("username") == username
                 and _password_matches(str(u.get("password", "")), body.get("password", ""))), None)
    if not username or user is None:
        raise HTTPException(401, "用户名或密码错误")
    tok = secrets.token_hex(16)
    if user.get("role") == "viewer" and mode == "full":
        raise HTTPException(403, "该账号仅有只读权限")
    _auth_tokens[tok] = mode
    _auth_users[tok] = {"username": username, "role": user.get("role", "admin"),
                        "clusters": user.get("clusters"), "operations": user.get("operations")}
    return {"token": tok, "mode": mode, "username": username}


def _auth_mode(request: Request) -> str | None:
    return auth_mode(request, _auth_tokens)


def _need_lab(request: Request) -> None:
    require_full(_auth_mode(request))


@router.get("/api/auth/me")
async def auth_me(request: Request):
    mode = _auth_mode(request)
    if mode is None:
        raise HTTPException(401, "unauthorized")
    return {"mode": mode}


@router.post("/api/auth/mode")
async def auth_switch_mode(body: dict, request: Request):
    """在已认证会话内切换只读/完整功能范围，与登录时可选模式保持一致。"""
    auth = request.headers.get("authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else ""
    if not token or token not in _auth_tokens:
        raise HTTPException(401, "登录已失效，请重新登录")
    mode = body.get("mode")
    if mode not in ("readonly", "full"):
        raise HTTPException(400, "mode must be readonly|full")
    if mode == "full" and _auth_users.get(token, {}).get("role") == "viewer":
        raise HTTPException(403, "该账号仅有只读权限")
    _auth_tokens[token] = mode
    return {"mode": mode}


@router.post("/api/auth/logout")
async def auth_logout(request: Request):
    auth = request.headers.get("authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else ""
    _auth_tokens.pop(token, None)
    _auth_users.pop(token, None)
    return {"ok": True}


@router.get("/api/health")
async def health():
    return {"ok": True, "service": "kafka-client-lab"}


@router.get("/api/clusters")
async def list_clusters(request: Request):
    if _auth_mode(request) is None:
        raise HTTPException(401, "登录已失效（后端重启或 token 过期），请退出后重新登录")
    token = request.headers.get("authorization", "")[7:]
    allowed = _auth_users.get(token, {}).get("clusters")
    return [ClusterManager.public(c) for c in request.app.state.clusters.list()
            if allowed is None or c.id in allowed]


@router.post("/api/clusters")
async def save_cluster(body: dict, request: Request):
    if (body or {}).get("id"):
        _need_lab(request)
        token = request.headers.get("authorization", "")[7:]
        allowed = _auth_users.get(token, {}).get("clusters")
        if allowed is not None and str(body["id"]) not in allowed:
            raise HTTPException(403, "无权修改该集群")
    elif _auth_mode(request) is None:
        raise HTTPException(401, "登录已失效（后端重启或 token 过期），请退出后重新登录")
    elif _auth_users.get(request.headers.get("authorization", "")[7:], {}).get("clusters") is not None:
        raise HTTPException(403, "受限账号不能新增集群")
    try:
        cluster = request.app.state.clusters.save(body or {})
        if (body or {}).get("monitoringEnabled") is False:
            status = monitoring.get_collector(cluster.id)
            if status["running"]:
                monitoring.set_collector(cluster.id, False, status["last_ok"], status["last_error"])
        return ClusterManager.public(cluster)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.delete("/api/clusters/{cid}")
async def delete_cluster(cid: str, request: Request):
    _need_lab(request)
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
        # Metadata v1 with an empty topic array is not consistently treated as
        # "all topics" by older/compatible brokers. Keep the v0 query as the
        # authoritative all-topics count, then read controller metadata v1.
        all_topics_dec, _, _ = await kc.call(
            3, "Metadata", 0, codec.encode_metadata_v0([]), 3, "metadata",
            extra_req={"topics": "ALL"}, decode=codec.decode_metadata_v0)
        controller_dec, _, _ = await kc.call(
            3, "Metadata", 1, codec.encode_metadata_v0([]), 3, "metadata",
            extra_req={"topics": "ALL", "controllerLookup": True},
            decode=codec.decode_metadata_v1)
        from ..producer.shared import _metadata_dict
        meta = _metadata_dict(all_topics_dec)
        broker_nodes = request.app.state.clusters.observe_brokers(
            cid, controller_dec.brokers, meta["topics"])
        return {"connected": True, "brokers": controller_dec.brokers,
                "brokerCount": len(controller_dec.brokers),
                "brokerNodes": broker_nodes,
                "topicCount": len(meta["topics"]),
                "controllerId": controller_dec.controller_id}
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
        # Metadata v0 supplies the complete topic list; v1 supplies the current controller.
        # Keep the distribution available if the controller lookup fails.
        controller_id = None
        try:
            controller_dec, _, _ = await kc.call(
                3, "Metadata", 1, codec.encode_metadata_v0([]), 3, "metadata",
                extra_req={"topics": "ALL", "controllerLookup": True},
                decode=codec.decode_metadata_v1)
            if any(b.get("id") == controller_dec.controller_id for b in meta["brokers"]):
                controller_id = controller_dec.controller_id
        except Exception:  # noqa: BLE001
            pass
        out = []
        broker_stats = {
            int(b["id"]): {"id": int(b["id"]), "host": b.get("host", ""),
                           "port": int(b.get("port") or 0), "leaders": 0,
                           "replicas": 0, "isr": 0}
            for b in meta["brokers"] if b.get("id") is not None
        }
        for t in meta["topics"]:
            ps = t.get("partitions", [])
            leader_counts = Counter(
                p["leader"] for p in ps
                if p.get("leader") is not None and p["leader"] >= 0
            )
            offline_partitions = 0
            under_replicated_partitions = 0
            for p in ps:
                leader = p.get("leader", -1)
                replicas = p.get("replicas", []) or []
                isr = p.get("isr", []) or []
                if leader is None or leader < 0:
                    offline_partitions += 1
                elif leader in broker_stats:
                    broker_stats[leader]["leaders"] += 1
                if len(isr) < len(replicas):
                    under_replicated_partitions += 1
                for bid in replicas:
                    if bid in broker_stats:
                        broker_stats[bid]["replicas"] += 1
                for bid in isr:
                    if bid in broker_stats:
                        broker_stats[bid]["isr"] += 1
            out.append({
                "topic": t["topic"],
                "partitions": len(ps),
                "replication": max((len(p.get("replicas", [])) for p in ps), default=0),
                "leaders": sorted(leader_counts),
                "leaderDistribution": [
                    {"brokerId": bid, "partitions": count}
                    for bid, count in sorted(leader_counts.items())
                ],
                "offlinePartitions": offline_partitions,
                "underReplicatedPartitions": under_replicated_partitions,
                "hasOffline": offline_partitions > 0,
                "hasUnderReplicated": under_replicated_partitions > 0,
            })
        out.sort(key=lambda x: x["topic"])
        return {"clusterId": cid, "brokers": meta["brokers"], "brokerStats": list(broker_stats.values()),
                "controllerId": controller_id,
                "brokerNodes": request.app.state.clusters.observe_brokers(cid, meta["brokers"], meta["topics"]),
                "topics": out}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"topics failed: {type(e).__name__}: {e}")
    finally:
        kc.close()


# Kafka Metadata still returns a complete snapshot; the API caches one snapshot
# and paginates/filter it server-side for browser-sized responses.
_topic_cache: dict[str, dict] = {}


@router.get("/api/clusters/{cid}/topics/page")
async def cluster_topics_page(cid: str, request: Request, page: int = Query(1, ge=1),
                              size: int = Query(20, ge=1, le=100),
                              q: str = Query(""), status: str = Query("all"),
                              sort: str = Query("topic"), asc: bool = Query(True),
                              refresh: bool = Query(False)):
    if status not in {"all", "healthy", "offline", "under-replicated"}:
        raise HTTPException(400, "invalid status")
    if sort not in {"topic", "partitions", "replication", "health"}:
        raise HTTPException(400, "invalid sort")
    if request.app.state.clusters.get(cid) is None:
        raise HTTPException(404, "cluster not found")
    now = time.time()
    cached = _topic_cache.get(cid)
    cache_hit = bool(cached and now - cached["sampledAt"] < 30 and not refresh)
    if not cache_hit:
        snapshot = await cluster_topics(cid, request)
        cached = {"data": snapshot, "sampledAt": int(now)}
        _topic_cache[cid] = cached
    data = cached["data"]
    all_rows = data["topics"]
    search = q.strip().lower()
    rows = [row for row in all_rows if (not search or search in row["topic"].lower())
            and (status == "all"
                 or status == "healthy" and not row["hasOffline"] and not row["hasUnderReplicated"]
                 or status == "offline" and row["hasOffline"]
                 or status == "under-replicated" and not row["hasOffline"] and row["hasUnderReplicated"])]
    def rank(row):
        return 2 if row["hasOffline"] else 1 if row["hasUnderReplicated"] else 0
    rows.sort(key=lambda row: (row["topic"] if sort == "topic" else
                               row["partitions"] if sort == "partitions" else
                               row["replication"] if sort == "replication" else rank(row),
                               row["topic"]), reverse=not asc)
    start = (page - 1) * size
    return {"clusterId": cid, "topics": rows[start:start + size], "total": len(rows),
            "totalTopics": len(all_rows), "page": page, "size": size,
            "sampledAt": cached["sampledAt"], "cached": cache_hit,
            "brokers": data["brokers"],
            "summary": {"partitions": sum(row["partitions"] for row in all_rows),
                        "offline": sum(bool(row["hasOffline"]) for row in all_rows),
                        "underReplicated": sum(bool(row["hasUnderReplicated"]) for row in all_rows)}}


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


async def _ensure_admin_api(kc: KafkaClient, api_key: int, api_name: str,
                            preferred_max: int | None = None) -> int:
    """ApiVersions 协商：返回当前实现与 Broker 都支持的最高版本。"""
    api_dec, _, _ = await kc.call(18, "ApiVersions", 0, codec.encode_apiversions_v0(), 2,
                                  "apiversions", extra_req={"probe": api_name},
                                  decode=codec.decode_apiversions_v0)
    supported = {a["apiKey"]: (a["minVersion"], a["maxVersion"])
                 for a in api_dec.get("apis", [])}
    if api_key not in supported:
        raise HTTPException(501, f"broker 不支持 {api_name}（API key {api_key}）")
    minimum, maximum = supported[api_key]
    selected = min(maximum, preferred_max) if preferred_max is not None else maximum
    if selected < minimum:
        raise HTTPException(501, f"broker 不支持平台实现的 {api_name} 协议版本")
    return selected


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
            previous_controller_id = broker["id"]
            log.warning(
                "CreateTopics reached a non-controller broker: topic=%s metadataController=%s target=%s:%s; refreshing metadata",
                name, previous_controller_id, broker["host"], broker["port"],
            )
            metadata = await _topic_cluster_metadata(cid, request, "topic-create-controller-refresh")
            broker = _controller_broker(metadata)
            if broker["id"] == previous_controller_id:
                log.warning(
                    "Controller ID remained %s after NOT_CONTROLLER for topic %s; check advertised.listeners and per-broker routing to %s:%s",
                    broker["id"], name, broker["host"], broker["port"],
                )
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

        _topic_cache.pop(cid, None)
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
    """通过 Controller 删除 Topic；按 ApiVersions 协商 v0–v3。"""
    _need_lab(request)
    return await delete_topic_once(cid, topic, request)


async def delete_topic_once(cid: str, topic: str, request: Request):
    import logging
    log = logging.getLogger("kcl.topic")
    try:
        for attempt in (1, 2):
            metadata = await _topic_cluster_metadata(cid, request, f"topic-delete-metadata-{attempt}")
            broker = _controller_broker(metadata)
            _, kc = _cluster_client(cid, request, f"topic-delete-{attempt}", broker)
            try:
                await kc.connect(1, label=f"topic-delete-controller-{attempt}")
                version = await _ensure_admin_api(kc, 20, "DeleteTopics", preferred_max=3)
                dec, _, raw = await kc.call(
                    20, "DeleteTopics", version, codec.encode_delete_topics_v0([topic]),
                    3, "delete-topics",
                    extra_req={"topic": topic, "controllerBrokerId": broker["id"],
                               "attempt": attempt, "version": version},
                    topic=topic, decode=(codec.decode_delete_topics_v0 if version == 0
                                         else codec.decode_delete_topics_v1_to_v3))
            finally:
                kc.close()
            t = next((item for item in dec.get("topics", [])
                      if item.get("topic") == topic), None)
            if t is None or dec.get("_end") != len(raw) - 4:
                raise HTTPException(502, "Kafka 删除结果无法确认，请刷新 Topic 列表核对状态")
            code = t["errorCode"]
            if code != 41 or attempt == 2:
                break
        if code != 0:
            log.warning("Kafka DeleteTopics rejected: topic=%s broker=%s version=%s error=%s (%s)",
                        topic, broker["id"], version, error_name(code), code)
            hints = {
                3: (404, "Topic 不存在或已被删除，请刷新列表"),
                7: (504, "Kafka 删除请求超时，请刷新 Topic 列表确认结果"),
                29: (403, "当前 Kafka 用户没有删除此 Topic 的权限"),
                31: (403, "当前 Kafka 用户没有删除 Topic 的集群权限"),
                41: (503, "Kafka Controller 正在切换，请稍后重试"),
                42: (502, "Kafka 拒绝了删除请求（INVALID_REQUEST），请检查 Broker 日志"),
                73: (409, "Kafka 集群已禁用 Topic 删除（delete.topic.enable=false）；请在 Broker 配置中启用后重试"),
            }
            status, message = hints.get(code, (502, f"Kafka 删除失败：{error_name(code)}（{code}）"))
            raise HTTPException(status, message)
        _topic_cache.pop(cid, None)
        return {"deleted": topic}
    except HTTPException:
        raise
    except (TimeoutError, asyncio.TimeoutError) as e:
        log.exception("Kafka DeleteTopics timed out for %s", topic)
        raise HTTPException(504, "Kafka 删除请求超时，请刷新 Topic 列表确认结果") from e
    except Exception as e:  # noqa: BLE001
        log.exception("Kafka DeleteTopics failed for %s", topic)
        raise HTTPException(502, f"Kafka 删除请求失败：{type(e).__name__}；请刷新 Topic 列表确认结果") from e


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
        41: "创建请求到达了非 Controller Broker。请检查各 Broker 的 advertised.listeners 与外部端口是否一一对应；若集群刚发生 Controller 切换，请稍后重试。",
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


async def _groups_snapshot(cid: str, request: Request, timeout_ms: int = 10000) -> dict:
    """Internal full group snapshot used by overview sampling; public group APIs omit rate counters."""
    from ..models import GroupDescribeRequest as _G
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    engine: ConsumerEngine = request.app.state.consumer
    return await engine.groups_overview(_G(
        bootstrap=c.bootstrap, groupId="probe", clientId="kafka-sim-01",
        requestTimeoutMs=timeout_ms, securityProtocol=c.securityProtocol,
        saslMechanism=c.saslMechanism, saslUsername=c.saslUsername,
        saslPassword=c.saslPassword))


@router.get("/api/clusters/{cid}/overview")
async def cluster_overview(cid: str, request: Request):
    """集群工作空间快照；Lag 趋势由后台采样任务独立落库。"""
    if _auth_mode(request) is None:
        raise HTTPException(401, "unauthorized")
    if not request.app.state.clusters.get(cid):
        raise HTTPException(404, "cluster not found")
    import asyncio as _asyncio
    import time as _t
    from .. import monitoring as _mon
    ts = int(_t.time())
    topic_data, group_data = await _asyncio.gather(
        cluster_topics(cid, request), _groups_snapshot(cid, request), return_exceptions=True)
    snap: dict = {"brokers": [], "topics": [], "groups": [], "errors": []}
    if isinstance(topic_data, Exception):
        snap["errors"].append(f"元数据采集失败：{topic_data}")
    else:
        snap["brokers"] = topic_data.get("brokers", [])
        snap["topics"] = topic_data.get("topics", [])
    if isinstance(group_data, Exception):
        snap["errors"].append(f"消费组采集失败：{group_data}")
    else:
        snap["groups"] = group_data.get("groups", [])
    topics = snap["topics"]
    groups = snap["groups"]
    list_groups_errors = ([] if isinstance(group_data, Exception)
                          else group_data.get("listGroupsErrors", []))
    if list_groups_errors:
        snap["errors"].append(f"{len(list_groups_errors)} 个 Broker 的消费组列表采集失败")
    valid_groups = [g for g in groups if not g.get("error") and isinstance(g.get("totalLag"), (int, float))]
    lag_unknown_groups = len(groups) - len(valid_groups)
    lag_unknown_partitions = sum(int(g.get("uncommitted") or 0) for g in valid_groups)
    lag_state = ("unavailable" if isinstance(group_data, Exception) or (list_groups_errors and not groups) else
                 "complete" if not groups or (lag_unknown_groups == 0 and lag_unknown_partitions == 0
                                              and not list_groups_errors) else
                 "partial")
    total_lag = (sum(float(g["totalLag"]) for g in valid_groups)
                 if lag_state != "unavailable" else None)
    broker_stats = topic_data.get("brokerStats", []) if not isinstance(topic_data, Exception) else []
    broker_nodes = topic_data.get("brokerNodes", []) if not isinstance(topic_data, Exception) else []
    offline_partitions = sum(int(t.get("offlinePartitions") or 0) for t in topics)
    under_replicated_partitions = sum(int(t.get("underReplicatedPartitions") or 0) for t in topics)
    topics_with_issues = sum(1 for t in topics if t.get("hasOffline") or t.get("hasUnderReplicated"))
    summary = {
        "brokerCount": len(snap["brokers"]),
        "topicCount": len(topics),
        "partitionCount": sum(t.get("partitions", 0) for t in topics),
        "offlineTopics": sum(1 for t in topics if t.get("hasOffline")),
        "underReplicatedTopics": sum(1 for t in topics if t.get("hasUnderReplicated")),
        "offlinePartitionCount": offline_partitions,
        "underReplicatedPartitionCount": under_replicated_partitions,
        "topicsWithIssues": topics_with_issues,
        "groupCount": len(groups),
        "memberCount": sum(int(g.get("memberCount") or 0) for g in groups if not g.get("error")),
        "totalLag": total_lag,
        "lagState": lag_state,
        "lagUnknownGroups": lag_unknown_groups,
        "lagUnknownPartitions": lag_unknown_partitions,
    }
    lag_top = sorted(
        ({"groupId": g.get("groupId"), "state": g.get("state"),
          "memberCount": g.get("memberCount", 0), "totalLag": g.get("totalLag", 0),
          "uncommitted": g.get("uncommitted", 0)} for g in valid_groups),
        key=lambda g: g["totalLag"], reverse=True)[:10]
    lag_note = ("部分消费组数据或位点不完整，合计为近似值" if lag_state == "partial"
                else "消费组数据不可用" if lag_state == "unavailable" else "")
    return {"summary": summary, "brokers": snap["brokers"], "brokerStats": broker_stats,
            "controllerId": topic_data.get("controllerId") if not isinstance(topic_data, Exception) else None,
            "brokerNodes": broker_nodes,
            "lagTop": lag_top, "groups": groups, "topics": topics,
            "collector": _mon.get_collector(cid), "collectedAt": ts * 1000,
            "errors": snap["errors"], "lagNote": lag_note}


@router.get("/api/clusters/{cid}/trends")
async def cluster_trends(cid: str, request: Request, metric: str = "group_lag",
                         hours: float = 1):
    """趋势查询：从采样库读取序列点；group_lag 按消费组拆分并合成集群合计。"""
    if _auth_mode(request) is None:
        raise HTTPException(401, "unauthorized")
    if not request.app.state.clusters.get(cid):
        raise HTTPException(404, "cluster not found")
    import time as _t
    from .. import monitoring as _mon
    # The background collector writes group_lag samples even without a login.
    store_metric = metric
    try:
        hours = max(0.05, min(72, float(hours)))
    except (TypeError, ValueError):
        hours = 1
    since_ts = int(_t.time()) - int(hours * 3600)
    rows = _mon.query_points(cid, store_metric, since_ts)
    # Read the old metric name for installations that sampled before the
    # overview recorder was standardized on `group_lag`.
    if metric == "group_lag" and not rows:
        rows = _mon.query_points(cid, "group_lag_now", since_ts)
    points: list[dict] = []
    for r in rows:
        try:
            labels = json.loads(r.get("labels") or "{}")
        except Exception:  # noqa: BLE001
            labels = {}
        series = labels.get("consumergroup") or r.get("series") or "__total__"
        points.append({"series": series, "value": r.get("value"), "ts": r.get("ts")})
    if metric == "group_lag":
        # Legacy page-driven sampling could write the same group more than once
        # in one second. Keep its latest value before deriving the cluster total.
        unique = {(p["ts"], p["series"]): p for p in points}
        points = list(unique.values())
        totals: dict[int, float] = {}
        for p in points:
            if p["series"] == "__total__":
                continue
            totals[p["ts"]] = totals.get(p["ts"], 0) + float(p["value"] or 0)
        points = [p for p in points if p["series"] != "__total__" or p["ts"] not in totals]
        for ts, value in totals.items():
            points.append({"series": "__total__", "value": value, "ts": ts})
    points.sort(key=lambda p: p["ts"])
    return {"points": points, "metric": metric, "hours": hours}


@router.get("/api/monitoring/catalog")
async def monitoring_catalog(request: Request):
    if _auth_mode(request) is None:
        raise HTTPException(401, "unauthorized")
    from .. import monitoring as _mon
    return {"metrics": _mon.CATALOG, "retentionDays": _mon.RETENTION_DAYS}


@router.get("/api/clusters/{cid}/topics/{topic}/config")
async def topic_config_get(cid: str, topic: str, request: Request):
    """读取 Topic 配置（含来源与只读标记）。"""
    if _auth_mode(request) is None:
        raise HTTPException(401, "unauthorized")
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    from ..topic_config import describe_topic_config
    engine: ConsumerEngine = request.app.state.consumer
    sec = SecurityConfig(securityProtocol=c.securityProtocol, saslMechanism=c.saslMechanism,
                         saslUsername=c.saslUsername, saslPassword=c.saslPassword)
    try:
        configs = await describe_topic_config(engine.traces, c.bootstrap, topic, sec)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"config failed: {type(e).__name__}: {e}")
    return {"topic": topic,
            "configs": configs,
            "editable": [x["name"] for x in configs if not x["readOnly"]],
            "readonly": [x["name"] for x in configs if x["readOnly"]],
            "sensitive": [x["name"] for x in configs if x["isSensitive"]]}


@router.put("/api/clusters/{cid}/topics/{topic}/config")
async def topic_config_put(cid: str, topic: str, request: Request):
    """修改 Topic 配置：value 为 null/空串表示删除覆盖（恢复默认）。仅全部功能模式。"""
    _need_lab(request)
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    raw = body.get("configs", {}) or {}
    changes = {k: (None if v is None or v == "" else str(v)) for k, v in raw.items()}
    if not changes:
        raise HTTPException(400, "没有可保存的变更")
    from ..topic_config import alter_topic_config
    engine: ConsumerEngine = request.app.state.consumer
    sec = SecurityConfig(securityProtocol=c.securityProtocol, saslMechanism=c.saslMechanism,
                         saslUsername=c.saslUsername, saslPassword=c.saslPassword)
    try:
        result = await alter_topic_config(engine.traces, c.bootstrap, topic, changes, sec)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"alter failed: {type(e).__name__}: {e}")
    result.update({"topic": topic})
    return result


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
