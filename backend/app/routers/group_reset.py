"""Guarded offset reset for inactive consumer groups."""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..models import GroupDescribeRequest, error_name
from ..protocol import codec
from .api import _cluster_client, _topic_cluster_metadata
from .permissions import _need_lab, principal

router = APIRouter()
_previews: dict[str, dict] = {}


class PreviewBody(BaseModel):
    topic: str = Field(min_length=1, max_length=249)
    target: str = Field(pattern="^(earliest|latest)$")


class ExecuteBody(BaseModel):
    previewToken: str
    confirmGroupId: str


def _request_for(request: Request, cid: str, group: str) -> GroupDescribeRequest:
    c = request.app.state.clusters.get(cid)
    if not c:
        raise HTTPException(404, "cluster not found")
    return GroupDescribeRequest(bootstrap=c.bootstrap, groupId=group,
        securityProtocol=c.securityProtocol, saslMechanism=c.saslMechanism,
        saslUsername=c.saslUsername, saslPassword=c.saslPassword)


async def _inactive(request: Request, req: GroupDescribeRequest) -> None:
    detail = await request.app.state.consumer.describe_group(req)
    group = detail.get("group") or {}
    if group.get("errorCode") not in (0, None):
        raise HTTPException(409, "Consumer group state could not be confirmed")
    if group.get("members") or group.get("state") not in {"Empty", "Dead"}:
        raise HTTPException(409, "Stop all group consumers before resetting offsets")


async def _earliest(request: Request, cid: str, topic: str, partition: int, leader: int) -> int:
    metadata = await _topic_cluster_metadata(cid, request, "offset-reset-metadata")
    broker = next((b for b in metadata.brokers if b["id"] == leader), None)
    if not broker:
        raise HTTPException(409, "Partition leader is unavailable")
    _, client = _cluster_client(cid, request, "offset-reset-earliest", broker)
    try:
        await client.connect(1, label="offset-reset-earliest")
        decoded, _, _ = await client.call(2, "ListOffsets", 1,
            codec.encode_listoffsets_v1(topic, partition, -2), 3, "listoffsets",
            extra_req={"target": "earliest"}, topic=topic, partition=partition,
            decode=codec.decode_listoffsets_v1)
        result = decoded["topics"][0]["partitions"][0]
        if result["errorCode"] != 0:
            raise HTTPException(502, "ListOffsets: " + error_name(result["errorCode"]))
        return int(result["offset"])
    finally:
        client.close()


@router.post("/api/clusters/{cid}/groups/{group}/offset-reset/preview")
async def offset_reset_preview(cid: str, group: str, body: PreviewBody, request: Request):
    _need_lab(request)
    req = _request_for(request, cid, group)
    await _inactive(request, req)
    detail = await request.app.state.consumer.group_lag(req, [body.topic])
    rows = [row for row in detail["rows"] if row["topic"] == body.topic]
    if not rows or len(rows) > 1000:
        raise HTTPException(400, "Topic has no partitions or exceeds the 1000 partition limit")
    preview_rows = []
    for row in rows:
        if row["end"] is None or row["leader"] is None or row["leader"] < 0:
            raise HTTPException(409, "A partition has no readable leader or end offset")
        target = (row["end"] if body.target == "latest"
                  else await _earliest(request, cid, body.topic, row["partition"], row["leader"]))
        preview_rows.append({"partition": row["partition"], "current": row["committed"],
                             "target": target, "end": row["end"]})
    now = int(time.time())
    token = uuid.uuid4().hex
    _previews[token] = {"cluster": cid, "group": group, "topic": body.topic,
                        "target": body.target, "actor": principal(request)["username"],
                        "rows": preview_rows, "expires": now + 120}
    for key, item in list(_previews.items()):
        if item["expires"] < now:
            _previews.pop(key, None)
    return {"previewToken": token, "groupId": group, "topic": body.topic,
            "target": body.target, "rows": preview_rows, "expiresAt": now + 120}


def _audit(item: dict, actor: str, results: list[dict]):
    data_dir = Path(os.environ.get("KCL_DATA_DIR") or str(Path(__file__).resolve().parents[2]))
    path = data_dir / "group_offset_audit.jsonl"
    event = {"at": int(time.time()), "actor": actor, "cluster": item["cluster"],
             "group": item["group"], "topic": item["topic"], "target": item["target"],
             "results": results}
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


@router.post("/api/clusters/{cid}/groups/{group}/offset-reset/execute")
async def offset_reset_execute(cid: str, group: str, body: ExecuteBody, request: Request):
    _need_lab(request)
    actor = principal(request)["username"]
    item = _previews.get(body.previewToken)
    if not item or item["expires"] < time.time() or item["cluster"] != cid or item["group"] != group:
        raise HTTPException(409, "Preview expired; generate a new preview")
    if item["actor"] != actor or body.confirmGroupId != group:
        raise HTTPException(403, "Confirmation does not match group ID")
    _previews.pop(body.previewToken, None)
    req = _request_for(request, cid, group)
    await _inactive(request, req)
    current = await request.app.state.consumer.group_lag(req, [item["topic"]])
    current_map = {row["partition"]: row for row in current["rows"] if row["topic"] == item["topic"]}
    if len(current_map) != len(item["rows"]):
        raise HTTPException(409, "Topic partition count changed; generate a new preview")
    for row in item["rows"]:
        now_row = current_map.get(row["partition"])
        if not now_row or now_row["committed"] != row["current"] or (
                item["target"] == "latest" and now_row["end"] != row["end"]):
            raise HTTPException(409, "Offsets changed; generate a new preview")
    _, bootstrap = _cluster_client(cid, request, "offset-reset-bootstrap")
    try:
        await bootstrap.connect(1, label="offset-reset-bootstrap")
        found, _, _ = await bootstrap.call(10, "FindCoordinator", 0,
            codec.encode_find_coordinator_v0(group), 2, "findcoordinator",
            extra_req={"groupId": group}, decode=codec.decode_find_coordinator_v0)
        if found.get("errorCode") not in (0, None):
            raise HTTPException(502, "FindCoordinator: " + error_name(found["errorCode"]))
    finally:
        bootstrap.close()
    broker = {"id": found.get("nodeId"), "host": found["host"], "port": found["port"]}
    _, client = _cluster_client(cid, request, "offset-reset-commit", broker)
    results = []
    try:
        await client.connect(1, label="offset-reset-commit")
        for row in item["rows"]:
            decoded, _, _ = await client.call(8, "OffsetCommit", 2,
                codec.encode_offset_commit_v2(group, -1, "", item["topic"],
                                              row["partition"], row["target"]),
                3, "offsetcommit", extra_req={"groupId": group, "offset": row["target"]},
                topic=item["topic"], partition=row["partition"],
                decode=codec.decode_offset_commit_v2)
            response = decoded["topics"][0]["partitions"][0]
            code = response["errorCode"]
            results.append({**row, "errorCode": code, "errorName": error_name(code)})
            if code != 0:
                break
    except Exception as exc:
        results.append({"error": type(exc).__name__ + ": " + str(exc)[:200]})
    finally:
        client.close()
    complete = len(results) == len(item["rows"]) and all(r.get("errorCode") == 0 for r in results)
    if complete:
        try:
            verified = await request.app.state.consumer.group_lag(req, [item["topic"]])
            actual = {row["partition"]: row["committed"] for row in verified["rows"]
                      if row["topic"] == item["topic"]}
            for result in results:
                result["verified"] = actual.get(result["partition"]) == result["target"]
            complete = all(result["verified"] for result in results)
        except Exception as exc:
            complete = False
            results.append({"error": "Verification failed: " + str(exc)[:200]})
    _audit(item, actor, results)
    return {"groupId": group, "topic": item["topic"],
            "status": "done" if complete else "partial", "results": results}
