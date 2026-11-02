from __future__ import annotations

from fastapi import HTTPException, Request

_auth_tokens: dict[str, str] = {}
_auth_users: dict[str, dict] = {}


def auth_mode(request: Request, tokens: dict[str, str]) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return tokens.get(auth[7:])
    return None


def _auth_mode(request: Request) -> str | None:
    return auth_mode(request, _auth_tokens)


def require_full(mode: str | None) -> None:
    if mode is None:
        raise HTTPException(401, "登录已失效（后端重启或 token 过期），请退出后重新登录")
    if mode != "full":
        raise HTTPException(403, "只读模式：该操作需要【全部功能】模式登录")


def _need_lab(request: Request) -> None:
    require_full(_auth_mode(request))


def principal(request: Request) -> dict:
    mode = _auth_mode(request)
    if mode is None:
        raise HTTPException(401, "登录已失效，请重新登录")
    token = request.headers.get("authorization", "")[7:]
    return {"username": _auth_users.get(token, {}).get("username", "admin"),
            "role": _auth_users.get(token, {}).get("role", "admin"),
            "clusters": _auth_users.get(token, {}).get("clusters"),
            "operations": _auth_users.get(token, {}).get("operations"),
            "mode": mode}


def require_cluster(request: Request, cluster_id: str) -> None:
    user = principal(request)
    allowed = user["clusters"]
    if allowed is not None and cluster_id not in allowed:
        raise HTTPException(403, "无权访问该集群")


def operation_scope(method: str, path: str) -> str | None:
    parts = path.strip("/").split("/")
    if method not in {"POST", "PUT", "PATCH", "DELETE"} or len(parts) < 2:
        return None
    if parts[1] == "auth":
        return None
    if parts[1] == "clusters":
        if len(parts) == 2 and method == "POST":
            return "cluster:write"
        if len(parts) >= 4 and parts[3] == "test":
            return None
        if len(parts) >= 4 and parts[3] == "topics":
            if len(parts) == 4 and method == "POST":
                return "topic:create"
            if len(parts) >= 6 and parts[5] == "delete-task":
                return "topic:delete"
            if len(parts) == 5 and method == "DELETE":
                return "topic:delete"
            if "reassignment" in parts:
                return "topic:reassign"
            if "partitions" in parts:
                return "topic:expand"
        if len(parts) >= 4 and parts[3] == "groups" and "offset-reset" in parts:
            return "group:offset_reset" if parts[-1] == "execute" else None
        if len(parts) >= 4 and parts[3] == "monitoring":
            return "alert:manage" if method != "GET" else None
        if len(parts) == 3 and method == "DELETE":
            return "cluster:delete"
    if parts[1] in {"groups"} and parts[-1] == "describe":
        return None
    if parts[1] == "client-sim" and parts[-1] in {"list"}:
        return None
    return "api:write"


def require_operation(user: dict, operation: str | None) -> None:
    allowed = user.get("operations")
    if operation and allowed is not None and operation not in allowed and "*" not in allowed:
        raise HTTPException(403, "该账号无权执行此操作")
