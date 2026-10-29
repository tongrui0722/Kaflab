from __future__ import annotations

from fastapi import HTTPException, Request

_auth_tokens: dict[str, str] = {}


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
