from __future__ import annotations

import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from .permissions import _auth_tokens, _auth_users

router = APIRouter()


@router.websocket("/ws/session/{session_id}")
async def ws_session(ws: WebSocket, session_id: str):
    protocols = ws.scope.get("subprotocols", [])
    token = next((value[6:] for value in protocols if value.startswith("token.")), "")
    if ("kcl" not in protocols or token not in _auth_tokens
            or _auth_users.get(token, {}).get("clusters") is not None):
        await ws.close(code=1008)
        return
    await ws.accept(subprotocol="kcl")
    traces = ws.app.state.traces
    q = traces.subscribe(session_id)
    try:
        while True:
            if token not in _auth_tokens:
                await ws.close(code=1008)
                break
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
