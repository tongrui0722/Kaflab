from __future__ import annotations

import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


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
