import asyncio
import struct
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.protocol import codec
from app.routers import api


def test_delete_uses_supported_v3_and_explains_disabled_cluster(monkeypatch):
    calls = []

    class Client:
        async def connect(self, *_args, **_kwargs):
            pass

        async def call(self, key, _name, version, _body, *_args, **kwargs):
            calls.append((key, version))
            if key == 18:
                return {"apis": [{"apiKey": 20, "minVersion": 0, "maxVersion": 5}]}, b"", b""
            body = struct.pack(">ii", 0, 1) + codec.enc_string("sample") + struct.pack(">h", 73)
            raw = struct.pack(">i", 1) + body
            return kwargs["decode"](body), b"", raw

        def close(self):
            pass

    broker = {"id": 10, "host": "broker", "port": 9092}
    monkeypatch.setattr(api, "_need_lab", lambda _request: None)
    monkeypatch.setattr(api, "_topic_cluster_metadata", lambda *_args: asyncio.sleep(
        0, result=SimpleNamespace(controller_id=10, brokers=[broker])))
    monkeypatch.setattr(api, "_cluster_client", lambda *_args: (None, Client()))

    with pytest.raises(HTTPException) as error:
        asyncio.run(api.cluster_topic_delete("cluster", "sample", object()))

    assert error.value.status_code == 409
    assert "delete.topic.enable=false" in error.value.detail
    assert calls == [(18, 0), (20, 3)]
