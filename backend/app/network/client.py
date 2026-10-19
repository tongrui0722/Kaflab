from __future__ import annotations
import asyncio
import hashlib
import struct
import time
from dataclasses import dataclass

from ..models import KafkaTraceEvent, SecurityConfig
from ..network.tcp import Connection, TcpConnector
from ..protocol import codec
from ..protocol import sasl
from ..trace.manager import TraceManager


@dataclass
class LiveConn:
    conn: Connection
    corr: int = 0
    sasl_done: bool = False


class KafkaClient:
    """One authenticated TCP session against a single broker address.

    Owns: TCP connect + SASL (PLAIN/SCRAM via SaslHandshake v1 + SaslAuthenticate v1)
    + framed request/response with per-event trace emission and real HEX capture.
    Reused across ApiVersions/Metadata/Produce/Fetch/Groups; leader connections
    open their own instances and re-authenticate.
    """

    def __init__(self, traces: TraceManager, session_id: str, trace_id: str,
                 client_id: str, timeout_ms: int, host: str, port: int,
                 security: SecurityConfig, broker_id=None, tls_ca_path: str = ""):
        self.traces = traces
        self.session_id = session_id
        self.trace_id = trace_id
        self.client_id = client_id
        self.timeout_ms = timeout_ms
        self.host = host
        self.port = port
        self.security = security
        self.tls_ca_path = tls_ca_path
        self.broker_id = broker_id
        self.conn: Connection | None = None
        self.corr = 0
        self.max_corr = 0

    # -- emit --

    def _emit(self, **kw) -> KafkaTraceEvent:
        ev = KafkaTraceEvent(traceId=self.trace_id, **kw)
        self.traces.append(self.session_id, ev)
        return ev

    def _broker(self, extra_id=None):
        bid = extra_id if extra_id is not None else self.broker_id
        ref: dict = {"host": self.host, "port": self.port}
        if bid is not None:
            ref["id"] = bid
        return ref

    # -- connect / sasl --

    async def connect(self, step_index: int, label: str = "") -> None:
        t = time.time()
        self.conn = await asyncio.to_thread(
            TcpConnector(self.timeout_ms, self.security.securityProtocol in ("SSL", "SASL_SSL"), self.tls_ca_path).connect, self.host, self.port)
        self._emit(step="tcp.connect", stepIndex=step_index, status="success",
                   direction="local", apiName="TCP", broker=self._broker(),
                   latencyMs=int((time.time() - t) * 1000),
                   request={"target": f"{self.host}:{self.port}", "label": label},
                   response={"status": "CONNECTED"})
        if self.security.securityProtocol.startswith("SASL_"):
            await self._sasl_auth(step_index)

    async def _sasl_auth(self, step_index: int) -> None:
        assert self.conn is not None
        mech = self.security.saslMechanism
        user = self.security.saslUsername
        pwd = self.security.saslPassword
        if not user or not pwd:
            raise RuntimeError(f"SASL {mech} requires username + password")
        # 1. SaslHandshake v1
        self.corr += 1
        body = sasl.encode_sasl_handshake_v1(mech)
        frame = codec.build_request(sasl.SASL_HANDSHAKE_KEY, 1, self.corr,
                                    self.client_id, body)
        self._emit(step="sasl.handshake", stepIndex=step_index, status="pending",
                   direction="request", apiKey=17, apiName="SaslHandshake",
                   apiVersion=1, correlationId=self.corr, broker=self._broker(),
                   request={"mechanism": mech}, rawRequest=codec.hexdump(frame))
        t = time.time()
        raw = await asyncio.to_thread(TcpConnector.request, self.conn, frame[4:])
        lat = int((time.time() - t) * 1000)
        (rcorr,) = struct.unpack_from(">i", raw, 0)
        dec = sasl.decode_sasl_handshake_v1(raw[4:])
        self._emit(step="sasl.handshake", stepIndex=step_index,
                   status="success" if dec["errorCode"] == 0 else "error",
                   direction="response", apiKey=17, apiName="SaslHandshake",
                   apiVersion=1, correlationId=rcorr, broker=self._broker(),
                   latencyMs=lat, errorCode=dec["errorCode"],
                   response={**dec, "mechanism": mech}, rawResponse=codec.hexdump(raw))
        if dec["errorCode"] != 0:
            raise RuntimeError(f"SaslHandshake failed errorCode={dec['errorCode']} "
                               f"(broker supports {dec['mechanisms']})")
        if mech == "PLAIN":
            await self._sasl_plain(step_index)
        elif mech.startswith("SCRAM"):
            await self._sasl_scram(step_index)
        else:
            raise RuntimeError(f"unsupported SASL mechanism {mech}")

    async def _sasl_send_auth(self, payload: bytes, step_index: int,
                              label: str) -> dict:
        assert self.conn is not None
        self.corr += 1
        frame = codec.build_request(sasl.SASL_AUTH_KEY, 1, self.corr,
                                    self.client_id, sasl.encode_sasl_auth_v1(payload))
        self._emit(step="sasl.authenticate", stepIndex=step_index, status="pending",
                   direction="request", apiKey=36, apiName="SaslAuthenticate",
                   apiVersion=1, correlationId=self.corr, broker=self._broker(),
                   request={"phase": label, "bytes": len(payload)})
        t = time.time()
        raw = await asyncio.to_thread(TcpConnector.request, self.conn, frame[4:])
        lat = int((time.time() - t) * 1000)
        (rcorr,) = struct.unpack_from(">i", raw, 0)
        dec = sasl.decode_sasl_auth_v1(raw[4:])
        ok = dec["errorCode"] == 0
        self._emit(step="sasl.authenticate", stepIndex=step_index,
                   status="success" if ok else "error", direction="response",
                   apiKey=36, apiName="SaslAuthenticate", apiVersion=1,
                   correlationId=rcorr, broker=self._broker(), latencyMs=lat,
                   errorCode=dec["errorCode"],
                   response={"phase": label, "errorCode": dec["errorCode"],
                             "messageLen": len(dec["message"] or b"")})
        if not ok:
            detail = dec.get("errorMessage") or ""
            try:
                detail = detail or (dec["message"] or b"").decode()
            except Exception:
                pass
            raise RuntimeError(f"SASL authentication failed errorCode={dec['errorCode']} {detail}")
        return dec

    async def _sasl_plain(self, step_index: int) -> None:
        payload = sasl.plain_initial("", self.security.saslUsername,
                                     self.security.saslPassword)
        await self._sasl_send_auth(payload, step_index, "PLAIN")

    async def _sasl_scram(self, step_index: int) -> None:
        mech = self.security.saslMechanism
        digest = hashlib.sha256 if "256" in mech else hashlib.sha512
        bare, first, _ = sasl.scram_client_first(self.security.saslUsername)
        r1 = await self._sasl_send_auth(first.encode(), step_index, "client-first")
        server_first = (r1["message"] or b"").decode()
        if err := sasl.parse_scram_error((r1["message"] or b"")):
            raise RuntimeError(f"SCRAM server error {err}")
        final, server_sig = sasl.scram_client_final(
            self.security.saslPassword, bare, server_first, digest)
        r2 = await self._sasl_send_auth(final, step_index, "client-final")
        if err := sasl.parse_scram_error((r2["message"] or b"")):
            raise RuntimeError(f"SCRAM server error {err}")
        if not sasl.verify_server_final(r2["message"] or b"", server_sig):
            raise RuntimeError("SCRAM server signature mismatch")

    # -- generic call --

    async def call(self, api_key: int, api_name: str, api_version: int,
                   body: bytes, step_index: int, step: str,
                   extra_req: dict | None = None,
                   topic: str | None = None, partition: int | None = None,
                   decode=None, extra_res: dict | None = None,
                   resp_index: int | None = None, flexible: bool = False,
                   capture_raw: bool = True) -> tuple[dict, bytes, bytes]:
        """Send one request; large repeated batches may omit HEX after the first chunk."""
        assert self.conn is not None
        self.corr += 1
        self.max_corr = max(self.max_corr, self.corr)
        frame = codec.build_request(api_key, api_version, self.corr,
                                    self.client_id, body, flexible=flexible)
        req_obj = {"clientId": self.client_id, **(extra_req or {})}
        self._emit(step=step, stepIndex=step_index, status="pending",
                   direction="request", apiKey=api_key, apiName=api_name,
                   apiVersion=api_version, correlationId=self.corr,
                   broker=self._broker(), topic=topic, partition=partition,
                   request=req_obj, rawRequest=codec.hexdump(frame) if capture_raw else None)
        t = time.time()
        raw = await asyncio.to_thread(TcpConnector.request, self.conn, frame[4:])
        lat = int((time.time() - t) * 1000)
        (rcorr,) = struct.unpack_from(">i", raw, 0)
        try:
            dec = decode(raw[4:]) if decode else {}
        except Exception as e:  # noqa: BLE001
            self._emit(step=step, stepIndex=resp_index if resp_index is not None else step_index, status="error",
                       direction="response", apiKey=api_key, apiName=api_name,
                       apiVersion=api_version, correlationId=rcorr,
                       broker=self._broker(), topic=topic, partition=partition,
                       latencyMs=lat, errorCode=-999, errorName="DECODE_ERROR",
                       response={"decodeError": str(e)}, rawResponse=codec.hexdump(raw) if capture_raw else None)
            raise RuntimeError(f"{api_name} response decode failed: {e}")
        err = dec.get("errorCode") if isinstance(dec, dict) else None
        self._emit(step=step, stepIndex=resp_index if resp_index is not None else step_index,
                   status="success" if err in (None, 0) else "success",
                   direction="response", apiKey=api_key, apiName=api_name,
                   apiVersion=api_version, correlationId=rcorr,
                   broker=self._broker(), topic=topic, partition=partition,
                   latencyMs=lat, errorCode=err, response={**(dec if isinstance(dec, dict) else {}),
                                                           **(extra_res or {})},
                   rawResponse=codec.hexdump(raw) if capture_raw else None)
        return dec, frame, raw

    def close(self) -> None:
        TcpConnector.close(self.conn)
        self.conn = None
