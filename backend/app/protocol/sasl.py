from __future__ import annotations
import base64
import hashlib
import hmac
import os
import struct
from typing import Optional

from .codec import (
    build_request, dec_bytes, dec_string, enc_bytes, enc_string,
    dec_array_count, enc_array, hexdump,
)


SASL_HANDSHAKE_KEY = 17
SASL_AUTH_KEY = 36


def encode_sasl_handshake_v1(mechanism: str) -> bytes:
    return enc_string(mechanism)


def decode_sasl_handshake_v1(buf: bytes) -> dict:
    off = 0
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    n, off = dec_array_count(buf, off)
    mechs = []
    for _ in range(max(n, 0)):
        m, off = dec_string(buf, off)
        mechs.append(m)
    return {"errorCode": err, "mechanisms": mechs}


def encode_sasl_auth_v1(payload: bytes) -> bytes:
    return enc_bytes(payload)


def decode_sasl_auth_v1(buf: bytes) -> dict:
    off = 0
    (err,) = struct.unpack_from(">h", buf, off)
    off += 2
    error_message, off = dec_string(buf, off)
    msg, off = dec_bytes(buf, off)
    (sess,) = struct.unpack_from(">q", buf, off) if off + 8 <= len(buf) else (-1,)
    return {"errorCode": err, "errorMessage": error_message,
            "message": msg, "sessionId": sess}


# ---------- PLAIN ----------

def plain_initial(authzid: str, username: str, password: str) -> bytes:
    return authzid.encode() + b"\x00" + username.encode() + b"\x00" + password.encode()


# ---------- SCRAM ----------

def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def scram_client_first(username: str) -> tuple[str, str, bytes]:
    nonce = _b64(os.urandom(18))
    # gs2 header "n,," ; username escaped per RFC 5802
    esc = username.replace("=", "=3D").replace(",", "=2C")
    bare = f"n={esc},r={nonce}"
    return bare, f"n,,{bare}", nonce.encode()


def _hi(password: str, salt: bytes, iters: int, digest) -> bytes:
    pw = password.encode("utf-8") if isinstance(password, str) else password
    u = hmac.new(pw, salt + b"\x00\x00\x00\x01", digest).digest()
    res = bytearray(u)
    for _ in range(iters - 1):
        u = hmac.new(pw, u, digest).digest()
        for i in range(len(res)):
            res[i] ^= u[i]
    return bytes(res)


def scram_client_final(password: str, client_first_bare: str, server_first: str,
                       digest, hash_len: int = 32) -> tuple[bytes, bytes]:
    # server-first: r=...,s=...,i=...
    kv: dict[str, str] = {}
    for part in server_first.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k] = v
    salt = base64.b64decode(kv["s"])
    iters = int(kv["i"])
    combined_nonce = kv["r"]
    salted = _hi(password, salt, iters, digest)
    client_key = hmac.new(salted, b"Client Key", digest).digest()
    stored_key = hashlib.new(digest().name, client_key).digest()
    cbind = _b64(b"n,,")
    final_wo_proof = f"c={cbind},r={combined_nonce}"
    auth_msg = f"{client_first_bare},{server_first},{final_wo_proof}"
    sig = hmac.new(stored_key, auth_msg.encode(), digest).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, sig))
    server_key = hmac.new(salted, b"Server Key", digest).digest()
    server_sig = hmac.new(server_key, auth_msg.encode(), digest).digest()
    msg = f"{final_wo_proof},p={_b64(proof)}".encode()
    return msg, server_sig


def verify_server_final(server_final: bytes, expected_sig: bytes) -> bool:
    # server-final: v=<sig>
    try:
        s = server_final.decode()
        assert s.startswith("v=")
        got = base64.b64decode(s[2:])
        return hmac.compare_digest(got, expected_sig)
    except Exception:
        return False


def parse_scram_error(payload: bytes) -> Optional[str]:
    try:
        s = payload.decode()
        if s.startswith("e="):
            return s
    except Exception:
        pass
    return None
