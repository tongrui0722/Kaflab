from __future__ import annotations
import socket
import ssl
import struct
import time
from dataclasses import dataclass


@dataclass
class Connection:
    sock: socket.socket
    host: str
    port: int


def parse_hostport(addr: str, default_port: int = 9092) -> tuple[str, int]:
    addr = addr.strip()
    if ":" in addr:
        h, p = addr.rsplit(":", 1)
        try:
            return h.strip() or "localhost", int(p)
        except ValueError:
            return h.strip() or "localhost", default_port
    return addr or "localhost", default_port


class TcpConnector:
    def __init__(self, timeout_ms: int = 3000, use_tls: bool = False, ca_path: str = ""):
        self.timeout = max(timeout_ms, 500) / 1000.0
        self.use_tls = use_tls
        self.ca_path = ca_path

    def connect(self, host: str, port: int) -> Connection:
        start = time.time()
        s = socket.create_connection((host, port), timeout=self.timeout)
        try:
            s.settimeout(self.timeout)
            if self.use_tls:
                context = ssl.create_default_context(cafile=self.ca_path or None)
                s = context.wrap_socket(s, server_hostname=host)
            return Connection(s, host, port)
        except Exception:
            s.close()
            raise

    @staticmethod
    def request(conn: Connection, payload_without_len: bytes) -> bytes:
        """Send one length-prefixed frame, read one response frame."""
        frame = struct.pack(">i", len(payload_without_len)) + payload_without_len
        conn.sock.sendall(frame)
        hdr = _recvall(conn.sock, 4)
        (ln,) = struct.unpack(">i", hdr)
        if ln < 0 or ln > 100 * 1024 * 1024:
            raise ValueError(f"invalid response length {ln}")
        return _recvall(conn.sock, ln)

    @staticmethod
    def close(conn: Connection | None) -> None:
        try:
            if conn is not None:
                conn.sock.close()
        except OSError:
            pass


def _recvall(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("broker closed connection")
        buf += chunk
    return bytes(buf)
