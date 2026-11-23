"""E2 transport: SCTP (standard, O-RAN.WG3.E2GAP) with an optional TCP fallback.

* **SCTP** (default where the kernel supports it, e.g. Linux): one-to-one
  style sockets, one E2AP PDU per SCTP message, PPID 70 (E2AP). Port 36421.
* **TCP** (non-standard, development only, e.g. on macOS which lacks kernel
  SCTP): each E2AP PDU is prefixed with a 4 byte big-endian length.

Only the python standard library is used.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
from typing import Awaitable, Callable, Optional

log = logging.getLogger("uric.transport")

E2AP_PPID = 70
E2_SCTP_PORT = 36421
IPPROTO_SCTP = getattr(socket, "IPPROTO_SCTP", 132)
SCTP_SNDRCV = 1  # cmsg type for struct sctp_sndrcvinfo (Linux)
_MAX_MSG = 1 << 20


def sctp_available() -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, IPPROTO_SCTP)
    except OSError:
        return False
    s.close()
    return True


class Connection:
    """A bidirectional, message-oriented E2 connection."""

    transport = "?"

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.sock.setblocking(False)
        self.loop = asyncio.get_running_loop()
        try:
            peer = sock.getpeername()
            self.peer = f"{peer[0]}:{peer[1]}"
        except OSError:
            self.peer = "?"
        self.closed = False
        self._send_lock = asyncio.Lock()

    async def recv(self) -> Optional[bytes]:
        raise NotImplementedError

    async def send(self, data: bytes) -> None:
        raise NotImplementedError

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            try:
                # shutdown first: wakes up a reader blocked on this socket and sends EOF to the peer
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass

    async def _wait_readable(self) -> None:
        fut = self.loop.create_future()
        fd = self.sock.fileno()
        self.loop.add_reader(fd, lambda: fut.done() or fut.set_result(None))
        try:
            await fut
        finally:
            self.loop.remove_reader(fd)

    async def _wait_writable(self) -> None:
        fut = self.loop.create_future()
        fd = self.sock.fileno()
        self.loop.add_writer(fd, lambda: fut.done() or fut.set_result(None))
        try:
            await fut
        finally:
            self.loop.remove_writer(fd)


class SCTPConnection(Connection):
    transport = "sctp"

    def __init__(self, sock: socket.socket):
        super().__init__(sock)
        self._cmsg_ok = hasattr(sock, "sendmsg")

    async def recv(self) -> Optional[bytes]:
        buf = b""
        while True:
            try:
                data, _anc, flags, _addr = self.sock.recvmsg(_MAX_MSG)
            except (BlockingIOError, InterruptedError):
                await self._wait_readable()
                continue
            except OSError:
                return None
            if not data:
                return None if not buf else buf
            buf += data
            # MSG_EOR marks the end of an SCTP message (Linux). A short read is
            # also complete, as SCTP never returns partial messages unless the
            # buffer was too small.
            if flags & getattr(socket, "MSG_EOR", 0x80) or len(data) < _MAX_MSG:
                return buf
            # Notifications (MSG_NOTIFICATION) are not enabled, so nothing else to filter

    async def send(self, data: bytes) -> None:
        async with self._send_lock:
            while True:
                try:
                    if self._cmsg_ok:
                        # struct sctp_sndrcvinfo: stream, ssn, flags, ppid, context,
                        # timetolive, tsn, cumtsn, assoc_id. PPID is in network order.
                        info = struct.pack("=HHHIIIIIi", 0, 0, 0, socket.htonl(E2AP_PPID), 0, 0, 0, 0, 0)
                        try:
                            self.sock.sendmsg([data], [(IPPROTO_SCTP, SCTP_SNDRCV, info)])
                        except OSError as exc:
                            if exc.errno in (22, 95, 92):  # EINVAL/EOPNOTSUPP/ENOPROTOOPT
                                self._cmsg_ok = False
                                self.sock.send(data)
                            else:
                                raise
                    else:
                        self.sock.send(data)
                    return
                except (BlockingIOError, InterruptedError):
                    await self._wait_writable()


class TCPConnection(Connection):
    transport = "tcp"

    def __init__(self, sock: socket.socket):
        super().__init__(sock)
        self._buf = bytearray()
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

    async def _read_exact(self, n: int) -> Optional[bytes]:
        while len(self._buf) < n:
            try:
                chunk = await self.loop.sock_recv(self.sock, 65536)
            except OSError:
                return None
            if not chunk:
                return None
            self._buf += chunk
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    async def recv(self) -> Optional[bytes]:
        hdr = await self._read_exact(4)
        if hdr is None:
            return None
        (n,) = struct.unpack("!I", hdr)
        if n > _MAX_MSG * 16:
            log.error("%s: oversized frame (%d bytes), closing", self.peer, n)
            return None
        return await self._read_exact(n)

    async def send(self, data: bytes) -> None:
        async with self._send_lock:
            await self.loop.sock_sendall(self.sock, struct.pack("!I", len(data)) + data)


Handler = Callable[[Connection], Awaitable[None]]


class E2Listener:
    """Accepts E2 connections and hands each one to ``handler``."""

    def __init__(self, host: str, port: int, transport: str, handler: Handler):
        self.host, self.port, self.transport = host, port, transport
        self.handler = handler
        self.sock: Optional[socket.socket] = None
        self._task: Optional[asyncio.Task] = None
        self._conns: set[asyncio.Task] = set()

    async def start(self) -> None:
        family = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        proto = IPPROTO_SCTP if self.transport == "sctp" else socket.IPPROTO_TCP
        sock = socket.socket(family, socket.SOCK_STREAM, proto)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(64)
        sock.setblocking(False)
        self.sock = sock
        self.port = sock.getsockname()[1]
        self._task = asyncio.create_task(self._accept_loop(), name=f"e2-accept-{self.transport}")
        log.info("E2 termination listening on %s %s:%d", self.transport.upper(), self.host, self.port)

    async def _accept_loop(self) -> None:
        loop = asyncio.get_running_loop()
        assert self.sock is not None
        while True:
            try:
                client, _ = await loop.sock_accept(self.sock)
            except asyncio.CancelledError:
                raise
            except OSError as exc:
                log.warning("accept failed: %s", exc)
                await asyncio.sleep(0.1)
                continue
            conn = SCTPConnection(client) if self.transport == "sctp" else TCPConnection(client)
            t = asyncio.create_task(self._run(conn))
            self._conns.add(t)
            t.add_done_callback(self._conns.discard)

    async def _run(self, conn: Connection) -> None:
        try:
            await self.handler(conn)
        except Exception:
            log.exception("E2 connection handler failed for %s", conn.peer)
        finally:
            conn.close()

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        for t in list(self._conns):
            t.cancel()
        if self.sock:
            self.sock.close()


async def connect(host: str, port: int, transport: str = "auto") -> Connection:
    """Open a client E2 connection (used by the E2 node simulator)."""
    if transport == "auto":
        transport = "sctp" if sctp_available() else "tcp"
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    family, _, _, _, addr = infos[0]
    proto = IPPROTO_SCTP if transport == "sctp" else socket.IPPROTO_TCP
    sock = socket.socket(family, socket.SOCK_STREAM, proto)
    sock.setblocking(False)
    try:
        await loop.sock_connect(sock, addr)
    except Exception:
        sock.close()
        raise
    return SCTPConnection(sock) if transport == "sctp" else TCPConnection(sock)
