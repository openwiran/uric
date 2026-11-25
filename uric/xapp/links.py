"""Transports between an :class:`~uric.xapp.xappbase.xAppBase` xApp and the RIC.

* :class:`LocalLink` – the xApp runs inside the RIC process (loaded by the
  xApp manager); calls are bridged from xApp threads to the RIC event loop.
* :class:`RemoteLink` – the xApp is a separate process connected to the RIC's
  northbound xApp interface (:mod:`uric.core.northbound`).

Both expose the same blocking API and deliver indications to ``on_indication``
as dicts ``{sub, node, ran_function_id, action_id, sn, type, header, message,
call_process_id, sm}`` (bytes for binary fields).
"""

from __future__ import annotations

import asyncio
import itertools
import json
import os
import queue
import socket
import struct
import threading
from typing import Any, Callable, Optional

from ..codec.asn import to_jsonable
from ..codec.e2sm import decode_ueid
from ..core.model import Action, Indication
from ..core.ric import RICError

DEFAULT_RIC_ADDRESS = "127.0.0.1:4560"
TIMEOUT = 15.0


class Link:
    on_indication: Optional[Callable[[dict], None]] = None
    embedded = False

    def nodes(self) -> list[dict]: ...
    def ues(self, node: Optional[str] = None) -> list[dict]: ...
    def subscribe(self, node: str, ran_function_id: int, event_trigger: bytes, actions: list[dict],
                  description: Optional[dict] = None) -> dict: ...
    def unsubscribe(self, sub: str) -> None: ...
    def control(self, node: str, ran_function_id: int, header: bytes, message: bytes, ack: bool = True,
                call_process_id: Optional[bytes] = None) -> dict: ...
    def status(self, status: dict) -> None: ...
    def close(self) -> None: ...

    def _deliver(self, d: dict) -> None:
        if self.on_indication is not None:
            self.on_indication(d)


class LocalLink(Link):
    embedded = True

    def __init__(self, session, loop: asyncio.AbstractEventLoop):
        self.ctx = session
        self.loop = loop

    def _run(self, coro, timeout: float = TIMEOUT):
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self.loop:
            coro.close()
            raise RICError("blocking xApp calls must not be made from the RIC event loop thread")
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def _sync(self, fn):
        async def call():
            return fn()
        return self._run(call())

    def nodes(self):
        ric = self.ctx.ric
        return self._sync(lambda: [to_jsonable(n.to_dict(len(ric.ues_of(n.id)))) for n in ric.connected_nodes()])

    def ues(self, node=None):
        ric = self.ctx.ric

        def get():
            src = ric.ues_of(node) if node else list(ric.ues.values())
            out = []
            for u in src:
                d = to_jsonable(u.to_dict())
                d["ue_id_raw"] = u.ue_id
                out.append(d)
            return out
        return self._sync(get)

    def subscribe(self, node, ran_function_id, event_trigger, actions, description=None):
        def on_ind(ind: Indication):
            self._deliver({"sub": ind.subscription.id, "node": ind.node_id,
                           "ran_function_id": ind.ran_function_id, "action_id": ind.action_id,
                           "sn": ind.sn, "type": ind.type, "header": ind.header, "message": ind.message,
                           "call_process_id": ind.call_process_id, "sm": ind.subscription.service_model})
        acts = [Action(a["id"], a.get("type", "report"), a.get("definition")) for a in actions]
        sub = self._run(self.ctx.subscribe(node, ran_function_id, event_trigger, acts, on_ind, description))
        return {"sub": sub.id, "service_model": sub.service_model, "admitted": sub.admitted,
                "not_admitted": sub.not_admitted}

    def unsubscribe(self, sub):
        async def unsub():
            for s in self.ctx.subscriptions():
                if s.id == sub:
                    await self.ctx.unsubscribe(s)
                    return
            # already gone (node disconnected / reset, or deleted from the dashboard)
        self._run(unsub())

    def control(self, node, ran_function_id, header, message, ack=True, call_process_id=None):
        res = self._run(self.ctx.control(node, ran_function_id, header, message, ack=ack,
                                         call_process_id=call_process_id))
        return to_jsonable(res.to_dict())

    def status(self, status):
        self.loop.call_soon_threadsafe(lambda: self.ctx.set_status(**status))

    def close(self):
        pass


class RemoteLink(Link):
    def __init__(self, address: Optional[str] = None, name: str = "xapp", version: Optional[str] = None,
                 description: Optional[str] = None):
        address = address or os.environ.get("URIC_RIC") or DEFAULT_RIC_ADDRESS
        host, _, port = address.rpartition(":")
        self.sock = socket.create_connection((host or "127.0.0.1", int(port)), timeout=TIMEOUT)
        self.sock.settimeout(None)
        self._ids = itertools.count(1)
        self._pending: dict[int, queue.Queue] = {}
        self._wlock = threading.Lock()
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, name="uric-xapp-link", daemon=True)
        self._reader.start()
        hello = self.call("hello", name=name, version=None if version is None else str(version),
                          description=description or None)
        self.name = hello["name"]
        self.ric_info = hello["ric"]

    # framing ------------------------------------------------------------
    def _send(self, obj: dict) -> None:
        data = json.dumps(obj).encode()
        with self._wlock:
            self.sock.sendall(struct.pack("!I", len(data)) + data)

    def _recv_exact(self, n: int) -> Optional[bytes]:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    def _read_loop(self) -> None:
        try:
            while True:
                hdr = self._recv_exact(4)
                if hdr is None:
                    break
                body = self._recv_exact(struct.unpack("!I", hdr)[0])
                if body is None:
                    break
                msg = json.loads(body)
                if "id" in msg and msg["id"] in self._pending:
                    self._pending[msg["id"]].put(msg)
                elif msg.get("event") == "indication":
                    for k in ("header", "message", "call_process_id"):
                        msg[k] = bytes.fromhex(msg[k]) if msg.get(k) else None
                    self._deliver(msg)
        except OSError:
            pass
        finally:
            self._closed = True
            for q in list(self._pending.values()):
                q.put({"ok": False, "error": "connection to the RIC closed"})

    def call(self, op: str, **kw) -> Any:
        if self._closed:
            raise RICError("connection to the RIC closed")
        rid = next(self._ids)
        q: queue.Queue = queue.Queue()
        self._pending[rid] = q
        try:
            self._send({"id": rid, "op": op, **kw})
            try:
                resp = q.get(timeout=TIMEOUT)
            except queue.Empty:
                raise RICError(f"{op}: no answer from the RIC") from None
        finally:
            self._pending.pop(rid, None)
        if not resp.get("ok"):
            raise RICError(resp.get("error", "error"))
        return resp.get("result")

    # API ----------------------------------------------------------------
    def nodes(self):
        return self.call("nodes")

    def ues(self, node=None):
        res = self.call("ues", node=node)
        for u in res:
            u["ue_id_raw"] = decode_ueid(bytes.fromhex(u["ue_id_raw"]))
        return res

    def subscribe(self, node, ran_function_id, event_trigger, actions, description=None):
        return self.call("subscribe", node=node, ran_function_id=ran_function_id,
                         event_trigger=event_trigger.hex(), description=description,
                         actions=[{"id": a["id"], "type": a.get("type", "report"),
                                   "definition": a["definition"].hex() if a.get("definition") else None}
                                  for a in actions])

    def unsubscribe(self, sub):
        self.call("unsubscribe", sub=sub)

    def control(self, node, ran_function_id, header, message, ack=True, call_process_id=None):
        return self.call("control", node=node, ran_function_id=ran_function_id, header=header.hex(),
                         message=message.hex(), ack=ack,
                         call_process_id=call_process_id.hex() if call_process_id else None)

    def status(self, status):
        self.call("status", status=to_jsonable(status))

    def close(self):
        self._closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
