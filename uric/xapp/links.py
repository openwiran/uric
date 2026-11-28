"""Transports between an :class:`~uric.xapp.xappbase.xAppBase` xApp and the RIC.

* :class:`LocalLink` – the xApp runs inside the RIC process (loaded by the
  xApp manager); calls are bridged from xApp threads to the RIC event loop.
* :class:`RemoteLink` – the xApp is a separate process connected to the RIC's
  northbound xApp interface (:mod:`uric.core.northbound`).

Both expose the same blocking API and deliver indications to ``on_indication``
as dicts ``{sub, node, ran_function_id, action_id, sn, type, header, message,
call_process_id, sm, trace?}`` (bytes for binary fields; ``trace`` is the RIC
latency trace ID of the indication, absent or None when not available).

Latency: :meth:`Link.timing` reports an xApp side duration (embedded: straight
into the RIC recorder; remote: batched in ``timing`` ops). Against a RIC that
does not support it (no ``features`` in the ``hello`` answer) the remote link
silently sends no trace IDs and no timing samples.
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
import time
import logging
from typing import Any, Callable, Optional

from ..codec.asn import to_jsonable
from ..codec.e2sm import decode_ueid
from ..core.model import Action, Indication
from ..core.latency import now_ns
from ..core.ric import RICError

log = logging.getLogger("uric.xapp.link")

DEFAULT_RIC_ADDRESS = "127.0.0.1:4560"
TIMEOUT = 15.0
#: remote timing samples are flushed every TIMING_BATCH samples or TIMING_FLUSH_S seconds
TIMING_BATCH = 256
TIMING_FLUSH_S = 1.0


class Link:
    on_indication: Optional[Callable[[dict], None]] = None
    embedded = False

    def nodes(self) -> list[dict]: ...
    def ues(self, node: Optional[str] = None) -> list[dict]: ...
    def subscribe(self, node: str, ran_function_id: int, event_trigger: bytes, actions: list[dict],
                  description: Optional[dict] = None) -> dict: ...
    def unsubscribe(self, sub: str) -> None: ...
    def control(self, node: str, ran_function_id: int, header: bytes, message: bytes, ack: bool = True,
                call_process_id: Optional[bytes] = None, trace: Optional[int] = None) -> dict: ...
    def status(self, status: dict) -> None: ...
    def close(self) -> None: ...

    #: True when xApp side timing samples are wanted by the RIC
    timing_enabled = False

    def timing(self, stage: str, ns: int, node: Optional[str] = None, trace: Optional[int] = None) -> None:
        """Report one xApp side duration (never raises)."""

    def tick(self) -> None:
        """Called periodically by the xApp dispatcher (flushes buffered samples)."""

    def latency(self, **filters) -> dict:
        """RIC side latency snapshot restricted to this xApp ({} when unsupported)."""
        return {}

    def _deliver(self, d: dict) -> None:
        if self.on_indication is not None:
            self.on_indication(d)


class LocalLink(Link):
    embedded = True

    def __init__(self, session, loop: asyncio.AbstractEventLoop):
        self.ctx = session
        self.loop = loop
        self.lat = session.ric.latency
        self.timing_enabled = self.lat.enabled
        # xApp side samples are batched: the recorder lock is shared with the RIC event
        # loop, and taking it per sample from the xApp thread costs throughput under load
        self._tbuf: list = []
        self._tlock = threading.Lock()
        self._tlast = time.monotonic()

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
        lat, name = self.lat, self.ctx.name

        def on_ind(ind: Indication):
            self._deliver({"sub": ind.subscription.id, "node": ind.node_id,
                           "ran_function_id": ind.ran_function_id, "action_id": ind.action_id,
                           "sn": ind.sn, "type": ind.type, "header": ind.header, "message": ind.message,
                           "call_process_id": ind.call_process_id, "sm": ind.subscription.service_model,
                           "trace": ind.trace})
            if ind.rx_ns is not None:
                lat.record("ric_dispatch", now_ns() - ind.rx_ns, name, ind.node_id, ind.trace)
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

    def control(self, node, ran_function_id, header, message, ack=True, call_process_id=None, trace=None):
        res = self._run(self.ctx.control(node, ran_function_id, header, message, ack=ack,
                                         call_process_id=call_process_id, trace=trace))
        return to_jsonable(res.to_dict())

    def timing(self, stage, ns, node=None, trace=None):
        with self._tlock:
            self._tbuf.append((stage, ns, node, trace))
            full = len(self._tbuf) >= TIMING_BATCH
        if full:
            self._flush_timing()

    def tick(self):
        if self._tbuf and time.monotonic() - self._tlast >= TIMING_FLUSH_S:
            self._flush_timing()

    def _flush_timing(self) -> None:
        with self._tlock:
            batch, self._tbuf = self._tbuf, []
            self._tlast = time.monotonic()
        if batch:
            self.lat.record_many(self.ctx.name, batch)

    def latency(self, **filters):
        self._flush_timing()
        return self.lat.snapshot(xapp=self.ctx.name, node=filters.get("node"), stage=filters.get("stage"))

    def status(self, status):
        self.loop.call_soon_threadsafe(lambda: self.ctx.set_status(**status))

    def close(self):
        self._flush_timing()


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
        self._fire_ids: set = set()
        self._tbuf: list = []
        self._tlock = threading.Lock()
        self._tlast = time.monotonic()
        self._reader = threading.Thread(target=self._read_loop, name="uric-xapp-link", daemon=True)
        self._reader.start()
        hello = self.call("hello", name=name, version=None if version is None else str(version),
                          description=description or None)
        self.name = hello["name"]
        self.ric_info = hello["ric"]
        # optional features of newer RICs; an old RIC sends none -> degrade silently
        feats = set(hello.get("features") or ())
        lat_on = bool((hello.get("latency") or {}).get("enabled"))
        self._trace_ok = "trace" in feats
        self._latency_ok = "latency" in feats
        self.timing_enabled = "timing" in feats and lat_on

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
                elif "id" in msg and msg["id"] in self._fire_ids:
                    self._fire_ids.discard(msg["id"])
                    if not msg.get("ok") and self.timing_enabled:
                        self.timing_enabled = False
                        log.warning("RIC refused timing samples (%s): xApp latency reporting disabled",
                                    msg.get("error"))
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

    def control(self, node, ran_function_id, header, message, ack=True, call_process_id=None, trace=None):
        kw = {}
        if trace is not None and self._trace_ok:
            kw["trace"] = trace
        return self.call("control", node=node, ran_function_id=ran_function_id, header=header.hex(),
                         message=message.hex(), ack=ack,
                         call_process_id=call_process_id.hex() if call_process_id else None, **kw)

    # latency ------------------------------------------------------------
    def timing(self, stage, ns, node=None, trace=None):
        if not self.timing_enabled:
            return
        try:
            s = {"stage": stage, "ns": int(ns)}
            if node is not None:
                s["node"] = node
            if trace is not None:
                s["trace"] = trace
            with self._tlock:
                self._tbuf.append(s)
                full = len(self._tbuf) >= TIMING_BATCH
            if full:
                self._flush_timing()
        except Exception:
            log.debug("timing sample dropped", exc_info=True)

    def tick(self):
        if self._tbuf and time.monotonic() - self._tlast >= TIMING_FLUSH_S:
            self._flush_timing()

    def _flush_timing(self) -> None:
        with self._tlock:
            batch, self._tbuf = self._tbuf, []
            self._tlast = time.monotonic()
        if not batch or self._closed or not self.timing_enabled:
            return
        # fire and forget: the answer is only looked at when it is an error
        rid = next(self._ids)
        self._fire_ids.add(rid)
        try:
            self._send({"id": rid, "op": "timing", "samples": batch})
        except OSError:
            self._fire_ids.discard(rid)

    def latency(self, **filters):
        if not self._latency_ok:
            return {}
        try:
            return self.call("latency", **{k: v for k, v in filters.items() if v is not None}) or {}
        except RICError:
            return {}

    def status(self, status):
        self.call("status", status=to_jsonable(status))

    def close(self):
        try:
            self._flush_timing()
        except Exception:
            pass
        self._closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
