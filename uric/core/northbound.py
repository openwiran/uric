"""Northbound xApp interface: lets xApps run as separate processes.

This plays the role of RMR + the subscription manager of the OSC RIC, with a
much simpler wire format: TCP, every frame is a 4 byte big-endian length
followed by a UTF-8 JSON object. Binary fields (E2SM payloads, UEIDs) are hex
strings.

Requests (xApp -> RIC) carry an ``id`` and an ``op``; the RIC answers with
``{"id": .., "ok": true, "result": ..}`` or ``{"id": .., "ok": false,
"error": ".."}``:

* ``hello {name, version?, description?}`` – must be first; registers the xApp
  (visible in the dashboard). The result is ``{name, ric, features?, latency?}``:
  ``features`` lists the optional protocol additions this RIC supports
  (``"trace"``, ``"timing"``, ``"latency"``) and ``latency`` is
  ``{"enabled": bool}``. Old RICs send neither: clients then use none of them.
* ``nodes`` – connected E2 nodes with their RAN functions/capabilities
* ``ues {node?}`` – UE registry, with ``ue_id_raw`` (APER encoded UEID)
* ``subscribe {node, ran_function_id, event_trigger, actions:[{id,type,definition}]}``
* ``unsubscribe {sub}``
* ``control {node, ran_function_id, header, message, ack, call_process_id?, trace?}``
  – ``trace`` (optional) is the trace ID of the indication that triggered the
  control; it links the two for the ``loop`` latency stage
* ``status {status}`` – publish xApp status
* ``timing {samples: [{stage, ns, node?, trace?}]}`` (optional) – a batch of
  xApp side durations (``xapp_queue``, ``xapp_callback``,
  ``xapp_control_call``) in nanoseconds of the xApp's own clock. Clients batch
  samples (every N samples or T ms), never one op per indication, and may
  ignore the answer
* ``latency {node?, stage?}`` (optional) – RIC latency snapshot restricted to
  this xApp (milliseconds)

Events (RIC -> xApp) have an ``event`` key, e.g. ``{"event": "indication",
"sub", "node", "ran_function_id", "action_id", "sn", "type", "header",
"message", "call_process_id", "sm", "trace"?}`` (``trace``: optional opaque
integer, present when latency instrumentation is on). When the connection
closes, every subscription owned by the xApp is deleted.
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from typing import TYPE_CHECKING, Any, Optional

from ..codec.asn import to_jsonable
from ..codec.e2sm import encode_ueid
from .latency import XAPP_STAGES, now_ns
from .model import Action, Indication

if TYPE_CHECKING:
    from ..xapp.manager import XAppManager
    from .ric import NearRTRIC

log = logging.getLogger("uric.northbound")

DEFAULT_XAPP_PORT = 4560
#: optional protocol additions advertised in the ``hello`` answer
FEATURES = ["trace", "timing", "latency"]


def _hex(b: Optional[bytes]) -> Optional[str]:
    return b.hex() if b is not None else None


def _unhex(s: Optional[str]) -> Optional[bytes]:
    return bytes.fromhex(s) if s else None


def _trace(v: Any) -> Optional[int]:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


class NorthboundServer:
    def __init__(self, ric: "NearRTRIC", manager: "XAppManager", host: str = "0.0.0.0",
                 port: int = DEFAULT_XAPP_PORT):
        self.ric, self.manager = ric, manager
        self.host, self.port = host, port
        self.server: Optional[asyncio.base_events.Server] = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._client, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        log.info("xApp interface listening on TCP %s:%d", self.host, self.port)

    async def stop(self) -> None:
        if self.server:
            self.server.close()

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        from ..xapp.manager import XAppRecord
        from .session import XAppSession

        peer = writer.get_extra_info("peername")
        lock = asyncio.Lock()
        ctx: Optional[XAppSession] = None
        rec: Optional[XAppRecord] = None

        async def send(obj: dict) -> None:
            data = json.dumps(obj, default=str).encode()
            async with lock:
                writer.write(struct.pack("!I", len(data)) + data)
                await writer.drain()

        lat = self.ric.latency

        async def on_indication(ind: Indication) -> None:
            ev = {"event": "indication", "sub": ind.subscription.id, "node": ind.node_id,
                  "ran_function_id": ind.ran_function_id, "action_id": ind.action_id,
                  "sn": ind.sn, "type": ind.type, "header": _hex(ind.header),
                  "message": _hex(ind.message), "call_process_id": _hex(ind.call_process_id),
                  "sm": ind.subscription.service_model}
            if ind.trace is not None:
                ev["trace"] = ind.trace
            try:
                await send(ev)
            except (ConnectionError, RuntimeError):
                return
            if ind.rx_ns is not None and ctx is not None:
                lat.record("ric_dispatch", now_ns() - ind.rx_ns, ctx.name, ind.node_id, ind.trace)

        try:
            while True:
                try:
                    hdr = await reader.readexactly(4)
                    (n,) = struct.unpack("!I", hdr)
                    msg = json.loads(await reader.readexactly(n))
                except (asyncio.IncompleteReadError, ConnectionError):
                    break
                rid, op = msg.get("id"), msg.get("op")
                try:
                    if op == "hello":
                        name = self._unique_name(str(msg.get("name") or "xapp"))
                        ctx = XAppSession(self.ric, name)
                        rec = XAppRecord(name=name, spec=f"remote {peer[0]}:{peer[1]}" if peer else "remote",
                                         source="remote", state="running", session=ctx, started_at=time.time(),
                                         version=msg.get("version") or None,
                                         description=msg.get("description") or None)
                        rec.remote_close = writer.close  # type: ignore[attr-defined]
                        self.manager.xapps[name] = rec
                        self.manager.notify(rec)
                        log.info("remote xApp %s connected from %s", name, peer)
                        result: Any = {"name": name, "ric": self.ric.info(), "features": FEATURES,
                                       "latency": {"enabled": self.ric.latency.enabled}}
                    elif ctx is None:
                        raise ValueError("send 'hello' first")
                    else:
                        result = await self._op(ctx, op, msg, on_indication)
                    await send({"id": rid, "ok": True, "result": to_jsonable(result)})
                except Exception as exc:
                    await send({"id": rid, "ok": False, "error": str(exc)})
        finally:
            if ctx is not None:
                await ctx.close()
                log.info("remote xApp %s disconnected", ctx.name)
                if rec is not None and self.manager.xapps.get(rec.name) is rec:
                    rec.state = "stopped"
                    del self.manager.xapps[rec.name]
                    self.manager.notify(rec)
            try:
                writer.close()
            except Exception:
                pass

    def _unique_name(self, name: str) -> str:
        base, i = name, 2
        while name in self.manager.xapps and self.manager.xapps[name].state == "running":
            name = f"{base}-{i}"
            i += 1
        return name

    async def _op(self, ctx, op: str, msg: dict, on_indication) -> Any:
        ric = self.ric
        if op == "nodes":
            return [n.to_dict(len(ric.ues_of(n.id))) for n in ric.connected_nodes()]
        if op == "ues":
            node = msg.get("node")
            ues = ric.ues_of(node) if node else list(ric.ues.values())
            out = []
            for u in ues:
                d = u.to_dict()
                d["ue_id_raw"] = encode_ueid(u.ue_id).hex()
                out.append(d)
            return out
        if op == "subscribe":
            actions = [Action(a["id"], a.get("type", "report"), _unhex(a.get("definition")))
                       for a in msg["actions"]]
            sub = await ctx.subscribe(msg["node"], int(msg["ran_function_id"]), _unhex(msg["event_trigger"]),
                                      actions, on_indication, description=msg.get("description"))
            return {"sub": sub.id, "service_model": sub.service_model, "admitted": sub.admitted,
                    "not_admitted": sub.not_admitted}
        if op == "unsubscribe":
            for s in ctx.subscriptions():
                if s.id == msg["sub"]:
                    await ctx.unsubscribe(s)
                    break
            return {"ok": True}  # unknown ids are already gone (node disconnected / reset)
        if op == "control":
            res = await ctx.control(msg["node"], int(msg["ran_function_id"]), _unhex(msg["header"]),
                                    _unhex(msg["message"]), ack=bool(msg.get("ack", True)),
                                    call_process_id=_unhex(msg.get("call_process_id")),
                                    trace=_trace(msg.get("trace")))
            return res.to_dict()
        if op == "timing":
            lat = ric.latency
            n = 0
            for smp in msg.get("samples") or []:
                stage = smp.get("stage")
                if stage not in XAPP_STAGES:
                    continue  # the RIC stages are measured by the RIC only
                ns = smp.get("ns")
                if not isinstance(ns, int) or isinstance(ns, bool) or ns < 0:
                    continue  # malformed sample: skip it, keep the rest of the batch
                lat.record(stage, ns, ctx.name, smp.get("node"), _trace(smp.get("trace")))
                n += 1
            return {"accepted": n}
        if op == "latency":
            return ric.latency.snapshot(xapp=ctx.name, node=msg.get("node"), stage=msg.get("stage"))
        if op == "status":
            ctx.set_status(**(msg.get("status") or {}))
            return {"ok": True}
        raise ValueError(f"unknown op {op!r}")
