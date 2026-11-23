"""Northbound xApp interface: lets xApps run as separate processes.

This plays the role of RMR + the subscription manager of the OSC RIC, with a
much simpler wire format: TCP, every frame is a 4 byte big-endian length
followed by a UTF-8 JSON object. Binary fields (E2SM payloads, UEIDs) are hex
strings.

Requests (xApp -> RIC) carry an ``id`` and an ``op``; the RIC answers with
``{"id": .., "ok": true, "result": ..}`` or ``{"id": .., "ok": false,
"error": ".."}``:

* ``hello {name}`` – must be first; registers the xApp (visible in the dashboard)
* ``nodes`` – connected E2 nodes with their RAN functions/capabilities
* ``ues {node?}`` – UE registry, with ``ue_id_raw`` (APER encoded UEID)
* ``subscribe {node, ran_function_id, event_trigger, actions:[{id,type,definition}]}``
* ``unsubscribe {sub}``
* ``control {node, ran_function_id, header, message, ack, call_process_id?}``
* ``status {status}`` – publish xApp status

Events (RIC -> xApp) have an ``event`` key, e.g. ``{"event": "indication",
"sub", "node", "ran_function_id", "action_id", "sn", "type", "header",
"message", "call_process_id", "sm"}``. When the connection closes, every
subscription owned by the xApp is deleted.
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
from .model import Action, Indication

if TYPE_CHECKING:
    from ..xapp.manager import XAppManager
    from .ric import NearRTRIC

log = logging.getLogger("uric.northbound")

DEFAULT_XAPP_PORT = 4560


def _hex(b: Optional[bytes]) -> Optional[str]:
    return b.hex() if b is not None else None


def _unhex(s: Optional[str]) -> Optional[bytes]:
    return bytes.fromhex(s) if s else None


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

        async def on_indication(ind: Indication) -> None:
            try:
                await send({"event": "indication", "sub": ind.subscription.id, "node": ind.node_id,
                            "ran_function_id": ind.ran_function_id, "action_id": ind.action_id,
                            "sn": ind.sn, "type": ind.type, "header": _hex(ind.header),
                            "message": _hex(ind.message), "call_process_id": _hex(ind.call_process_id),
                            "sm": ind.subscription.service_model})
            except (ConnectionError, RuntimeError):
                pass

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
                                         source="remote", state="running", session=ctx, started_at=time.time())
                        rec.remote_close = writer.close  # type: ignore[attr-defined]
                        self.manager.xapps[name] = rec
                        self.manager.notify(rec)
                        log.info("remote xApp %s connected from %s", name, peer)
                        result: Any = {"name": name, "ric": self.ric.info()}
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
                                    call_process_id=_unhex(msg.get("call_process_id")))
            return res.to_dict()
        if op == "status":
            ctx.set_status(**(msg.get("status") or {}))
            return {"ok": True}
        raise ValueError(f"unknown op {op!r}")
