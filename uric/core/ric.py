"""The near-RT RIC engine: E2 termination, subscription manager and UE registry.

Implemented E2AP procedures (O-RAN.WG3.E2AP):

==============================  =========  ===============================
Procedure                       Initiator  Notes
==============================  =========  ===============================
E2 Setup                        E2 node    RAN functions + component config
RIC Service Update              E2 node    add / modify / delete functions
RIC Service Query               RIC        :meth:`NearRTRIC.service_query`
E2 Node Configuration Update    E2 node    component add / update / remove
Reset                           both       :meth:`NearRTRIC.reset`
E2 Removal                      both       :meth:`NearRTRIC.remove_node`
RIC Subscription                RIC        :meth:`NearRTRIC.subscribe`
RIC Subscription Delete         RIC        :meth:`NearRTRIC.unsubscribe`
RIC Subscription Delete Req.    E2 node    triggers RIC Subscription Delete
RIC Indication                  E2 node    report / insert
RIC Control                     RIC        :meth:`NearRTRIC.control`
Error Indication                both
==============================  =========  ===============================
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from ..codec import e2ap, kpm, rc
from ..codec.asn import CodecError, to_jsonable
from ..codec.e2ap import E2APMessage, cause, cause_str, global_node_key
from . import events as ev
from .events import EventBus
from .latency import LatencyRecorder, now_ns
from .model import UE, Action, E2Node, Indication, RANFunction, Subscription
from .transport import Connection, E2Listener, sctp_available, E2_SCTP_PORT

log = logging.getLogger("uric.ric")


class RICError(Exception):
    """Base class for RIC procedure errors."""


class E2Timeout(RICError):
    pass


class SubscriptionError(RICError):
    def __init__(self, msg: str, cause_value=None):
        super().__init__(msg)
        self.cause = cause_value


class ControlError(RICError):
    def __init__(self, msg: str, cause_value=None, outcome: Optional[bytes] = None):
        super().__init__(msg)
        self.cause = cause_value
        self.outcome = outcome


@dataclass
class ControlResult:
    node_id: str
    ran_function_id: int
    success: bool
    outcome: Optional[bytes] = None
    decoded_outcome: Any = None
    cause: Any = None
    call_process_id: Optional[bytes] = None

    def to_dict(self) -> dict:
        return {"node_id": self.node_id, "ran_function_id": self.ran_function_id,
                "success": self.success, "outcome": to_jsonable(self.decoded_outcome),
                "cause": cause_str(self.cause) if self.cause else None}


@dataclass
class RICConfig:
    plmn: str = "00101"
    ric_id: int = 1
    e2_host: str = "0.0.0.0"
    e2_port: int = E2_SCTP_PORT
    #: list of transports to listen on: "sctp", "tcp" ("auto" = sctp if available else tcp)
    e2_transports: tuple = ("auto",)
    #: TCP port used when both SCTP and TCP are enabled (TCP and SCTP may share the number)
    e2_tcp_port: Optional[int] = None
    procedure_timeout: float = 5.0
    #: seconds after which a UE without updates is removed (0 = never)
    ue_timeout: float = 30.0
    #: seconds after which a disconnected E2 node is forgotten (0 = never)
    node_retention: float = 600.0
    #: control-loop latency instrumentation (:mod:`uric.core.latency`)
    latency_enabled: bool = True
    #: optional JSONL file receiving every raw latency sample
    latency_log_file: Optional[str] = None
    #: seconds an indication trace ID stays valid for linking a control to it
    latency_trace_ttl_s: float = 10.0


class NearRTRIC:
    """Near-RT RIC platform core."""

    def __init__(self, config: Optional[RICConfig] = None):
        self.config = config or RICConfig()
        self.events = EventBus()
        self.nodes: dict[str, E2Node] = {}
        self.ues: dict[str, UE] = {}
        self.subscriptions: dict[tuple[str, int, int], Subscription] = {}
        self.started_at = time.time()
        self._listeners: list[E2Listener] = []
        self._pending: dict[tuple, asyncio.Future] = {}
        self._transaction = itertools.cycle(range(256))
        self._requestors: dict[str, int] = {}
        self._next_requestor = 1
        self._instances: dict[int, itertools.count] = {}
        self._tasks: list[asyncio.Task] = []
        self.stats = {"rx": 0, "tx": 0, "indications": 0, "controls": 0, "errors": 0}
        self.latency = LatencyRecorder(self.config.latency_enabled, self.config.latency_log_file,
                                       self.config.latency_trace_ttl_s)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        transports = []
        for t in self.config.e2_transports:
            if t == "auto":
                t = "sctp" if sctp_available() else "tcp"
            if t == "sctp" and not sctp_available():
                log.warning("SCTP not supported by this OS kernel, skipping SCTP listener")
                continue
            if t not in transports:
                transports.append(t)
        if not transports:
            transports = ["tcp"]
        for t in transports:
            port = self.config.e2_port
            if t == "tcp" and self.config.e2_tcp_port is not None:
                port = self.config.e2_tcp_port
            lst = E2Listener(self.config.e2_host, port, t, self._serve)
            await lst.start()
            self._listeners.append(lst)
        self._tasks.append(asyncio.create_task(self._housekeeping(), name="ric-housekeeping"))

    @property
    def listeners(self) -> list[dict]:
        return [{"transport": l.transport, "host": l.host, "port": l.port} for l in self._listeners]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for l in self._listeners:
            await l.stop()
        for n in list(self.nodes.values()):
            if n.conn:
                n.conn.close()
        self.latency.close()

    async def _housekeeping(self) -> None:
        while True:
            await asyncio.sleep(2.0)
            now = time.time()
            if self.config.ue_timeout:
                for key, ue in list(self.ues.items()):
                    if now - ue.last_seen > self.config.ue_timeout:
                        self._remove_ue(key, "timeout")
            if self.config.node_retention:
                for nid, n in list(self.nodes.items()):
                    if not n.connected and now - n.last_seen > self.config.node_retention:
                        del self.nodes[nid]

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------

    def connected_nodes(self) -> list[E2Node]:
        return [n for n in self.nodes.values() if n.connected]

    def get_node(self, node_id: str) -> E2Node:
        n = self.nodes.get(node_id)
        if n is None or not n.connected:
            raise RICError(f"E2 node {node_id!r} not connected")
        return n

    def ues_of(self, node_id: str) -> list[UE]:
        return [u for u in self.ues.values() if u.node_id == node_id]

    def info(self) -> dict:
        return {"plmn": self.config.plmn, "ric_id": self.config.ric_id,
                "started_at": self.started_at, "uptime": time.time() - self.started_at,
                "listeners": self.listeners,
                "nodes": len(self.connected_nodes()), "ues": len(self.ues),
                "subscriptions": len([s for s in self.subscriptions.values() if s.state == "active"]),
                "stats": dict(self.stats),
                # CPU seconds used by the RIC process (user + system), e.g. for overhead measurements
                "cpu_s": time.process_time()}

    # ------------------------------------------------------------------
    # connection handling
    # ------------------------------------------------------------------

    async def _send(self, node: E2Node | Connection, name: str, ies: dict) -> int:
        """Encode and send a PDU; returns the ``perf_counter_ns`` time at which it was
        written to the transport (0 when latency instrumentation is disabled)."""
        conn = node.conn if isinstance(node, E2Node) else node
        if conn is None or conn.closed:
            raise RICError("connection closed")
        data = e2ap.encode(name, ies)
        await conn.send(data)
        sent = now_ns() if self.latency.enabled else 0
        self.stats["tx"] += 1
        if isinstance(node, E2Node):
            node.stats["tx"] += 1
        log.debug("TX %s -> %s", name, conn.peer)
        return sent

    async def _serve(self, conn: Connection) -> None:
        log.info("new E2 association from %s (%s)", conn.peer, conn.transport)
        node: Optional[E2Node] = None
        try:
            while True:
                data = await conn.recv()
                if data is None:
                    break
                lat = self.latency
                rx_ns = now_ns() if lat.enabled else None
                self.stats["rx"] += 1
                try:
                    msg = e2ap.decode(data)
                    if rx_ns is not None:
                        lat.record("e2ap_decode", now_ns() - rx_ns, None, node.id if node else None)
                except CodecError as exc:
                    self.stats["errors"] += 1
                    log.warning("undecodable PDU from %s: %s", conn.peer, exc)
                    try:
                        await self._send(conn, "ErrorIndication",
                                         {"Cause": cause("protocol", "transfer-syntax-error")})
                    except Exception:
                        pass
                    continue
                log.debug("RX %s <- %s", msg.name, conn.peer)
                if msg.name == "E2setupRequest":
                    # first setup, or a repeated setup refreshing the node state
                    node = await self._on_setup(conn, msg) or node
                    continue
                if node is None:
                    log.warning("%s: %s received before E2 Setup, ignored", conn.peer, msg.name)
                    continue
                node.stats["rx"] += 1
                node.last_seen = time.time()
                try:
                    await self._dispatch(node, msg, rx_ns)
                except Exception:
                    self.stats["errors"] += 1
                    node.stats["errors"] += 1
                    log.exception("error handling %s from %s", msg.name, node.id)
        finally:
            if node is not None and node.connected and self.nodes.get(node.id) is node:
                self._on_disconnect(node)

    def _on_disconnect(self, node: E2Node) -> None:
        log.info("E2 node %s disconnected", node.id)
        node.connected = False
        node.last_seen = time.time()
        for key, s in list(self.subscriptions.items()):
            if s.node_id == node.id:
                s.state = "deleted"
                del self.subscriptions[key]
                self.events.emit(ev.SUBSCRIPTION_DELETED, s.to_dict())
        for k, fut in list(self._pending.items()):
            if k[0] == node.id and not fut.done():
                fut.set_exception(RICError(f"E2 node {node.id} disconnected"))
        for key in [k for k, u in self.ues.items() if u.node_id == node.id]:
            self._remove_ue(key, "node disconnected")
        self.events.emit(ev.NODE_DISCONNECTED, {"id": node.id})

    # ------------------------------------------------------------------
    # E2 setup / service update / configuration update
    # ------------------------------------------------------------------

    def _component_acks(self, items: Iterable[dict]) -> list[dict]:
        return [{"e2nodeComponentInterfaceType": c["e2nodeComponentInterfaceType"],
                 "e2nodeComponentID": c["e2nodeComponentID"],
                 "e2nodeComponentConfigurationAck": {"updateOutcome": "success"}} for c in items]

    async def _on_setup(self, conn: Connection, msg: E2APMessage) -> Optional[E2Node]:
        tid = msg.get("TransactionID", 0)
        try:
            gid = msg["GlobalE2node-ID"]
            node_id = global_node_key(gid)
        except Exception as exc:
            log.warning("E2 Setup from %s with invalid GlobalE2node-ID: %s", conn.peer, exc)
            await self._send(conn, "E2setupFailure", {"TransactionID": tid,
                                                      "Cause": cause("protocol", "semantic-error")})
            return None
        old = self.nodes.get(node_id)
        if old is not None and old.connected and old.conn is not None and old.conn is not conn:
            log.warning("E2 node %s reconnected from %s, closing previous association", node_id, conn.peer)
            old_conn = old.conn
            self._on_disconnect(old)
            old_conn.close()
        node = E2Node(id=node_id, global_id=gid, conn=conn)
        accepted, rejected = [], []
        for item in msg.get("RANfunctionsAdded", []) or []:
            f = RANFunction.from_item(item)
            node.ran_functions[f.id] = f
            accepted.append({"ranFunctionID": f.id, "ranFunctionRevision": f.revision})
            if f.error:
                log.warning("node %s RAN function %d (%s): %s", node_id, f.id, f.oid, f.error)
        node.components = list(msg.get("E2nodeComponentConfigAddition", []) or [])
        resp = {"TransactionID": tid,
                "GlobalRIC-ID": e2ap.global_ric_id(self.config.plmn, self.config.ric_id),
                "RANfunctionsAccepted": accepted or None,
                "RANfunctionsRejected": rejected or None,
                "E2nodeComponentConfigAdditionAck": self._component_acks(node.components)}
        await self._send(conn, "E2setupResponse", resp)
        self.nodes[node_id] = node
        node.stats["rx"] += 1
        node.stats["tx"] += 1
        log.info("E2 node %s connected from %s with RAN functions %s", node_id, conn.peer,
                 ", ".join(f"{f.id}:{f.service_model}" for f in node.ran_functions.values()) or "-")
        self.events.emit(ev.NODE_CONNECTED, node.to_dict())
        return node

    async def _on_service_update(self, node: E2Node, msg: E2APMessage) -> None:
        accepted = []
        for key in ("RANfunctionsAdded", "RANfunctionsModified"):
            for item in msg.get(key, []) or []:
                f = RANFunction.from_item(item)
                node.ran_functions[f.id] = f
                accepted.append({"ranFunctionID": f.id, "ranFunctionRevision": f.revision})
        for item in msg.get("RANfunctionsDeleted", []) or []:
            node.ran_functions.pop(item["ranFunctionID"], None)
        await self._send(node, "RICserviceUpdateAcknowledge", {
            "TransactionID": msg["TransactionID"], "RANfunctionsAccepted": accepted or None})
        self.events.emit(ev.NODE_UPDATED, node.to_dict(len(self.ues_of(node.id))))

    async def _on_config_update(self, node: E2Node, msg: E2APMessage) -> None:
        add = msg.get("E2nodeComponentConfigAddition", []) or []
        upd = msg.get("E2nodeComponentConfigUpdate", []) or []
        rem = msg.get("E2nodeComponentConfigRemoval", []) or []
        def ident(c):
            return (c["e2nodeComponentInterfaceType"], repr(c["e2nodeComponentID"]))
        comps = {ident(c): c for c in node.components}
        for c in add + upd:
            comps[ident(c)] = c
        for c in rem:
            comps.pop(ident(c), None)
        node.components = list(comps.values())
        await self._send(node, "E2nodeConfigurationUpdateAcknowledge", {
            "TransactionID": msg["TransactionID"],
            "E2nodeComponentConfigAdditionAck": self._component_acks(add) or None,
            "E2nodeComponentConfigUpdateAck": self._component_acks(upd) or None,
            "E2nodeComponentConfigRemovalAck": self._component_acks(rem) or None})
        self.events.emit(ev.NODE_UPDATED, node.to_dict(len(self.ues_of(node.id))))

    # ------------------------------------------------------------------
    # dispatcher
    # ------------------------------------------------------------------

    def _resolve(self, key: tuple, value: Any) -> bool:
        fut = self._pending.pop(key, None)
        if fut is not None and not fut.done():
            fut.set_result(value)
            return True
        return False

    async def _dispatch(self, node: E2Node, msg: E2APMessage, rx_ns: Optional[int] = None) -> None:
        n = msg.name
        if n == "RICindication":
            await self._on_indication(node, msg, rx_ns)
        elif n in ("RICsubscriptionResponse", "RICsubscriptionFailure"):
            r = msg["RICrequestID"]
            self._resolve((node.id, "sub", r["ricRequestorID"], r["ricInstanceID"]), msg)
        elif n in ("RICsubscriptionDeleteResponse", "RICsubscriptionDeleteFailure"):
            r = msg["RICrequestID"]
            self._resolve((node.id, "subdel", r["ricRequestorID"], r["ricInstanceID"]), msg)
        elif n in ("RICcontrolAcknowledge", "RICcontrolFailure"):
            r = msg["RICrequestID"]
            self._resolve((node.id, "ctrl", r["ricRequestorID"], r["ricInstanceID"]), msg)
        elif n == "RICsubscriptionDeleteRequired":
            for item in msg["RICsubscriptionToBeRemoved"]:
                r = item["ricRequestID"]
                sub = self.subscriptions.get((node.id, r["ricRequestorID"], r["ricInstanceID"]))
                if sub:
                    log.info("node %s requires deletion of %s (%s)", node.id, sub.id, cause_str(item["cause"]))
                    asyncio.ensure_future(self._safe_unsubscribe(sub))
        elif n == "RICserviceUpdate":
            await self._on_service_update(node, msg)
            self._resolve((node.id, "query"), msg)
        elif n == "E2nodeConfigurationUpdate":
            await self._on_config_update(node, msg)
        elif n == "ResetRequest":
            self._drop_node_subscriptions(node, "reset by E2 node")
            await self._send(node, "ResetResponse", {"TransactionID": msg["TransactionID"]})
        elif n == "ResetResponse":
            self._resolve((node.id, "reset", msg["TransactionID"]), msg)
        elif n == "E2RemovalRequest":
            await self._send(node, "E2RemovalResponse", {"TransactionID": msg["TransactionID"]})
            await asyncio.sleep(0.05)
            if node.conn:
                node.conn.close()
        elif n in ("E2RemovalResponse", "E2RemovalFailure"):
            self._resolve((node.id, "removal", msg["TransactionID"]), msg)
        elif n == "ErrorIndication":
            node.stats["errors"] += 1
            info = {k: to_jsonable(v) for k, v in msg.ies.items()}
            log.warning("Error Indication from %s: %s", node.id, info)
            self.events.emit(ev.ERROR, {"node_id": node.id, "ies": info})
        else:
            log.info("unsupported procedure %s from %s", n, node.id)
            await self._send(node, "ErrorIndication", {
                "Cause": cause("protocol", "message-not-compatible-with-receiver-state")})

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------

    def requestor_id(self, owner: str) -> int:
        """RIC Requestor ID allocated to an owner (one per xApp)."""
        if owner not in self._requestors:
            self._requestors[owner] = self._next_requestor
            self._next_requestor += 1
        return self._requestors[owner]

    def _instance_id(self, requestor: int) -> int:
        cnt = self._instances.setdefault(requestor, itertools.count(1))
        return next(cnt) % 65536

    async def _request(self, node: E2Node, key: tuple, name: str, ies: dict,
                       timeout: Optional[float] = None,
                       on_sent: Optional[Callable[[int], None]] = None) -> E2APMessage:
        fut = asyncio.get_running_loop().create_future()
        self._pending[key] = fut
        try:
            sent = await self._send(node, name, ies)
            if on_sent is not None:
                on_sent(sent)
            return await asyncio.wait_for(fut, timeout or self.config.procedure_timeout)
        except asyncio.TimeoutError:
            raise E2Timeout(f"{name} to {node.id}: no response") from None
        finally:
            self._pending.pop(key, None)

    async def subscribe(self, node_id: str, ran_function_id: int, event_trigger: bytes,
                        actions: list[Action], callback: Optional[Callable] = None,
                        owner: str = "ric", description: Optional[dict] = None) -> Subscription:
        """Send a RIC Subscription Request and wait for the outcome.

        ``callback(indication: Indication)`` is invoked (sync or async) for every
        RIC Indication received for this subscription.
        """
        node = self.get_node(node_id)
        f = node.ran_functions.get(ran_function_id)
        if f is None:
            raise SubscriptionError(f"RAN function {ran_function_id} not supported by {node_id}")
        req = self.requestor_id(owner)
        inst = self._instance_id(req)
        while (node_id, req, inst) in self.subscriptions:
            inst = self._instance_id(req)
        sub = Subscription(node_id=node_id, ran_function_id=ran_function_id, requestor_id=req,
                           instance_id=inst, event_trigger=event_trigger, actions=actions,
                           callback=callback, owner=owner, service_model=f.service_model,
                           description=description or {})
        self.subscriptions[(node_id, req, inst)] = sub
        ies = {"RICrequestID": e2ap.ric_request_id(req, inst), "RANfunctionID": ran_function_id,
               "RICsubscriptionDetails": {"ricEventTriggerDefinition": event_trigger,
                                          "ricAction-ToBeSetup-List": [a.to_item() for a in actions]}}
        try:
            resp = await self._request(node, (node_id, "sub", req, inst), "RICsubscriptionRequest", ies)
        except RICError as exc:
            sub.state, sub.error = "failed", str(exc)
            self.subscriptions.pop((node_id, req, inst), None)
            self.events.emit(ev.SUBSCRIPTION_FAILED, sub.to_dict())
            raise SubscriptionError(str(exc)) from exc
        if resp.name == "RICsubscriptionFailure":
            sub.state, sub.error = "failed", cause_str(resp.get("Cause"))
            self.subscriptions.pop((node_id, req, inst), None)
            self.events.emit(ev.SUBSCRIPTION_FAILED, sub.to_dict())
            raise SubscriptionError(f"subscription refused by {node_id}: {sub.error}", resp.get("Cause"))
        sub.admitted = [a["ricActionID"] for a in resp.get("RICactions-Admitted", []) or []]
        sub.not_admitted = [{"action": a["ricActionID"], "cause": cause_str(a["cause"])}
                            for a in resp.get("RICactions-NotAdmitted", []) or []]
        sub.state = "active"
        log.info("subscription %s active on %s (RAN function %d, owner %s)", sub.id, node_id,
                 ran_function_id, owner)
        self.events.emit(ev.SUBSCRIPTION_CREATED, sub.to_dict())
        return sub

    async def unsubscribe(self, sub: Subscription) -> None:
        """Send a RIC Subscription Delete Request for ``sub``."""
        key = (sub.node_id, sub.requestor_id, sub.instance_id)
        node = self.nodes.get(sub.node_id)
        if sub.state in ("deleted",) or node is None or not node.connected:
            self.subscriptions.pop(key, None)
            sub.state = "deleted"
            return
        sub.state = "deleting"
        try:
            resp = await self._request(node, (sub.node_id, "subdel", sub.requestor_id, sub.instance_id),
                                       "RICsubscriptionDeleteRequest",
                                       {"RICrequestID": e2ap.ric_request_id(sub.requestor_id, sub.instance_id),
                                        "RANfunctionID": sub.ran_function_id})
            if resp.name == "RICsubscriptionDeleteFailure":
                log.warning("subscription delete %s failed: %s", sub.id, cause_str(resp.get("Cause")))
        finally:
            self.subscriptions.pop(key, None)
            sub.state = "deleted"
            self.events.emit(ev.SUBSCRIPTION_DELETED, sub.to_dict())

    async def _safe_unsubscribe(self, sub: Subscription) -> None:
        try:
            await self.unsubscribe(sub)
        except RICError as exc:
            log.warning("unsubscribe %s: %s", sub.id, exc)

    def _drop_node_subscriptions(self, node: E2Node, reason: str) -> None:
        for key, s in list(self.subscriptions.items()):
            if s.node_id == node.id:
                s.state, s.error = "deleted", reason
                del self.subscriptions[key]
                self.events.emit(ev.SUBSCRIPTION_DELETED, s.to_dict())

    def subscriptions_of(self, owner: Optional[str] = None, node_id: Optional[str] = None) -> list[Subscription]:
        return [s for s in self.subscriptions.values()
                if (owner is None or s.owner == owner) and (node_id is None or s.node_id == node_id)]

    # ------------------------------------------------------------------
    # indications
    # ------------------------------------------------------------------

    async def _on_indication(self, node: E2Node, msg: E2APMessage, rx_ns: Optional[int] = None) -> None:
        r = msg["RICrequestID"]
        sub = self.subscriptions.get((node.id, r["ricRequestorID"], r["ricInstanceID"]))
        node.stats["indications"] += 1
        self.stats["indications"] += 1
        if sub is None:
            log.debug("indication for unknown subscription %s from %s", r, node.id)
            return
        sub.indications += 1
        sub.last_indication = time.time()
        ind = Indication(subscription=sub, node_id=node.id, ran_function_id=msg["RANfunctionID"],
                         action_id=msg["RICactionID"], sn=msg.get("RICindicationSN"),
                         type=msg["RICindicationType"], header=msg["RICindicationHeader"],
                         message=msg["RICindicationMessage"], call_process_id=msg.get("RICcallProcessID"))
        lat = self.latency
        timed = rx_ns is not None and lat.enabled
        t0 = now_ns() if timed else 0
        try:
            if sub.service_model == "KPM":
                ind.decoded = kpm.decode_indication(ind.header, ind.message)
                self._update_from_kpm(node, ind.decoded)
            elif sub.service_model == "RC":
                ind.decoded = rc.decode_indication(ind.header, ind.message)
                self._update_from_rc(node, ind.decoded)
        except CodecError as exc:
            log.warning("cannot decode %s indication from %s: %s", sub.service_model, node.id, exc)
        if timed:
            lat.record("sm_decode", now_ns() - t0, sub.owner, node.id)
            ind.rx_ns = rx_ns
            ind.trace = lat.new_trace(rx_ns)
        self.events.emit(ev.INDICATION, {"node_id": node.id, "subscription": sub.id,
                                         "service_model": sub.service_model, "owner": sub.owner},
                         record=False)
        if sub.callback is not None:
            try:
                res = sub.callback(ind)
                if inspect.isawaitable(res):
                    await res
            except Exception:
                log.exception("indication callback of %s failed", sub.owner)

    def _touch_ue(self, node: E2Node, key: str, ue_id: Any) -> UE:
        now = time.time()
        ue = self.ues.get(key)
        if ue is None:
            ue = UE(key=key, ue_id=ue_id, node_id=node.id)
            self.ues[key] = ue
            log.info("UE %s attached to %s", key, node.id)
            self.events.emit(ev.UE_ADDED, ue.to_dict())
        elif ue.node_id != node.id:
            log.info("UE %s moved %s -> %s", key, ue.node_id, node.id)
            ue.attrs["previous_node"] = ue.node_id
            ue.node_id = node.id
        ue.last_seen = now
        return ue

    def _update_from_kpm(self, node: E2Node, rep: kpm.KPMReport) -> None:
        now = time.time()
        if rep.measurements:
            node.metrics.update({k: v[-1] for k, v in rep.measurements.items() if v})
            node.metrics_ts = now
        for key, u in rep.ues.items():
            ue = self._touch_ue(node, key, u["ue_id"])
            vals = {k: v[-1] for k, v in u["measurements"].items() if v}
            if vals:
                ue.metrics.update(vals)
                ue.metrics_ts = now
            self.events.emit(ev.UE_UPDATED, {"key": key, "node_id": node.id, "metrics": to_jsonable(vals)},
                             record=False)

    def _update_from_rc(self, node: E2Node, d: dict) -> None:
        for key, u in d.get("ues", {}).items():
            ue = self._touch_ue(node, key, u["ue_id"])
            if u.get("cell") is not None:
                ue.cell = u["cell"]
            if u.get("params"):
                ue.rc_params.update(u["params"])
            self.events.emit(ev.UE_UPDATED, {"key": key, "node_id": node.id}, record=False)

    def _remove_ue(self, key: str, reason: str) -> None:
        ue = self.ues.pop(key, None)
        if ue is not None:
            log.info("UE %s removed (%s)", key, reason)
            self.events.emit(ev.UE_REMOVED, {"key": key, "node_id": ue.node_id, "reason": reason})

    def register_ue(self, node_id: str, key: str, ue_id: Any) -> UE:
        """Let xApps add a UE they learned about by other means."""
        return self._touch_ue(self.get_node(node_id), key, ue_id)

    # ------------------------------------------------------------------
    # control & other RIC initiated procedures
    # ------------------------------------------------------------------

    async def control(self, node_id: str, ran_function_id: int, header: bytes, message: bytes,
                      ack: bool = True, call_process_id: Optional[bytes] = None,
                      owner: str = "ric", timeout: Optional[float] = None,
                      trace: Optional[int] = None) -> ControlResult:
        """Send a RIC Control Request and (optionally) wait for the acknowledgement.

        ``trace`` is the trace ID of the indication that triggered this control
        (:attr:`Indication.trace`); it links the control to that indication for
        the ``loop`` latency stage.
        """
        lat = self.latency
        t_enter = now_ns() if lat.enabled else 0
        node = self.get_node(node_id)
        f = node.ran_functions.get(ran_function_id)
        if f is None:
            raise ControlError(f"RAN function {ran_function_id} not supported by {node_id}")
        req = self.requestor_id(owner)
        inst = self._instance_id(req)
        ies = {"RICrequestID": e2ap.ric_request_id(req, inst), "RANfunctionID": ran_function_id,
               "RICcallProcessID": call_process_id, "RICcontrolHeader": header,
               "RICcontrolMessage": message, "RICcontrolAckRequest": "ack" if ack else "noAck"}
        self.stats["controls"] += 1
        if not ack:
            sent = await self._send(node, "RICcontrolRequest", ies)
            if t_enter and sent:
                lat.control_sent(owner, node_id, trace, t_enter, sent)
            res = ControlResult(node_id, ran_function_id, True)
            self.events.emit(ev.CONTROL, {"owner": owner, **res.to_dict()})
            return res
        sent_at = [0]

        def on_sent(ts: int) -> None:
            sent_at[0] = ts
            if t_enter and ts:
                lat.control_sent(owner, node_id, trace, t_enter, ts)

        resp = await self._request(node, (node_id, "ctrl", req, inst), "RICcontrolRequest", ies, timeout,
                                   on_sent=on_sent)
        if t_enter and sent_at[0]:
            lat.record("e2_ack_rtt", now_ns() - sent_at[0], owner, node_id, trace)
        outcome = resp.get("RICcontrolOutcome")
        decoded = None
        if outcome and f.service_model == "RC":
            try:
                decoded = rc.decode_control_outcome(outcome)
            except CodecError:
                decoded = None
        res = ControlResult(node_id, ran_function_id, resp.name == "RICcontrolAcknowledge",
                            outcome=outcome, decoded_outcome=decoded, cause=resp.get("Cause"),
                            call_process_id=resp.get("RICcallProcessID"))
        self.events.emit(ev.CONTROL, {"owner": owner, **res.to_dict()})
        return res

    async def service_query(self, node_id: str) -> E2APMessage:
        """RIC Service Query: the node answers with a RIC Service Update."""
        node = self.get_node(node_id)
        accepted = [{"ranFunctionID": f.id, "ranFunctionRevision": f.revision}
                    for f in node.ran_functions.values()]
        return await self._request(node, (node_id, "query"), "RICserviceQuery",
                                   {"TransactionID": next(self._transaction),
                                    "RANfunctionsAccepted": accepted or None})

    async def reset(self, node_id: str, reason: tuple = ("misc", "om-intervention")) -> None:
        """RIC initiated E2 Reset: drops every subscription on the node."""
        node = self.get_node(node_id)
        tid = next(self._transaction)
        await self._request(node, (node_id, "reset", tid), "ResetRequest",
                            {"TransactionID": tid, "Cause": reason})
        self._drop_node_subscriptions(node, "reset by RIC")

    async def remove_node(self, node_id: str) -> None:
        """RIC initiated E2 Removal."""
        node = self.get_node(node_id)
        tid = next(self._transaction)
        try:
            await self._request(node, (node_id, "removal", tid), "E2RemovalRequest", {"TransactionID": tid})
        finally:
            if node.conn:
                node.conn.close()
