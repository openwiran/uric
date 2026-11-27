"""A simulated E2 node (gNB) implementing E2AP, E2SM-KPM v3 and E2SM-RC v1.03.

It is meant for development and testing of the RIC and of xApps without a
real RAN. Several nodes can share a :class:`World`, so that RC handovers
between cells of different nodes actually move UEs across nodes.

Supported:

* KPM report styles 1-5 (indication formats 1, 2 and 3)
* RC report style 4 (UE information, indication format 4) on UE events
* RC control style 2 / action 6 (slice level PRB quota)
* RC control style 3 / action 1 (handover)
* RIC subscription delete, service query, reset, E2 removal
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import math
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ..codec import e2ap, kpm, rc
from ..codec.asn import CodecError
from ..codec.e2ap import E2APMessage, cause
from ..codec.e2sm import describe_cgi, gnb_ueid, nr_cgi, ue_key
from ..core.transport import Connection, connect

log = logging.getLogger("uric.sim")

KPM_RAN_FUNCTION_ID = 2
RC_RAN_FUNCTION_ID = 3

NODE_METRICS = ["DRB.UEThpDl", "DRB.UEThpUl", "RRU.PrbTotDl", "RRU.PrbTotUl",
                "RRU.PrbAvailDl", "RRC.ConnMean", "DRB.RlcSduDelayDl"]
UE_METRICS = ["DRB.UEThpDl", "DRB.UEThpUl", "RRU.PrbUsedDl", "RRU.PrbUsedUl",
              "DRB.RlcSduDelayDl", "CQI", "RSRP", "RSRQ"]

_amf_ids = itertools.count(1)


@dataclass
class SimCell:
    plmn: str
    nci: int
    pci: int

    @property
    def cgi(self):
        return nr_cgi(self.plmn, self.nci)


@dataclass
class SimUE:
    amf_id: int
    ran_ueid: int
    sst: int
    cell: SimCell
    rsrp: float = -90.0
    rsrq: float = -10.0
    cqi: int = 10
    load: float = 1.0  # traffic demand factor
    thp_dl: float = 0.0
    thp_ul: float = 0.0
    prb_dl: int = 0
    prb_ul: int = 0
    delay: float = 5.0

    @property
    def ueid(self):
        return gnb_ueid(self.amf_id, self.cell.plmn, ran_ueid=self.ran_ueid, cu_f1ap_id=self.ran_ueid)

    def metric(self, name: str):
        return {"DRB.UEThpDl": self.thp_dl, "DRB.UEThpUl": self.thp_ul, "RRU.PrbUsedDl": self.prb_dl,
                "RRU.PrbUsedUl": self.prb_ul, "DRB.RlcSduDelayDl": self.delay, "CQI": self.cqi,
                "RSRP": round(self.rsrp, 1), "RSRQ": round(self.rsrq, 1)}.get(name)


class World:
    """Shared radio environment of simulated nodes (for inter-node handovers)."""

    def __init__(self):
        self.nodes: list["SimE2Node"] = []

    def find_cell(self, nci: int) -> tuple[Optional["SimE2Node"], Optional[SimCell]]:
        for n in self.nodes:
            for c in n.cells:
                if c.nci == nci:
                    return n, c
        return None, None


@dataclass
class SimSubscription:
    req: dict
    ran_function_id: int
    action_id: int
    period_ms: int
    style: int
    adef: dict
    task: Optional[asyncio.Task] = None
    sn: int = 0
    sm: str = "KPM"


class SimE2Node:
    def __init__(self, gnb_id: int, plmn: str = "00101", n_cells: int = 2, n_ues: int = 4,
                 world: Optional[World] = None, churn: float = 0.0, seed: Optional[int] = None,
                 name: Optional[str] = None):
        self.gnb_id = gnb_id
        self.plmn = plmn
        self.name = name or f"gnb-{gnb_id:x}"
        self.world = world or World()
        self.world.nodes.append(self)
        self.rand = random.Random(seed if seed is not None else gnb_id)
        self.cells = [SimCell(plmn, (gnb_id << 4) | i, (gnb_id * 3 + i) % 1008) for i in range(n_cells)]
        self.ues: list[SimUE] = []
        for _ in range(n_ues):
            self.add_ue()
        self.churn = churn
        self.slice_quota: dict[int, dict] = {1: {"min": 0, "max": 100, "dedicated": 0},
                                             2: {"min": 0, "max": 100, "dedicated": 0}}
        self.conn: Optional[Connection] = None
        self.subs: dict[tuple, SimSubscription] = {}
        self._tid = itertools.cycle(range(256))
        self._tasks: list[asyncio.Task] = []
        self.connected = asyncio.Event() if _loop_running() else None
        self.stopped = False
        self.controls: list[dict] = []

    # ---------------------------------------------------------------- RAN model

    def add_ue(self, ue: Optional[SimUE] = None) -> SimUE:
        if ue is None:
            n = next(_amf_ids)
            ue = SimUE(amf_id=n, ran_ueid=n, sst=1 if n % 3 else 2,
                       cell=self.rand.choice(self.cells), rsrp=self.rand.uniform(-110, -75),
                       load=self.rand.uniform(0.3, 1.5))
        self.ues.append(ue)
        return ue

    def tick(self, dt: float = 1.0) -> None:
        """Advance the radio model by ``dt`` seconds."""
        r = self.rand
        per_slice: dict[int, int] = {}
        for u in self.ues:
            per_slice[u.sst] = per_slice.get(u.sst, 0) + 1
        prb_total = 106
        for u in self.ues:
            u.rsrp = max(-125.0, min(-60.0, u.rsrp + r.gauss(0, 1.5)))
            u.rsrq = max(-20.0, min(-3.0, -10.0 + (u.rsrp + 90) / 6 + r.gauss(0, 0.5)))
            u.cqi = int(max(1, min(15, round((u.rsrp + 125) / 4.3 + r.gauss(0, 0.7)))))
            q = self.slice_quota.get(u.sst, {"max": 100})
            share = max(1, per_slice.get(u.sst, 1))
            prb = int(prb_total * q["max"] / 100 / share * min(1.0, u.load))
            se = 0.15 * u.cqi  # bits/s/Hz, rough CQI -> spectral efficiency mapping
            u.prb_dl = max(0, prb)
            u.prb_ul = max(0, int(prb * 0.4))
            u.thp_dl = round(u.prb_dl * 180e3 * se / 1e3 * r.uniform(0.85, 1.0), 1)  # kbps
            u.thp_ul = round(u.prb_ul * 180e3 * se * 0.6 / 1e3 * r.uniform(0.8, 1.0), 1)
            u.delay = round(max(0.5, 2 + 40 * (1 - q["max"] / 100) + r.gauss(0, 0.5) + 15 / u.cqi), 2)
            u.load = max(0.1, min(2.0, u.load + r.gauss(0, 0.05)))

    def node_metric(self, name: str):
        ues = self.ues
        if name == "DRB.UEThpDl":
            return round(sum(u.thp_dl for u in ues), 1)
        if name == "DRB.UEThpUl":
            return round(sum(u.thp_ul for u in ues), 1)
        if name == "RRU.PrbTotDl":
            return min(100, int(sum(u.prb_dl for u in ues) / (106 * len(self.cells)) * 100))
        if name == "RRU.PrbTotUl":
            return min(100, int(sum(u.prb_ul for u in ues) / (106 * len(self.cells)) * 100))
        if name == "RRU.PrbAvailDl":
            return max(0, 106 * len(self.cells) - sum(u.prb_dl for u in ues))
        if name == "RRC.ConnMean":
            return len(ues)
        if name == "DRB.RlcSduDelayDl":
            return round(sum(u.delay for u in ues) / len(ues), 2) if ues else 0.0
        return None

    # ---------------------------------------------------------------- E2 setup

    def ran_function_items(self) -> list[dict]:
        name = {"ranFunction-ShortName": kpm.SHORT_NAME, "ranFunction-E2SM-OID": kpm.OID,
                "ranFunction-Description": "KPM Monitor", "ranFunction-Instance": 1}
        kpm_def = kpm.ran_function_description(name, [
            {"type": 1, "name": "E2 Node Measurement", "action_format": 1, "message_format": 1,
             "metrics": NODE_METRICS},
            {"type": 2, "name": "E2 Node Measurement for a single UE", "action_format": 2,
             "message_format": 1, "metrics": UE_METRICS},
            {"type": 3, "name": "Condition-based, UE-level E2 Node Measurement", "action_format": 3,
             "message_format": 2, "metrics": UE_METRICS},
            {"type": 4, "name": "Common Condition-based, UE-level Measurement", "action_format": 4,
             "message_format": 3, "metrics": UE_METRICS},
            {"type": 5, "name": "E2 Node Measurement for multiple UEs", "action_format": 5,
             "message_format": 3, "metrics": UE_METRICS},
        ])
        rc_name = {"ranFunction-ShortName": rc.SHORT_NAME, "ranFunction-E2SM-OID": rc.OID,
                   "ranFunction-Description": "RAN Control", "ranFunction-Instance": 1}
        s = lambda i, n, params=None, kind="structure": {  # noqa: E731
            "id": i, "name": n, **({"definition": {"kind": kind, "params": params}} if params else {})}
        rc_def = rc.ran_function_definition(
            rc_name,
            event_trigger_styles=[{"type": 1, "name": "Message Event", "format": 1},
                                  {"type": 4, "name": "UE Information Change", "format": 4}],
            report_styles=[{"type": 4, "name": "UE Information", "event_trigger_style": 4,
                            "action_format": 1, "header_format": 1, "message_format": 4,
                            "params": [s(202, "RRC State"), s(17011, "Cell Global ID")]}],
            control_styles=[
                {"type": 2, "name": "Radio Resource Allocation Control", "actions": [
                    {"id": 6, "name": "Slice-level PRB quota", "params": [
                        s(1, "RRM Policy Ratio List", [
                            s(2, "RRM Policy Ratio Group", [
                                s(3, "RRM Policy", [
                                    s(5, "RRM Policy Member List", [
                                        s(6, "RRM Policy Member", [
                                            s(7, "PLMN Identity"),
                                            s(8, "S-NSSAI", [s(9, "SST"), s(10, "SD")])])], "list")]),
                                s(11, "Min PRB Policy Ratio"), s(12, "Max PRB Policy Ratio"),
                                s(13, "Dedicated PRB Policy Ratio")])], "list")]}]},
                {"type": 3, "name": "Connected Mode Mobility Control", "actions": [
                    {"id": 1, "name": "Handover Control", "params": [
                        s(1, "Target Primary Cell ID", [
                            s(2, "CHOICE Target Cell", [
                                s(3, "NR Cell", [s(4, "NR CGI")]),
                                s(5, "E-UTRA Cell", [s(6, "E-UTRA CGI")])])])]}]},
            ])
        return [
            {"ranFunctionID": KPM_RAN_FUNCTION_ID, "ranFunctionDefinition": kpm_def,
             "ranFunctionRevision": 1, "ranFunctionOID": kpm.OID},
            {"ranFunctionID": RC_RAN_FUNCTION_ID, "ranFunctionDefinition": rc_def,
             "ranFunctionRevision": 1, "ranFunctionOID": rc.OID},
        ]

    def setup_request(self) -> bytes:
        return e2ap.encode("E2setupRequest", {
            "TransactionID": next(self._tid),
            "GlobalE2node-ID": e2ap.gnb_global_node_id(self.plmn, self.gnb_id, 32),
            "RANfunctionsAdded": self.ran_function_items(),
            "E2nodeComponentConfigAddition": [{
                "e2nodeComponentInterfaceType": "ng",
                "e2nodeComponentID": ("e2nodeComponentInterfaceTypeNG", {"amf-name": "amf1"}),
                "e2nodeComponentConfiguration": {"e2nodeComponentRequestPart": b"\x00",
                                                 "e2nodeComponentResponsePart": b"\x00"}}]})

    # ---------------------------------------------------------------- run loop

    async def run(self, host: str, port: int, transport: str = "auto", reconnect: bool = True) -> None:
        if self.connected is None:
            self.connected = asyncio.Event()
        self._tasks.append(asyncio.create_task(self._model_loop()))
        delay = 1.0
        while not self.stopped:
            try:
                self.conn = await connect(host, port, transport)
            except OSError as exc:
                if not reconnect:
                    raise
                log.info("%s: connect failed (%s), retrying in %.0fs", self.name, exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)
                continue
            delay = 1.0
            try:
                await self._session()
            except (ConnectionError, OSError) as exc:
                log.info("%s: connection lost: %s", self.name, exc)
            finally:
                self._drop_subs()
                self.connected.clear()
                if self.conn:
                    self.conn.close()
            if not reconnect or self.stopped:
                break
            await asyncio.sleep(1.0)

    async def stop(self) -> None:
        self.stopped = True
        for t in self._tasks:
            t.cancel()
        self._drop_subs()
        if self.conn:
            self.conn.close()

    async def _send(self, name: str, ies: dict) -> None:
        if self.conn is None or self.conn.closed:
            return
        await self.conn.send(e2ap.encode(name, ies))

    async def _session(self) -> None:
        assert self.conn is not None
        await self.conn.send(self.setup_request())
        data = await self.conn.recv()
        if data is None:
            raise ConnectionError("closed during E2 setup")
        resp = e2ap.decode(data)
        if resp.name != "E2setupResponse":
            raise ConnectionError(f"E2 setup failed: {resp.name} {resp.ies.get('Cause')}")
        log.info("%s: E2 setup complete (%d cells, %d UEs)", self.name, len(self.cells), len(self.ues))
        self.connected.set()
        while True:
            data = await self.conn.recv()
            if data is None:
                raise ConnectionError("closed by RIC")
            try:
                msg = e2ap.decode(data)
            except CodecError as exc:
                log.warning("%s: bad PDU: %s", self.name, exc)
                continue
            try:
                await self._handle(msg)
            except Exception:
                log.exception("%s: error handling %s", self.name, msg.name)

    async def _handle(self, msg: E2APMessage) -> None:
        n = msg.name
        if n == "RICsubscriptionRequest":
            await self._on_subscription(msg)
        elif n == "RICsubscriptionDeleteRequest":
            r = msg["RICrequestID"]
            sub = self.subs.pop((r["ricRequestorID"], r["ricInstanceID"]), None)
            if sub and sub.task:
                sub.task.cancel()
            if sub:
                await self._send("RICsubscriptionDeleteResponse", {"RICrequestID": r,
                                                                   "RANfunctionID": msg["RANfunctionID"]})
            else:
                await self._send("RICsubscriptionDeleteFailure", {
                    "RICrequestID": r, "RANfunctionID": msg["RANfunctionID"],
                    "Cause": cause("ricRequest", "request-id-unknown")})
        elif n == "RICcontrolRequest":
            await self._on_control(msg)
        elif n == "RICserviceQuery":
            await self._send("RICserviceUpdate", {"TransactionID": msg["TransactionID"],
                                                  "RANfunctionsModified": self.ran_function_items()})
        elif n == "ResetRequest":
            self._drop_subs()
            await self._send("ResetResponse", {"TransactionID": msg["TransactionID"]})
        elif n == "E2RemovalRequest":
            await self._send("E2RemovalResponse", {"TransactionID": msg["TransactionID"]})
            self.stopped = True
            if self.conn:
                self.conn.close()
        elif n in ("RICserviceUpdateAcknowledge", "E2nodeConfigurationUpdateAcknowledge", "ResetResponse"):
            pass
        elif n == "ErrorIndication":
            log.warning("%s: error indication from RIC: %s", self.name, msg.ies)
        else:
            log.info("%s: unhandled %s", self.name, n)

    def _drop_subs(self) -> None:
        for s in self.subs.values():
            if s.task:
                s.task.cancel()
        self.subs.clear()

    # ---------------------------------------------------------------- subscriptions

    async def _on_subscription(self, msg: E2APMessage) -> None:
        r = msg["RICrequestID"]
        fid = msg["RANfunctionID"]
        det = msg["RICsubscriptionDetails"]
        base = {"RICrequestID": r, "RANfunctionID": fid}

        async def fail(c):
            await self._send("RICsubscriptionFailure", {**base, "Cause": c})

        if fid not in (KPM_RAN_FUNCTION_ID, RC_RAN_FUNCTION_ID):
            return await fail(cause("ricRequest", "ran-function-id-invalid"))
        actions = det["ricAction-ToBeSetup-List"]
        admitted, not_admitted, created = [], [], []
        try:
            if fid == KPM_RAN_FUNCTION_ID:
                et = kpm.decode_event_trigger(det["ricEventTriggerDefinition"])
                period = et["period_ms"] or 1000
            else:
                et = rc.decode_event_trigger(det["ricEventTriggerDefinition"])
                period = 0
        except CodecError:
            return await fail(cause("ricRequest", "event-trigger-not-supported"))
        for a in actions:
            if a["ricActionType"] != "report" or "ricActionDefinition" not in a:
                not_admitted.append({"ricActionID": a["ricActionID"],
                                     "cause": cause("ricRequest", "action-not-supported")})
                continue
            try:
                if fid == KPM_RAN_FUNCTION_ID:
                    adef = kpm.decode_action_definition(a["ricActionDefinition"])
                    sm = "KPM"
                else:
                    adef = rc.decode_action_definition(a["ricActionDefinition"])
                    sm = "RC"
                    if adef["style"] != 4:
                        raise CodecError("unsupported RC report style")
            except CodecError:
                not_admitted.append({"ricActionID": a["ricActionID"],
                                     "cause": cause("ricRequest", "action-not-supported")})
                continue
            sub = SimSubscription(req=r, ran_function_id=fid, action_id=a["ricActionID"], period_ms=period,
                                  style=adef["style"], adef=adef, sm=sm)
            created.append(sub)
            admitted.append({"ricActionID": a["ricActionID"]})
        if not admitted:
            return await fail(cause("ricRequest", "action-not-supported"))
        await self._send("RICsubscriptionResponse", {**base, "RICactions-Admitted": admitted,
                                                     "RICactions-NotAdmitted": not_admitted or None})
        key = (r["ricRequestorID"], r["ricInstanceID"])
        for sub in created:
            self.subs[key] = sub
            if sub.sm == "KPM":
                sub.task = asyncio.create_task(self._kpm_loop(sub))
            else:
                await self._rc_report(sub)

    async def _indication(self, sub: SimSubscription, header: bytes, message: bytes) -> None:
        sub.sn = (sub.sn + 1) % 65536
        await self._send("RICindication", {
            "RICrequestID": sub.req, "RANfunctionID": sub.ran_function_id, "RICactionID": sub.action_id,
            "RICindicationSN": sub.sn, "RICindicationType": "report",
            "RICindicationHeader": header, "RICindicationMessage": message})

    def _ue_by_id(self, ueid) -> Optional[SimUE]:
        k = ue_key(ueid)
        for u in self.ues:
            if ue_key(u.ueid) == k:
                return u
        return None

    def _match(self, u: SimUE, cond: dict) -> bool:
        ttype = cond["testType"][0]
        expr = cond.get("testExpr")
        val = cond.get("testValue")
        v = val[1] if val else None
        if ttype == "sNSSAI":
            sst = v[0] if isinstance(v, (bytes, bytearray)) else v
            return v is None or u.sst == sst
        cur = {"rSRP": u.rsrp, "rSRQ": u.rsrq, "cQI": u.cqi, "fiveQI": 9}.get(ttype)
        if cur is None or expr in (None, "present") or v is None:
            return True
        if isinstance(v, tuple):
            from ..codec.asn import real_to_float
            v = real_to_float(v)
        return {"equal": cur == v, "greaterthan": cur > v, "lessthan": cur < v}.get(expr, True)

    async def _kpm_loop(self, sub: SimSubscription) -> None:
        period = max(sub.period_ms, 50) / 1000
        a = sub.adef
        while True:
            await asyncio.sleep(period)
            hdr = kpm.indication_header(sender=self.name, vendor="uric-sim")
            metrics = a["metrics"]
            gp = a.get("granularity_ms")
            if a["format"] == 1:
                msg = kpm.indication_message_format1(metrics, [self.node_metric(m) for m in metrics], gp)
            elif a["format"] == 2:
                u = self._ue_by_id(a["ue_id"])
                if u is None:
                    continue
                msg = kpm.indication_message_format1(metrics, [u.metric(m) for m in metrics], gp)
            elif a["format"] == 3:
                items = []
                for c in a["conditions"]:
                    name = c["measType"][1]
                    tests = [m["matchingCondChoice"][1] for m in c["matchingCond"]
                             if m["matchingCondChoice"][0] == "testCondInfo"]
                    matched = [u for u in self.ues if all(self._match(u, t) for t in tests)]
                    vals = [u.metric(name) for u in matched if u.metric(name) is not None]
                    items.append((name, c["matchingCond"], [u.ueid for u in matched],
                                  (sum(vals) / len(vals)) if vals else None))
                msg = kpm.indication_message_format2(items, gp)
            else:
                if a["format"] == 4:
                    ues = [u for u in self.ues if all(self._match(u, c) for c in a["ue_conditions"])]
                else:
                    ues = [u for u in (self._ue_by_id(x) for x in a["ue_ids"]) if u is not None]
                if not ues:
                    continue
                msg = kpm.indication_message_format3([(u.ueid, metrics, [u.metric(m) for m in metrics])
                                                      for u in ues], gp)
            await self._indication(sub, hdr, msg)

    async def _rc_report(self, sub: SimSubscription, ues: Optional[list[SimUE]] = None) -> None:
        ues = self.ues if ues is None else ues
        if not ues:
            return
        await self._indication(sub, rc.indication_header_format1(1),
                               rc.indication_message_format4([(u.ueid, u.cell.cgi, None) for u in ues]))

    async def notify_ue_change(self, ues: list[SimUE]) -> None:
        for sub in list(self.subs.values()):
            if sub.sm == "RC":
                await self._rc_report(sub, ues)

    # ---------------------------------------------------------------- control

    async def _on_control(self, msg: E2APMessage) -> None:
        base = {"RICrequestID": msg["RICrequestID"], "RANfunctionID": msg["RANfunctionID"],
                "RICcallProcessID": msg.get("RICcallProcessID")}
        want_ack = msg.get("RICcontrolAckRequest", "ack") == "ack"

        async def fail(c):
            await self._send("RICcontrolFailure", {**base, "Cause": c})

        if msg["RANfunctionID"] != RC_RAN_FUNCTION_ID:
            return await fail(cause("ricRequest", "ran-function-id-invalid"))
        try:
            hdr = rc.decode_control_header(msg["RICcontrolHeader"])
            body = rc.decode_control_message(msg["RICcontrolMessage"])
        except CodecError:
            return await fail(cause("ricRequest", "control-message-invalid"))
        style, action = hdr.get("style"), hdr.get("action")
        params = body.get("params", {})
        self.controls.append({"style": style, "action": action, "params": params,
                              "ue": ue_key(hdr["ue_id"]) if hdr.get("ue_id") is not None else None,
                              "ts": time.time()})
        outcome: dict[int, Any] = {}
        if style == 2 and action == 6:
            try:
                for grp in params[1]:
                    g = grp[2]
                    # E2SM-RC v5.00 RAN parameter IDs (see uric.codec.rc.slice_prb_quota_params)
                    member = g[3][5][0][6]
                    sst = member[8][9][0]
                    self.slice_quota[sst] = {"min": g.get(11, 0), "max": g.get(12, 100),
                                             "dedicated": g.get(13, 0)}
            except (KeyError, IndexError, TypeError):
                return await fail(cause("ricRequest", "control-message-invalid"))
            log.info("%s: slice PRB quota now %s", self.name, self.slice_quota)
            outcome = {1: len(params[1])}
        elif style == 3 and action == 1:
            tgt = rc.parse_handover_target(params)
            ue = self._ue_by_id(hdr["ue_id"])
            if tgt is None or ue is None:
                return await fail(cause("ricRequest", "control-message-invalid"))
            node, cell = self.world.find_cell(tgt[2])
            if cell is None or node is None:
                return await fail(cause("ricRequest", "control-message-invalid"))
            if node is self:
                ue.cell = cell
                await self.notify_ue_change([ue])
            else:
                self.ues.remove(ue)
                ue.cell = cell
                node.add_ue(ue)
                await node.notify_ue_change([ue])
            log.info("%s: UE %d handed over to %s cell %x", self.name, ue.amf_id, node.name, cell.nci)
            outcome = {1: cell.nci}
        else:
            return await fail(cause("ricRequest", "action-not-supported"))
        if want_ack:
            await self._send("RICcontrolAcknowledge", {**base, "RICcontrolOutcome": rc.control_outcome(outcome)})

    # ---------------------------------------------------------------- model loop

    async def _model_loop(self) -> None:
        while True:
            self.tick(1.0)
            if self.churn and self.connected is not None and self.connected.is_set():
                if self.ues and self.rand.random() < self.churn:
                    gone = self.rand.choice(self.ues)
                    self.ues.remove(gone)
                    log.info("%s: UE %d detached", self.name, gone.amf_id)
                if self.rand.random() < self.churn:
                    u = self.add_ue()
                    log.info("%s: UE %d attached", self.name, u.amf_id)
                    await self.notify_ue_change([u])
            await asyncio.sleep(1.0)

    def describe(self) -> dict:
        return {"name": self.name, "gnb_id": self.gnb_id, "cells": [describe_cgi(c.cgi) for c in self.cells],
                "ues": [{"amf_id": u.amf_id, "sst": u.sst, "cell": u.cell.nci} for u in self.ues],
                "slice_quota": self.slice_quota}


def _loop_running() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False
