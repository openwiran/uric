"""E2SM-KPM v03.00 codec helpers (O-RAN.WG3.E2SM-KPM).

Builders return APER-encoded ``bytes`` ready to be placed in E2AP messages;
decoders return plain, JSON-friendly python structures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from ._gen import e2sm_kpm_asn as _K
from .asn import aper_decode, aper_encode, float_to_real, real_to_float, to_jsonable
from .e2sm import describe_ueid, ntp_timestamp, parse_ntp_timestamp, ran_function_name, ue_key

_M = _K.E2SM_KPM_IEs

EventTriggerDefinition = _M.E2SM_KPM_EventTriggerDefinition
ActionDefinition = _M.E2SM_KPM_ActionDefinition
IndicationHeader = _M.E2SM_KPM_IndicationHeader
IndicationMessage = _M.E2SM_KPM_IndicationMessage
RANFunctionDescription = _M.E2SM_KPM_RANfunction_Description

SHORT_NAME = "ORAN-E2SM-KPM"
OID = "1.3.6.1.4.1.53148.1.3.2.2"  # E2SM-KPM v3
OIDS = ("1.3.6.1.4.1.53148.1.1.2.2", "1.3.6.1.4.1.53148.1.2.2.2", OID)

#: RIC report styles defined by E2SM-KPM v3
REPORT_STYLES = {
    1: "E2 Node Measurement",
    2: "E2 Node Measurement for a single UE",
    3: "Condition-based, UE-level E2 Node Measurement",
    4: "Common Condition-based, UE-level Measurement",
    5: "E2 Node Measurement for multiple UEs",
}

TEST_TYPES = ("gBR", "aMBR", "isStat", "isCatM", "rSRP", "rSRQ", "ul-rSRP", "cQI",
              "fiveQI", "qCI", "sNSSAI")


def is_kpm(oid: str | None, short_name: str | None = None) -> bool:
    if oid and (oid in OIDS or oid.endswith(".2.2") and "53148" in oid):
        return True
    return bool(short_name and "KPM" in short_name.upper())


# --------------------------------------------------------------------------
# Builders
# --------------------------------------------------------------------------

def event_trigger(period_ms: int) -> bytes:
    """Event trigger definition format 1: periodic report every ``period_ms``."""
    return aper_encode(EventTriggerDefinition, {
        "eventDefinition-formats": ("eventDefinition-Format1", {"reportingPeriod": int(period_ms)})})


def _meas_type(m) -> tuple:
    if isinstance(m, int):
        return ("measID", m)
    return ("measName", str(m))


def _label(label: dict | None) -> dict:
    if not label:
        return {"noLabel": "true"}
    return dict(label)


def meas_info_list(metrics: Iterable[Any]) -> list:
    """Build a MeasurementInfoList.

    Each metric is either a name (``"DRB.UEThpDl"``), a measurement ID (int)
    or a ``(name, [label, ...])`` tuple where labels are MeasurementLabel dicts.
    """
    out = []
    for m in metrics:
        if isinstance(m, dict) and "measType" in m:
            out.append(m)
            continue
        if isinstance(m, tuple):
            name, labels = m
            out.append({"measType": _meas_type(name),
                        "labelInfoList": [{"measLabel": _label(l)} for l in labels]})
        else:
            out.append({"measType": _meas_type(m), "labelInfoList": [{"measLabel": _label(None)}]})
    return out


def _format1(metrics, granularity_ms, cell_global_id=None) -> dict:
    v: dict[str, Any] = {"measInfoList": meas_info_list(metrics), "granulPeriod": int(granularity_ms)}
    if cell_global_id is not None:
        v["cellGlobalID"] = cell_global_id
    return v


def _test_value(v) -> tuple:
    if isinstance(v, bool):
        return ("valueBool", v)
    if isinstance(v, int):
        return ("valueInt", v)
    if isinstance(v, float):
        return ("valueReal", float_to_real(v))
    if isinstance(v, (bytes, bytearray)):
        return ("valueOctS", bytes(v))
    if isinstance(v, tuple):
        return v
    return ("valuePrtS", str(v))


def test_condition(test_type: str, expr: str | None = None, value: Any = None) -> dict:
    """Build a TestCondInfo, e.g. ``test_condition("rSRP", "lessthan", -110)``."""
    t: dict[str, Any] = {"testType": (test_type, "true")}
    if expr is not None:
        t["testExpr"] = expr
    if value is not None:
        t["testValue"] = _test_value(value)
    return t


def action_definition(style: int, fmt: dict, fmt_no: int) -> bytes:
    return aper_encode(ActionDefinition, {
        "ric-Style-Type": style,
        "actionDefinition-formats": (f"actionDefinition-Format{fmt_no}", fmt)})


def action_format1(metrics, granularity_ms: int = 1000, cell_global_id=None, style: int = 1) -> bytes:
    """Report style 1: E2 node measurement."""
    return action_definition(style, _format1(metrics, granularity_ms, cell_global_id), 1)


def action_format2(ueid, metrics, granularity_ms: int = 1000, cell_global_id=None, style: int = 2) -> bytes:
    """Report style 2: measurements for a single UE."""
    return action_definition(style, {"ueID": ueid,
                                     "subscriptInfo": _format1(metrics, granularity_ms, cell_global_id)}, 2)


def action_format3(conditions: Sequence[tuple[Any, Sequence[dict]]], granularity_ms: int = 1000,
                   cell_global_id=None, style: int = 3) -> bytes:
    """Report style 3: condition based UE-level measurement.

    ``conditions`` is a list of ``(metric, [MatchingCondItem-Choice ...])`` where
    each choice is ``("measLabel", {...})`` or ``("testCondInfo", test_condition(...))``.
    """
    lst = []
    for metric, conds in conditions:
        lst.append({"measType": _meas_type(metric),
                    "matchingCond": [{"matchingCondChoice": c} for c in conds]})
    v: dict[str, Any] = {"measCondList": lst, "granulPeriod": int(granularity_ms)}
    if cell_global_id is not None:
        v["cellGlobalID"] = cell_global_id
    return action_definition(style, v, 3)


def action_format4(ue_conditions: Sequence[dict], metrics, granularity_ms: int = 1000,
                   cell_global_id=None, style: int = 4) -> bytes:
    """Report style 4: common condition-based UE-level measurement.

    ``ue_conditions`` is a list of :func:`test_condition` results. The E2 node
    reports metrics for every UE matching the conditions (indication format 3).
    """
    return action_definition(style, {
        "matchingUeCondList": [{"testCondInfo": c} for c in ue_conditions],
        "subscriptionInfo": _format1(metrics, granularity_ms, cell_global_id)}, 4)


def action_format5(ueids: Sequence[Any], metrics, granularity_ms: int = 1000,
                   cell_global_id=None, style: int = 5) -> bytes:
    """Report style 5: measurements for a list of UEs (at least two)."""
    return action_definition(style, {
        "matchingUEidList": [{"ueID": u} for u in ueids],
        "subscriptionInfo": _format1(metrics, granularity_ms, cell_global_id)}, 5)


# --------------------------------------------------------------------------
# Indication encoding (E2 node side, used by the simulator)
# --------------------------------------------------------------------------

def _record_item(v) -> tuple:
    if v is None:
        return ("noValue", 0)
    if isinstance(v, int) and 0 <= v <= 0xFFFFFFFF:
        return ("integer", v)
    return ("real", float_to_real(float(v)))


def indication_header(timestamp: float | None = None, sender: str | None = None,
                      vendor: str | None = None) -> bytes:
    h: dict[str, Any] = {"colletStartTime": ntp_timestamp(timestamp), "fileFormatversion": "version3"}
    if sender:
        h["senderName"] = sender
    if vendor:
        h["vendorName"] = vendor
    return aper_encode(IndicationHeader, {"indicationHeader-formats": ("indicationHeader-Format1", h)})


def _format1_msg(metrics: Sequence[str], values: Sequence[Any], granularity_ms: int | None) -> dict:
    v: dict[str, Any] = {"measData": [{"measRecord": [_record_item(x) for x in values]}],
                         "measInfoList": meas_info_list(metrics)}
    if granularity_ms:
        v["granulPeriod"] = int(granularity_ms)
    return v


def indication_message_format1(metrics: Sequence[str], values: Sequence[Any],
                               granularity_ms: int | None = None) -> bytes:
    return aper_encode(IndicationMessage, {"indicationMessage-formats": (
        "indicationMessage-Format1", _format1_msg(metrics, values, granularity_ms))})


def indication_message_format2(items: Sequence[tuple[Any, Sequence[dict], Sequence[Any], Any]],
                               granularity_ms: int | None = None) -> bytes:
    """Condition based UE level report.

    ``items`` is a list of ``(metric, matching conditions, matched UEIDs, value)``.
    """
    conds = []
    rec = []
    for metric, matching, ueids, value in items:
        c: dict[str, Any] = {"measType": _meas_type(metric), "matchingCond": list(matching)}
        if ueids:
            c["matchingUEidList"] = [{"ueID": u} for u in ueids]
        conds.append(c)
        rec.append(_record_item(value))
    v: dict[str, Any] = {"measData": [{"measRecord": rec}], "measCondUEidList": conds}
    if granularity_ms:
        v["granulPeriod"] = int(granularity_ms)
    return aper_encode(IndicationMessage, {"indicationMessage-formats": ("indicationMessage-Format2", v)})


def indication_message_format3(reports: Sequence[tuple[Any, Sequence[str], Sequence[Any]]],
                               granularity_ms: int | None = None) -> bytes:
    """``reports`` is a list of ``(ueid, metrics, values)``."""
    lst = [{"ueID": u, "measReport": _format1_msg(m, v, granularity_ms)} for u, m, v in reports]
    return aper_encode(IndicationMessage, {"indicationMessage-formats": (
        "indicationMessage-Format3", {"ueMeasReportList": lst})})


def ran_function_description(name: dict, report_styles: Sequence[dict],
                             event_trigger_styles: Sequence[dict] | None = None) -> bytes:
    """Encode an E2SM-KPM RAN function description.

    ``report_styles`` items: ``{"type": 1, "name": "...", "action_format": 1,
    "header_format": 1, "message_format": 1, "metrics": ["DRB.UEThpDl", ...]}``.
    """
    if event_trigger_styles is None:
        event_trigger_styles = [{"type": 1, "name": "Periodic Report", "format": 1}]
    rs = []
    for s in report_styles:
        mi = []
        for i, m in enumerate(s["metrics"], 1):
            mi.append({"measName": m, "measID": i})
        rs.append({"ric-ReportStyle-Type": s["type"], "ric-ReportStyle-Name": s["name"],
                   "ric-ActionFormat-Type": s.get("action_format", s["type"]),
                   "measInfo-Action-List": mi,
                   "ric-IndicationHeaderFormat-Type": s.get("header_format", 1),
                   "ric-IndicationMessageFormat-Type": s.get("message_format", 1)})
    return aper_encode(RANFunctionDescription, {
        "ranFunction-Name": name,
        "ric-EventTriggerStyle-List": [
            {"ric-EventTriggerStyle-Type": e["type"], "ric-EventTriggerStyle-Name": e["name"],
             "ric-EventTriggerFormat-Type": e["format"]} for e in event_trigger_styles],
        "ric-ReportStyle-List": rs})


# --------------------------------------------------------------------------
# Decoders
# --------------------------------------------------------------------------

def decode_event_trigger(data: bytes) -> dict:
    v = aper_decode(EventTriggerDefinition, data)
    fmt, body = v["eventDefinition-formats"]
    return {"format": int(fmt[-1]), "period_ms": body.get("reportingPeriod")}


def _meas_name(mt) -> str:
    kind, v = mt
    return str(v) if kind == "measName" else f"#{v}"


def _label_suffix(label: dict) -> str:
    parts = []
    for k, v in label.items():
        if k == "noLabel":
            continue
        if isinstance(v, (bytes, bytearray)):
            v = v.hex()
        elif isinstance(v, dict):
            v = ".".join(x.hex() if isinstance(x, (bytes, bytearray)) else str(x) for x in v.values())
        parts.append(f"{k}={v}")
    return ",".join(parts)


def decode_action_definition(data: bytes) -> dict:
    v = aper_decode(ActionDefinition, data)
    fmt, body = v["actionDefinition-formats"]
    out: dict[str, Any] = {"style": v["ric-Style-Type"], "format": int(fmt.rsplit("Format", 1)[1])}
    sub = body
    if out["format"] == 2:
        out["ue_id"] = body["ueID"]
        sub = body["subscriptInfo"]
    elif out["format"] in (4, 5):
        sub = body["subscriptionInfo"]
        if out["format"] == 4:
            out["ue_conditions"] = [c["testCondInfo"] for c in body["matchingUeCondList"]]
        else:
            out["ue_ids"] = [u["ueID"] for u in body["matchingUEidList"]]
    if out["format"] == 3:
        out["metrics"] = [_meas_name(c["measType"]) for c in body["measCondList"]]
        out["conditions"] = body["measCondList"]
        out["granularity_ms"] = body["granulPeriod"]
        out["cell"] = body.get("cellGlobalID")
    else:
        out["metrics"] = [_meas_name(m["measType"]) for m in sub["measInfoList"]]
        out["granularity_ms"] = sub["granulPeriod"]
        out["cell"] = sub.get("cellGlobalID")
    return out


def _value(item) -> Any:
    kind, v = item
    if kind == "integer":
        return v
    if kind == "real":
        return real_to_float(v)
    return None


def _decode_format1(body: dict) -> dict[str, list]:
    """Map measurement names to the list of values (one per granularity period)."""
    names: list[str] = []
    for mi in body.get("measInfoList") or []:
        base = _meas_name(mi["measType"])
        labels = mi.get("labelInfoList") or [{"measLabel": {"noLabel": "true"}}]
        if len(labels) == 1:
            names.append(base)
        else:
            for l in labels:
                suf = _label_suffix(l["measLabel"])
                names.append(f"{base}[{suf}]" if suf else base)
    res: dict[str, list] = {}
    for data in body.get("measData", []):
        rec = data["measRecord"]
        for i, item in enumerate(rec):
            name = names[i] if i < len(names) else f"#{i}"
            res.setdefault(name, []).append(_value(item))
    return res


@dataclass
class KPMReport:
    """Decoded E2SM-KPM indication."""
    format: int
    timestamp: float
    granularity_ms: int | None = None
    #: node/cell level measurements: name -> [value per granularity period]
    measurements: dict[str, list] = field(default_factory=dict)
    #: UE-level measurements: ue key -> {"ue_id": UEID, "measurements": {...}}
    ues: dict[str, dict] = field(default_factory=dict)
    sender: str | None = None

    def latest(self, name: str, ue: str | None = None):
        src = self.ues[ue]["measurements"] if ue else self.measurements
        vals = src.get(name) or [None]
        return vals[-1]

    def to_dict(self) -> dict:
        return {"format": self.format, "timestamp": self.timestamp,
                "granularity_ms": self.granularity_ms, "measurements": self.measurements,
                "ues": {k: {"ue_id": describe_ueid(v["ue_id"]), "measurements": v["measurements"]}
                        for k, v in self.ues.items()},
                "sender": self.sender}


def decode_indication(header: bytes, message: bytes) -> KPMReport:
    h = aper_decode(IndicationHeader, header)
    hb = h["indicationHeader-formats"][1]
    ts = parse_ntp_timestamp(hb.get("colletStartTime", b""))
    m = aper_decode(IndicationMessage, message)
    fmt, body = m["indicationMessage-formats"]
    fno = int(fmt.rsplit("Format", 1)[1])
    rep = KPMReport(format=fno, timestamp=ts, sender=hb.get("senderName"))
    if fno == 1:
        rep.granularity_ms = body.get("granulPeriod")
        rep.measurements = _decode_format1(body)
    elif fno == 2:
        rep.granularity_ms = body.get("granulPeriod")
        names = [_meas_name(c["measType"]) for c in body["measCondUEidList"]]
        for data in body.get("measData", []):
            for i, item in enumerate(data["measRecord"]):
                rep.measurements.setdefault(names[i] if i < len(names) else f"#{i}", []).append(_value(item))
        for c in body["measCondUEidList"]:
            for u in c.get("matchingUEidList") or []:
                rep.ues.setdefault(ue_key(u["ueID"]), {"ue_id": u["ueID"], "measurements": {}})
    elif fno == 3:
        for item in body["ueMeasReportList"]:
            mr = item["measReport"]
            rep.granularity_ms = rep.granularity_ms or mr.get("granulPeriod")
            rep.ues[ue_key(item["ueID"])] = {"ue_id": item["ueID"], "measurements": _decode_format1(mr)}
    return rep


def decode_ran_function(data: bytes) -> dict:
    """Decode an E2SM-KPM RAN function description into capabilities."""
    v = aper_decode(RANFunctionDescription, data)
    out = {"service_model": "KPM", "name": ran_function_name(v["ranFunction-Name"]),
           "event_trigger_styles": [], "report_styles": []}
    for e in v.get("ric-EventTriggerStyle-List") or []:
        out["event_trigger_styles"].append({"type": e["ric-EventTriggerStyle-Type"],
                                            "name": e["ric-EventTriggerStyle-Name"],
                                            "format": e["ric-EventTriggerFormat-Type"]})
    for r in v.get("ric-ReportStyle-List") or []:
        out["report_styles"].append({
            "type": r["ric-ReportStyle-Type"], "name": r["ric-ReportStyle-Name"],
            "action_format": r["ric-ActionFormat-Type"],
            "header_format": r["ric-IndicationHeaderFormat-Type"],
            "message_format": r["ric-IndicationMessageFormat-Type"],
            "metrics": [{"name": mi["measName"], "id": mi.get("measID")}
                        for mi in r.get("measInfo-Action-List") or []]})
    return to_jsonable(out)
