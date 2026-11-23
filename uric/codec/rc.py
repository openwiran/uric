"""E2SM-RC v01.03 codec helpers (O-RAN.WG3.E2SM-RC).

RAN parameters are expressed with plain python values:

* scalar -> RANParameter ELEMENT (``bool``, ``int``, ``float``, ``bytes``, ``str``
  or a ``(value, nbits)`` tuple for a BIT STRING). Wrap with :class:`Key` to set
  the ELEMENT "key flag" (``ranP-Choice-ElementTrue``).
* ``dict`` ``{param_id: value}`` -> RANParameter STRUCTURE
* ``list`` of dicts -> RANParameter LIST (each item is a STRUCTURE)

Example (slice level PRB quota, control style 2 / action 6)::

    msg = control_message({1: [{2: {3: [{4: [{5: {6: plmn, 7: {8: b"\\x01"}}}]}],
                                       10: 10, 11: 100, 12: 5}}]})
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from ._gen import e2sm_rc_asn as _R
from .asn import aper_decode, aper_encode, float_to_real, real_to_float, to_jsonable
from .e2ap import plmn_to_bytes
from .e2sm import ran_function_name, ue_key

_M = _R.E2SM_RC_IEs

EventTrigger = _M.E2SM_RC_EventTrigger
ActionDefinition = _M.E2SM_RC_ActionDefinition
IndicationHeader = _M.E2SM_RC_IndicationHeader
IndicationMessage = _M.E2SM_RC_IndicationMessage
CallProcessID = _M.E2SM_RC_CallProcessID
ControlHeader = _M.E2SM_RC_ControlHeader
ControlMessage = _M.E2SM_RC_ControlMessage
ControlOutcome = _M.E2SM_RC_ControlOutcome
RANFunctionDefinition = _M.E2SM_RC_RANFunctionDefinition

SHORT_NAME = "ORAN-E2SM-RC"
OID = "1.3.6.1.4.1.53148.1.1.2.3"

#: RIC control styles defined by E2SM-RC (section 7.6)
CONTROL_STYLES = {
    1: "Radio Bearer Control",
    2: "Radio Resource Allocation Control",
    3: "Connected Mode Mobility Control",
    4: "Radio Access Control",
    5: "Dual Connectivity (DC) Control",
    6: "Carrier Aggregation (CA) Control",
    7: "Idle Mode Mobility Control",
    8: "UE Information And Assignment",
    9: "Measurement Reporting Configuration Control",
    10: "Multiple Actions Control",
    255: "Multiple Actions Control",
}

#: RIC report styles defined by E2SM-RC (section 7.4)
REPORT_STYLES = {
    1: "Message Copy",
    2: "Call Process Outcome",
    3: "E2 Node Information",
    4: "UE Information",
    5: "On Demand Report",
    6: "Multiple Actions",
}

# Well known control actions / RAN parameter IDs (E2SM-RC section 8.4)
STYLE_RADIO_RESOURCE_ALLOCATION = 2
ACTION_SLICE_PRB_QUOTA = 6
STYLE_CONNECTED_MODE_MOBILITY = 3
ACTION_HANDOVER = 1


def is_rc(oid: str | None, short_name: str | None = None) -> bool:
    if oid and (oid == OID or oid.endswith(".2.3") and "53148" in oid):
        return True
    return bool(short_name and short_name.upper().endswith("RC"))


# --------------------------------------------------------------------------
# RAN parameter values
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Key:
    """Marks a RAN parameter ELEMENT as key (``ranP-Choice-ElementTrue``)."""
    value: Any


def ran_parameter_value(v) -> tuple:
    if isinstance(v, bool):
        return ("valueBoolean", v)
    if isinstance(v, int):
        return ("valueInt", v)
    if isinstance(v, float):
        return ("valueReal", float_to_real(v))
    if isinstance(v, (bytes, bytearray)):
        return ("valueOctS", bytes(v))
    if isinstance(v, tuple) and len(v) == 2 and all(isinstance(x, int) for x in v):
        return ("valueBitS", v)
    if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str):
        return v  # already a RANParameter-Value CHOICE
    return ("valuePrintableString", str(v))


def _structure(d: dict) -> dict:
    return {"sequence-of-ranParameters": [
        {"ranParameter-ID": int(k), "ranParameter-valueType": ran_parameter_value_type(v)}
        for k, v in d.items()]}


def ran_parameter_value_type(v) -> tuple:
    """Convert a python value into a RANParameter-ValueType CHOICE."""
    if isinstance(v, Key):
        return ("ranP-Choice-ElementTrue", {"ranParameter-value": ran_parameter_value(v.value)})
    if isinstance(v, dict):
        return ("ranP-Choice-Structure", {"ranParameter-Structure": _structure(v)})
    if isinstance(v, list):
        return ("ranP-Choice-List", {"ranParameter-List": {
            "list-of-ranParameter": [_structure(x) for x in v]}})
    return ("ranP-Choice-ElementFalse", {"ranParameter-value": ran_parameter_value(v)})


def ran_parameters(params: dict) -> list:
    """``{id: value}`` -> list of ``{ranParameter-ID, ranParameter-valueType}``."""
    return [{"ranParameter-ID": int(k), "ranParameter-valueType": ran_parameter_value_type(v)}
            for k, v in params.items()]


def _py_value(v):
    kind, x = v
    if kind == "valueReal":
        return real_to_float(x)
    return x


def parse_value_type(vt) -> Any:
    """Inverse of :func:`ran_parameter_value_type` (key flags are dropped)."""
    kind, body = vt
    if kind in ("ranP-Choice-ElementTrue", "ranP-Choice-ElementFalse"):
        v = body.get("ranParameter-value")
        return None if v is None else _py_value(v)
    if kind == "ranP-Choice-Structure":
        return _parse_structure(body["ranParameter-Structure"])
    if kind == "ranP-Choice-List":
        return [_parse_structure(s) for s in body["ranParameter-List"]["list-of-ranParameter"]]
    return None


def _parse_structure(s: dict) -> dict:
    return {i["ranParameter-ID"]: parse_value_type(i["ranParameter-valueType"])
            for i in s.get("sequence-of-ranParameters") or []}


def parse_ran_parameters(lst: Sequence[dict]) -> dict:
    out = {}
    for i in lst or []:
        if "ranParameter-valueType" in i:
            out[i["ranParameter-ID"]] = parse_value_type(i["ranParameter-valueType"])
        elif "ranParameter-value" in i:
            out[i["ranParameter-ID"]] = _py_value(i["ranParameter-value"])
    return out


# --------------------------------------------------------------------------
# Control
# --------------------------------------------------------------------------

def control_header(ueid, style: int, action: int, decision: str | None = None) -> bytes:
    """Control header format 1."""
    h: dict[str, Any] = {"ueID": ueid, "ric-Style-Type": style, "ric-ControlAction-ID": action}
    if decision:
        h["ric-ControlDecision"] = decision
    return aper_encode(ControlHeader, {"ric-controlHeader-formats": ("controlHeader-Format1", h)})


def control_message(params: dict) -> bytes:
    """Control message format 1 from a ``{ran_param_id: value}`` dict."""
    return aper_encode(ControlMessage, {"ric-controlMessage-formats": (
        "controlMessage-Format1", {"ranP-List": ran_parameters(params)})})


def decode_control_header(data: bytes) -> dict:
    v = aper_decode(ControlHeader, data)
    fmt, b = v["ric-controlHeader-formats"]
    if fmt == "controlHeader-Format1":
        return {"format": 1, "ue_id": b["ueID"], "style": b["ric-Style-Type"],
                "action": b["ric-ControlAction-ID"], "decision": b.get("ric-ControlDecision")}
    return {"format": 2, "ue_id": b.get("ueID"), "decision": b.get("ric-ControlDecision")}


def decode_control_message(data: bytes) -> dict:
    v = aper_decode(ControlMessage, data)
    fmt, b = v["ric-controlMessage-formats"]
    if fmt == "controlMessage-Format1":
        return {"format": 1, "params": parse_ran_parameters(b["ranP-List"])}
    styles = []
    for s in b["ric-ControlStyle-List"]:
        styles.append({"style": s["indicated-Control-Style-Type"], "actions": [
            {"action": a["ric-ControlAction-ID"], "params": parse_ran_parameters(a["ranP-List"]["ranP-List"])}
            for a in s["ric-ControlAction-List"]]})
    return {"format": 2, "styles": styles}


def control_outcome(params: dict) -> bytes:
    """Control outcome format 1 (E2 node side): ``{param_id: scalar}``."""
    return aper_encode(ControlOutcome, {"ric-controlOutcome-formats": ("controlOutcome-Format1", {
        "ranP-List": [{"ranParameter-ID": int(k), "ranParameter-value": ran_parameter_value(v)}
                      for k, v in params.items()]})})


def decode_control_outcome(data: bytes) -> dict:
    v = aper_decode(ControlOutcome, data)
    fmt, b = v["ric-controlOutcome-formats"]
    fno = int(fmt.rsplit("Format", 1)[1])
    if fno in (1, 3):
        return {"format": fno, "params": parse_ran_parameters(b["ranP-List"])}
    return {"format": 2, "styles": to_jsonable(b)}


# Convenience builders for common control actions ---------------------------

def slice_prb_quota_params(slices: Sequence[dict], plmn: str = "00101") -> dict:
    """RAN parameters for "Slice-level PRB quota" (style 2, action 6).

    ``slices`` items: ``{"sst": 1, "sd": 0x000001 (optional), "min": 10,
    "max": 100, "dedicated": 5}`` (ratios in percent).
    """
    groups = []
    for s in slices:
        snssai: dict[int, Any] = {8: bytes([s["sst"]])}
        if s.get("sd") is not None:
            snssai[9] = int(s["sd"]).to_bytes(3, "big")
        member = {6: plmn_to_bytes(s.get("plmn", plmn)), 7: snssai}
        groups.append({2: {3: {4: [{5: member}]},
                           10: int(s.get("min", 0)), 11: int(s.get("max", 100)),
                           12: int(s.get("dedicated", 0))}})
    return {1: groups}


def handover_params(target_cgi) -> dict:
    """RAN parameters for "Handover Control" (style 3, action 1).

    ``target_cgi`` is a CGI CHOICE (see :func:`uric.codec.e2sm.nr_cgi`). The NR
    CGI ELEMENT carries the NR-CGI as an OCTET STRING (PLMN + 36 bit NCI).
    """
    kind, body = target_cgi
    if kind == "nR-CGI":
        nci, _ = body["nRCellIdentity"]
        cgi = body["pLMNIdentity"] + (nci << 4).to_bytes(5, "big")
        return {1: {2: {3: {4: cgi}}}}
    eci, _ = body["eUTRACellIdentity"]
    cgi = body["pLMNIdentity"] + (eci << 4).to_bytes(4, "big")
    return {1: {2: {5: {6: cgi}}}}


def parse_handover_target(params: dict) -> tuple[str, str, int] | None:
    """Return (type, plmn, cell id) from handover control RAN parameters."""
    from .e2ap import bytes_to_plmn
    try:
        choice = params[1][2]
        if 3 in choice:
            b = choice[3][4]
            return ("nr", bytes_to_plmn(b[:3]), int.from_bytes(b[3:], "big") >> 4)
        b = choice[5][6]
        return ("eutra", bytes_to_plmn(b[:3]), int.from_bytes(b[3:], "big") >> 4)
    except (KeyError, TypeError, IndexError):
        return None


# --------------------------------------------------------------------------
# Report service (event triggers / action definitions / indications)
# --------------------------------------------------------------------------

def event_trigger_ue_info_change(rrc_states: Sequence[str] = ("rrc-connected", "rrc-inactive", "rrc-idle"),
                                 condition_id: int = 1) -> bytes:
    """Event trigger format 4 (UE information change: RRC state changes)."""
    return aper_encode(EventTrigger, {"ric-eventTrigger-formats": ("eventTrigger-Format4", {
        "uEInfoChange-List": [{"ric-eventTriggerCondition-ID": condition_id,
                               "triggerType": ("triggerType-Choice-RRCstate", {
                                   "rrcState-List": [{"stateChangedTo": s} for s in rrc_states]})}]})})


def event_trigger_on_demand() -> bytes:
    """Event trigger format 5 (on demand)."""
    return aper_encode(EventTrigger, {"ric-eventTrigger-formats": ("eventTrigger-Format5", {
        "onDemand": "true"})})


def event_trigger_message(messages: Sequence[tuple], condition_id: int = 1) -> bytes:
    """Event trigger format 1. ``messages`` items: ``("rrc", (msg_type, id))`` or
    ``("ni", "f1"|"e1"|...)``."""
    items = []
    for i, (kind, val) in enumerate(messages, condition_id):
        if kind == "rrc":
            mt = ("messageType-Choice-RRC", {"rRC-Message": {"rrcType": val[0], "messageID": val[1]}})
        else:
            mt = ("messageType-Choice-NI", {"nI-Type": val})
        items.append({"ric-eventTriggerCondition-ID": i, "messageType": mt})
    return aper_encode(EventTrigger, {"ric-eventTrigger-formats": ("eventTrigger-Format1", {
        "message-List": items})})


def action_definition_report(style: int, param_ids: Sequence[int]) -> bytes:
    """Action definition format 1: list of RAN parameters to be reported."""
    return aper_encode(ActionDefinition, {"ric-Style-Type": style, "ric-actionDefinition-formats": (
        "actionDefinition-Format1", {"ranP-ToBeReported-List": [{"ranParameter-ID": p} for p in param_ids]})})


def decode_event_trigger(data: bytes) -> dict:
    v = aper_decode(EventTrigger, data)
    fmt, b = v["ric-eventTrigger-formats"]
    return {"format": int(fmt.rsplit("Format", 1)[1]), "body": b}


def decode_action_definition(data: bytes) -> dict:
    v = aper_decode(ActionDefinition, data)
    fmt, b = v["ric-actionDefinition-formats"]
    out: dict[str, Any] = {"style": v["ric-Style-Type"], "format": int(fmt.rsplit("Format", 1)[1]), "body": b}
    if out["format"] == 1:
        out["params"] = [i["ranParameter-ID"] for i in b["ranP-ToBeReported-List"]]
    return out


def indication_header_format1(condition_id: int | None = None) -> bytes:
    b = {} if condition_id is None else {"ric-eventTriggerCondition-ID": condition_id}
    return aper_encode(IndicationHeader, {"ric-indicationHeader-formats": ("indicationHeader-Format1", b)})


def indication_message_format2(ues: Sequence[tuple[Any, dict]]) -> bytes:
    """UE parameter report: ``[(ueid, {param_id: value}), ...]``."""
    return aper_encode(IndicationMessage, {"ric-indicationMessage-formats": ("indicationMessage-Format2", {
        "ueParameter-List": [{"ueID": u, "ranP-List": ran_parameters(p)} for u, p in ues]})})


def indication_message_format1(params: dict) -> bytes:
    return aper_encode(IndicationMessage, {"ric-indicationMessage-formats": ("indicationMessage-Format1", {
        "ranP-Reported-List": ran_parameters(params)})})


def indication_message_format4(ues: Sequence[tuple[Any, Any, bytes | None]], cells: Sequence[Any] = ()) -> bytes:
    """UE information report: ``ues`` = [(ueid, serving cell CGI, context bytes or None)]."""
    ue_items = []
    for u, cgi, ctx in ues:
        it: dict[str, Any] = {"ueID": u, "cellGlobal-ID": cgi}
        if ctx:
            it["ueContextInfo"] = ctx
        ue_items.append(it)
    return aper_encode(IndicationMessage, {"ric-indicationMessage-formats": ("indicationMessage-Format4", {
        "ueInfo-List": ue_items, "cellInfo-List": [{"cellGlobal-ID": c} for c in cells]})})


def decode_indication(header: bytes, message: bytes) -> dict:
    """Decode an E2SM-RC indication into ``{"header":..., "format": n, ...}``.

    UE related formats expose ``"ues": {ue_key: {...}}`` so that the RIC can
    keep its UE registry up to date.
    """
    h = aper_decode(IndicationHeader, header)
    hfmt, hb = h["ric-indicationHeader-formats"]
    m = aper_decode(IndicationMessage, message)
    fmt, b = m["ric-indicationMessage-formats"]
    fno = int(fmt.rsplit("Format", 1)[1])
    out: dict[str, Any] = {"header_format": int(hfmt.rsplit("Format", 1)[1]), "header": hb,
                           "format": fno, "ues": {}}
    if "ueID" in hb:
        out["ue_id"] = hb["ueID"]
    if fno == 1:
        out["params"] = parse_ran_parameters(b["ranP-Reported-List"])
        if "ueID" in hb:
            out["ues"][ue_key(hb["ueID"])] = {"ue_id": hb["ueID"], "params": out["params"]}
    elif fno == 2:
        for it in b["ueParameter-List"]:
            out["ues"][ue_key(it["ueID"])] = {"ue_id": it["ueID"], "params": parse_ran_parameters(it["ranP-List"])}
    elif fno == 4:
        for it in b.get("ueInfo-List") or []:
            out["ues"][ue_key(it["ueID"])] = {"ue_id": it["ueID"], "cell": it["cellGlobal-ID"],
                                              "context": it.get("ueContextInfo")}
        out["cells"] = [c["cellGlobal-ID"] for c in b.get("cellInfo-List") or []]
    elif fno == 5:
        out["params"] = parse_ran_parameters(b["ranP-Requested-List"])
    else:
        out["body"] = b
    return out


# --------------------------------------------------------------------------
# RAN function definition
# --------------------------------------------------------------------------

def _param_def(d) -> list | None:
    if not d:
        return None
    kind, body = d["ranParameter-Definition-Choice"]
    items = body.get("ranParameter-STRUCTURE") or body.get("ranParameter-List") or []
    return {"kind": "structure" if kind == "choiceSTRUCTURE" else "list",
            "params": [_param(i) for i in items]}


def _param(i: dict) -> dict:
    p = {"id": i["ranParameter-ID"], "name": i.get("ranParameter-name")}
    d = _param_def(i.get("ranParameter-Definition"))
    if d:
        p["definition"] = d
    return p


def _params(lst) -> list:
    return [_param(i) for i in lst or []]


def decode_ran_function(data: bytes) -> dict:
    """Decode an E2SM-RC RAN function definition into capabilities."""
    v = aper_decode(RANFunctionDefinition, data)
    out: dict[str, Any] = {"service_model": "RC", "name": ran_function_name(v["ranFunction-Name"]),
                           "event_trigger_styles": [], "report_styles": [], "insert_styles": [],
                           "control_styles": [], "policy_styles": []}
    et = v.get("ranFunctionDefinition-EventTrigger")
    if et:
        for s in et["ric-EventTriggerStyle-List"]:
            out["event_trigger_styles"].append({"type": s["ric-EventTriggerStyle-Type"],
                                                "name": s["ric-EventTriggerStyle-Name"],
                                                "format": s["ric-EventTriggerFormat-Type"]})
        out["l2_parameters"] = _params(et.get("ran-L2Parameters-List"))
        out["ue_identification_parameters"] = _params(et.get("ran-UEIdentificationParameters-List"))
        out["cell_identification_parameters"] = _params(et.get("ran-CellIdentificationParameters-List"))
        out["call_process_types"] = [{
            "id": c["callProcessType-ID"], "name": c["callProcessType-Name"],
            "breakpoints": [{"id": b["callProcessBreakpoint-ID"], "name": b["callProcessBreakpoint-Name"],
                             "params": _params(b.get("ran-CallProcessBreakpointParameters-List"))}
                            for b in c["callProcessBreakpoints-List"]]}
            for c in et.get("ran-CallProcessTypes-List") or []]
    rep = v.get("ranFunctionDefinition-Report")
    if rep:
        for s in rep["ric-ReportStyle-List"]:
            out["report_styles"].append({
                "type": s["ric-ReportStyle-Type"], "name": s["ric-ReportStyle-Name"],
                "event_trigger_style": s["ric-SupportedEventTriggerStyle-Type"],
                "action_format": s["ric-ReportActionFormat-Type"],
                "header_format": s["ric-IndicationHeaderFormat-Type"],
                "message_format": s["ric-IndicationMessageFormat-Type"],
                "params": _params(s.get("ran-ReportParameters-List"))})
    ins = v.get("ranFunctionDefinition-Insert")
    if ins:
        for s in ins["ric-InsertStyle-List"]:
            out["insert_styles"].append({
                "type": s["ric-InsertStyle-Type"], "name": s["ric-InsertStyle-Name"],
                "event_trigger_style": s["ric-SupportedEventTriggerStyle-Type"],
                "action_format": s["ric-ActionDefinitionFormat-Type"],
                "header_format": s["ric-IndicationHeaderFormat-Type"],
                "message_format": s["ric-IndicationMessageFormat-Type"],
                "call_process_id_format": s["ric-CallProcessIDFormat-Type"],
                "indications": [{"id": i["ric-InsertIndication-ID"], "name": i["ric-InsertIndication-Name"],
                                 "params": _params(i.get("ran-InsertIndicationParameters-List"))}
                                for i in s.get("ric-InsertIndication-List") or []]})
    ctl = v.get("ranFunctionDefinition-Control")
    if ctl:
        for s in ctl["ric-ControlStyle-List"]:
            out["control_styles"].append({
                "type": s["ric-ControlStyle-Type"], "name": s["ric-ControlStyle-Name"],
                "header_format": s["ric-ControlHeaderFormat-Type"],
                "message_format": s["ric-ControlMessageFormat-Type"],
                "call_process_id_format": s.get("ric-CallProcessIDFormat-Type"),
                "outcome_format": s["ric-ControlOutcomeFormat-Type"],
                "actions": [{"id": a["ric-ControlAction-ID"], "name": a["ric-ControlAction-Name"],
                             "params": _params(a.get("ran-ControlActionParameters-List"))}
                            for a in s.get("ric-ControlAction-List") or []],
                "outcome_params": _params(s.get("ran-ControlOutcomeParameters-List"))})
    pol = v.get("ranFunctionDefinition-Policy")
    if pol:
        for s in pol["ric-PolicyStyle-List"]:
            out["policy_styles"].append({
                "type": s["ric-PolicyStyle-Type"], "name": s["ric-PolicyStyle-Name"],
                "event_trigger_style": s.get("ric-SupportedEventTriggerStyle-Type"),
                "actions": [{"id": a.get("ric-PolicyAction-ID"), "name": a.get("ric-PolicyAction-Name"),
                             "action_format": a.get("ric-ActionDefinitionFormat-Type"),
                             "params": _params(a.get("ran-PolicyActionParameters-List")),
                             "condition_params": _params(a.get("ran-PolicyConditionParameters-List"))}
                            for a in s.get("ric-PolicyAction-List") or []]})
    return to_jsonable(out)


def ran_function_definition(name: dict, *, event_trigger_styles=(), report_styles=(),
                            control_styles=(), ue_id_params=(), raw_extra: dict | None = None) -> bytes:
    """Encode an E2SM-RC RAN function definition (E2 node side).

    Styles are given in the same JSON shape produced by :func:`decode_ran_function`.
    """
    def enc_params(ps):
        res = []
        for p in ps:
            it = {"ranParameter-ID": p["id"], "ranParameter-name": p["name"]}
            d = p.get("definition")
            if d:
                choice = "choiceSTRUCTURE" if d["kind"] == "structure" else "choiceLIST"
                key = "ranParameter-STRUCTURE" if d["kind"] == "structure" else "ranParameter-List"
                it["ranParameter-Definition"] = {"ranParameter-Definition-Choice": (
                    choice, {key: enc_params(d["params"])})}
            res.append(it)
        return res

    v: dict[str, Any] = {"ranFunction-Name": name}
    if event_trigger_styles:
        et: dict[str, Any] = {"ric-EventTriggerStyle-List": [
            {"ric-EventTriggerStyle-Type": s["type"], "ric-EventTriggerStyle-Name": s["name"],
             "ric-EventTriggerFormat-Type": s["format"]} for s in event_trigger_styles]}
        if ue_id_params:
            et["ran-UEIdentificationParameters-List"] = enc_params(ue_id_params)
        v["ranFunctionDefinition-EventTrigger"] = et
    if report_styles:
        rs = []
        for s in report_styles:
            it = {"ric-ReportStyle-Type": s["type"], "ric-ReportStyle-Name": s["name"],
                  "ric-SupportedEventTriggerStyle-Type": s["event_trigger_style"],
                  "ric-ReportActionFormat-Type": s["action_format"],
                  "ric-IndicationHeaderFormat-Type": s["header_format"],
                  "ric-IndicationMessageFormat-Type": s["message_format"]}
            if s.get("params"):
                it["ran-ReportParameters-List"] = enc_params(s["params"])
            rs.append(it)
        v["ranFunctionDefinition-Report"] = {"ric-ReportStyle-List": rs}
    if control_styles:
        cs = []
        for s in control_styles:
            it = {"ric-ControlStyle-Type": s["type"], "ric-ControlStyle-Name": s["name"],
                  "ric-ControlHeaderFormat-Type": s.get("header_format", 1),
                  "ric-ControlMessageFormat-Type": s.get("message_format", 1),
                  "ric-ControlOutcomeFormat-Type": s.get("outcome_format", 1)}
            if s.get("actions"):
                acts = []
                for a in s["actions"]:
                    ai = {"ric-ControlAction-ID": a["id"], "ric-ControlAction-Name": a["name"]}
                    if a.get("params"):
                        ai["ran-ControlActionParameters-List"] = enc_params(a["params"])
                    acts.append(ai)
                it["ric-ControlAction-List"] = acts
            if s.get("outcome_params"):
                it["ran-ControlOutcomeParameters-List"] = enc_params(s["outcome_params"])
            cs.append(it)
        v["ranFunctionDefinition-Control"] = {"ric-ControlStyle-List": cs}
    if raw_extra:
        v.update(raw_extra)
    return aper_encode(RANFunctionDefinition, v)
