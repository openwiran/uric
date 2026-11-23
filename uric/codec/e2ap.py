"""E2AP codec (O-RAN.WG3.E2AP) built on top of the pycrate E2AP module.

Messages are represented as :class:`E2APMessage` objects carrying the message
name (e.g. ``"E2setupRequest"``) and a dict of IEs keyed by the IE identifier
name without its ``id-`` prefix (e.g. ``"GlobalE2node-ID"``, ``"RICrequestID"``).

ProtocolIE single containers used in lists (``RANfunctions-List``,
``RICaction-ToBeSetup-List`` ...) are wrapped/unwrapped automatically, so
callers only deal with plain item values::

    encode("E2setupResponse", {
        "TransactionID": 1,
        "GlobalRIC-ID": {"pLMN-Identity": b"\\x00\\xf1\\x10", "ric-ID": (1, 20)},
        "RANfunctionsAccepted": [{"ranFunctionID": 2, "ranFunctionRevision": 1}],
    })
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pycrate_asn1dir import E2AP as _E2AP

from .asn import ASN_LOCK, CodecError

_PDU = _E2AP.E2AP_PDU_Descriptions.E2AP_PDU
_CONTENTS = _E2AP.E2AP_PDU_Contents
_CONSTS = _E2AP.E2AP_Constants

INITIATING = "initiatingMessage"
SUCCESSFUL = "successfulOutcome"
UNSUCCESSFUL = "unsuccessfulOutcome"

#: message name -> (procedure code, PDU kind, procedure criticality)
PROCEDURES: dict[str, tuple[int, str, str]] = {}
_BY_CODE: dict[tuple[int, str], str] = {}
_MSG_TYPES: dict[str, Any] = {}

for _proc in _E2AP.E2AP_PDU_Descriptions.E2AP_ELEMENTARY_PROCEDURES._val.root:
    for _key, _kind in (("InitiatingMessage", INITIATING),
                        ("SuccessfulOutcome", SUCCESSFUL),
                        ("UnsuccessfulOutcome", UNSUCCESSFUL)):
        _t = _proc.get(_key)
        if _t is None:
            continue
        PROCEDURES[_t._typeref.called[1]] = (_proc["procedureCode"], _kind, _proc["criticality"])
for _name, (_code, _kind, _crit) in PROCEDURES.items():
    _BY_CODE[(_code, _kind)] = _name
    _MSG_TYPES[_name] = getattr(_CONTENTS, _name.replace("-", "_"))

#: IE id name ("TransactionID") <-> integer id
IE_ID: dict[str, int] = {}
IE_NAME: dict[int, str] = {}
#: procedure code name ("E2setup") <-> integer code
PROC_CODE: dict[str, int] = {}
for _n in _CONSTS._obj_:
    if not _n.startswith("id-"):
        continue
    _c = getattr(_CONSTS, _n.replace("-", "_"))
    _ref = getattr(_c, "_typeref", None)
    _called = getattr(_ref, "called", (None, None))[1] if _ref is not None else None
    if _called == "ProtocolIE-ID":
        IE_ID[_n[3:]] = _c._val
        IE_NAME[_c._val] = _n[3:]
    elif _called == "ProcedureCode":
        PROC_CODE[_n[3:]] = _c._val


def _refname(t) -> str:
    ref = getattr(t, "_typeref", None)
    called = getattr(ref, "called", None)
    if called:
        return called[1]
    return t.TYPE


def _ie_table(msg_type) -> list[dict]:
    return msg_type._cont["protocolIEs"]._cont._cont["id"]._const_tab._val.root


def _is_ie_container(t) -> bool:
    return t.TYPE == "SEQUENCE" and list(t._cont.keys()) == ["id", "criticality", "value"]


def _wrap(t, v):
    """Recursively wrap plain values into ProtocolIE single containers."""
    T = t.TYPE
    if T in ("SEQUENCE", "SET") and isinstance(v, dict):
        out = {}
        for k, x in v.items():
            if k not in t._cont:
                raise CodecError(f"unknown component {k!r} in {t._name}")
            out[k] = _wrap(t._cont[k], x)
        return out
    if T in ("SEQUENCE OF", "SET OF") and isinstance(v, (list, tuple)):
        el = t._cont
        if _is_ie_container(el):
            tab = el._cont["id"]._const_tab._val.root
            ent = tab[0]
            res = []
            for item in v:
                if isinstance(item, dict) and set(item) == {"id", "criticality", "value"}:
                    res.append(item)
                else:
                    res.append({"id": ent["id"], "criticality": ent["criticality"],
                                "value": (_refname(ent["Value"]), _wrap(ent["Value"], item))})
            return res
        return [_wrap(el, x) for x in v]
    if T == "CHOICE" and isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str):
        return (v[0], _wrap(t._cont[v[0]], v[1]))
    return v


def _unwrap(v):
    """Structurally remove ProtocolIE single containers from a decoded value."""
    if isinstance(v, dict):
        if set(v) == {"id", "criticality", "value"} and isinstance(v["value"], tuple):
            return _unwrap(v["value"][1])
        return {k: _unwrap(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_unwrap(x) for x in v]
    if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str):
        return (v[0], _unwrap(v[1]))
    return v


@dataclass
class E2APMessage:
    name: str
    ies: dict[str, Any] = field(default_factory=dict)

    @property
    def procedure_code(self) -> int:
        return PROCEDURES[self.name][0]

    @property
    def kind(self) -> str:
        return PROCEDURES[self.name][1]

    def get(self, ie: str, default=None):
        return self.ies.get(ie, default)

    def __getitem__(self, ie: str):
        return self.ies[ie]

    def __contains__(self, ie: str) -> bool:
        return ie in self.ies


def encode(name: str, ies: dict[str, Any]) -> bytes:
    """Encode an E2AP message (APER)."""
    try:
        code, kind, crit = PROCEDURES[name]
    except KeyError:
        raise CodecError(f"unknown E2AP message {name!r}") from None
    msg_type = _MSG_TYPES[name]
    table = _ie_table(msg_type)
    order = {e["id"]: i for i, e in enumerate(table)}
    by_id = {e["id"]: e for e in table}
    lst = []
    with ASN_LOCK:
        for ie_name, val in ies.items():
            if val is None:
                continue
            try:
                iid = IE_ID[ie_name]
                ent = by_id[iid]
            except KeyError:
                raise CodecError(f"IE {ie_name!r} not allowed in {name}") from None
            lst.append({"id": iid, "criticality": ent["criticality"],
                        "value": (_refname(ent["Value"]), _wrap(ent["Value"], val))})
        lst.sort(key=lambda x: order[x["id"]])
        try:
            _PDU.set_val((kind, {"procedureCode": code, "criticality": crit,
                                 "value": (name, {"protocolIEs": lst})}))
            return _PDU.to_aper()
        except Exception as exc:
            raise CodecError(f"cannot encode {name}: {exc}") from exc


def decode(data: bytes) -> E2APMessage:
    """Decode an APER E2AP PDU."""
    with ASN_LOCK:
        try:
            _PDU.from_aper(data)
            kind, body = _PDU.get_val()
        except Exception as exc:
            raise CodecError(f"cannot decode E2AP PDU: {exc}") from exc
    name, content = body["value"]
    ies = {}
    for ie in content.get("protocolIEs", []):
        ies[IE_NAME.get(ie["id"], str(ie["id"]))] = _unwrap(ie["value"][1])
    return E2APMessage(name, ies)


def peek(data: bytes) -> tuple[str, str, int]:
    """Return (kind, message name, procedure code) without unwrapping the IEs."""
    m = decode(data)
    return m.kind, m.name, m.procedure_code


# --------------------------------------------------------------------------
# Common IE helpers
# --------------------------------------------------------------------------

def plmn_to_bytes(plmn: str) -> bytes:
    """Encode a PLMN string ("00101" or "001010") into 3 TBCD octets."""
    plmn = str(plmn)
    if len(plmn) not in (5, 6) or not plmn.isdigit():
        raise ValueError(f"invalid PLMN {plmn!r}")
    mcc, mnc = plmn[:3], plmn[3:]
    d = [int(c) for c in mcc]
    m = [int(c) for c in mnc]
    mnc3 = m[2] if len(m) == 3 else 0xF
    return bytes([(d[1] << 4) | d[0], (mnc3 << 4) | d[2], (m[1] << 4) | m[0]])


def bytes_to_plmn(b: bytes) -> str:
    if len(b) != 3:
        return b.hex()
    mcc = f"{b[0] & 0xF}{b[0] >> 4}{b[1] & 0xF}"
    mnc3 = b[1] >> 4
    mnc = f"{b[2] & 0xF}{b[2] >> 4}" + ("" if mnc3 == 0xF else str(mnc3))
    return mcc + mnc


def global_ric_id(plmn: str, ric_id: int) -> dict:
    return {"pLMN-Identity": plmn_to_bytes(plmn), "ric-ID": (ric_id & 0xFFFFF, 20)}


def ric_request_id(requestor: int, instance: int) -> dict:
    return {"ricRequestorID": requestor, "ricInstanceID": instance}


def cause(group: str = "misc", value: str = "unspecified") -> tuple:
    """Build a Cause CHOICE (group: ricRequest/ricService/e2Node/transport/protocol/misc)."""
    return (group, value)


def cause_str(c) -> str:
    if isinstance(c, tuple) and len(c) == 2:
        return f"{c[0]}:{c[1]}"
    return str(c)


def _bits_hex(b) -> str:
    if isinstance(b, tuple):
        val, n = b
        return f"{val:0{(n + 3) // 4}x}"
    return str(b)


def describe_global_node_id(gid) -> dict:
    """Return a flat, human-readable description of a GlobalE2node-ID."""
    ntype, body = gid
    info: dict[str, Any] = {"type": ntype}
    if ntype == "gNB":
        g = body["global-gNB-ID"]
        info["plmn"] = bytes_to_plmn(g["plmn-id"])
        info["nb_id"] = g["gnb-id"][1][0]
        info["nb_id_len"] = g["gnb-id"][1][1]
        if "gNB-CU-UP-ID" in body:
            info["cu_up_id"] = body["gNB-CU-UP-ID"]
        if "gNB-DU-ID" in body:
            info["du_id"] = body["gNB-DU-ID"]
    elif ntype == "en-gNB":
        g = body["global-gNB-ID"]
        info["plmn"] = bytes_to_plmn(g["pLMN-Identity"])
        info["nb_id"] = g["gNB-ID"][1][0]
        info["nb_id_len"] = g["gNB-ID"][1][1]
    elif ntype == "ng-eNB":
        g = body["global-ng-eNB-ID"]
        info["plmn"] = bytes_to_plmn(g["plmn-id"])
        info["enb_kind"] = g["enb-id"][0]
        info["nb_id"] = g["enb-id"][1][0]
        info["nb_id_len"] = g["enb-id"][1][1]
        if "ngENB-DU-ID" in body:
            info["du_id"] = body["ngENB-DU-ID"]
    elif ntype == "eNB":
        g = body["global-eNB-ID"]
        info["plmn"] = bytes_to_plmn(g["pLMN-Identity"])
        info["enb_kind"] = g["eNB-ID"][0]
        info["nb_id"] = g["eNB-ID"][1][0]
        info["nb_id_len"] = g["eNB-ID"][1][1]
    return info


def global_node_key(gid) -> str:
    """Stable string key for an E2 node, e.g. ``gnb_00101_0000e00`` or ``gnb_00101_e00_du1``."""
    d = describe_global_node_id(gid)
    t = d["type"].lower().replace("-", "")
    key = f"{t}_{d.get('plmn', 'x')}_{d['nb_id']:x}"
    if "cu_up_id" in d:
        key += f"_cuup{d['cu_up_id']}"
    if "du_id" in d:
        key += f"_du{d['du_id']}"
    return key


def gnb_global_node_id(plmn: str, gnb_id: int, gnb_id_len: int = 32,
                       du_id: int | None = None, cu_up_id: int | None = None) -> tuple:
    """Build a gNB GlobalE2node-ID (used by the simulator and by tests)."""
    body: dict[str, Any] = {"global-gNB-ID": {"plmn-id": plmn_to_bytes(plmn),
                                              "gnb-id": ("gnb-ID", (gnb_id, gnb_id_len))}}
    if cu_up_id is not None:
        body["gNB-CU-UP-ID"] = cu_up_id
    if du_id is not None:
        body["gNB-DU-ID"] = du_id
    return ("gNB", body)
