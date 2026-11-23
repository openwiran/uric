"""Helpers for the E2SM-COMMON-IEs shared by all service models (UEID, CGI ...)."""

from __future__ import annotations

import time
from typing import Any

from .asn import to_jsonable
from .e2ap import bytes_to_plmn, plmn_to_bytes

NTP_EPOCH_OFFSET = 2208988800  # seconds between 1900-01-01 and 1970-01-01


def ntp_timestamp(t: float | None = None) -> bytes:
    """8-octet NTP timestamp (used by E2SM-KPM TimeStamp)."""
    if t is None:
        t = time.time()
    sec = int(t) + NTP_EPOCH_OFFSET
    frac = int((t - int(t)) * (1 << 32)) & 0xFFFFFFFF
    return sec.to_bytes(4, "big") + frac.to_bytes(4, "big")


def parse_ntp_timestamp(b: bytes) -> float:
    if len(b) != 8:
        return 0.0
    sec = int.from_bytes(b[:4], "big")
    frac = int.from_bytes(b[4:], "big")
    if sec > NTP_EPOCH_OFFSET:
        return sec - NTP_EPOCH_OFFSET + frac / (1 << 32)
    # Some implementations put a UNIX timestamp (or ms) here instead of NTP
    return float(int.from_bytes(b, "big"))


# --------------------------------------------------------------------------
# UE identifiers
# --------------------------------------------------------------------------

def guami(plmn: str = "00101", region: int = 1, set_id: int = 1, pointer: int = 0) -> dict:
    return {"pLMNIdentity": plmn_to_bytes(plmn), "aMFRegionID": (region, 8),
            "aMFSetID": (set_id, 10), "aMFPointer": (pointer, 6)}


def gnb_ueid(amf_ue_ngap_id: int, plmn: str = "00101", ran_ueid: int | None = None,
             cu_f1ap_id: int | None = None, region: int = 1, set_id: int = 1,
             pointer: int = 0) -> tuple:
    """Build a ``gNB-UEID`` UEID CHOICE."""
    body: dict[str, Any] = {"amf-UE-NGAP-ID": amf_ue_ngap_id,
                            "guami": guami(plmn, region, set_id, pointer)}
    if cu_f1ap_id is not None:
        body["gNB-CU-UE-F1AP-ID-List"] = [{"gNB-CU-UE-F1AP-ID": cu_f1ap_id}]
    if ran_ueid is not None:
        body["ran-UEID"] = ran_ueid.to_bytes(8, "big")
    return ("gNB-UEID", body)


def gnb_du_ueid(cu_f1ap_id: int, ran_ueid: int | None = None) -> tuple:
    body: dict[str, Any] = {"gNB-CU-UE-F1AP-ID": cu_f1ap_id}
    if ran_ueid is not None:
        body["ran-UEID"] = ran_ueid.to_bytes(8, "big")
    return ("gNB-DU-UEID", body)


_UE_PRIMARY = {
    "gNB-UEID": "amf-UE-NGAP-ID",
    "ng-eNB-UEID": "amf-UE-NGAP-ID",
    "gNB-DU-UEID": "gNB-CU-UE-F1AP-ID",
    "gNB-CU-UP-UEID": "gNB-CU-CP-UE-E1AP-ID",
    "ng-eNB-DU-UEID": "ng-eNB-CU-UE-W1AP-ID",
    "en-gNB-UEID": "m-eNB-UE-X2AP-ID",
    "eNB-UEID": "mME-UE-S1AP-ID",
}


def ue_key(ueid) -> str:
    """Canonical, hashable string for a UEID CHOICE value.

    ``("gNB-UEID", {"amf-UE-NGAP-ID": 7, ...})`` -> ``"gNB-UEID:7"``.
    """
    try:
        kind, body = ueid
        prim = _UE_PRIMARY.get(kind)
        if prim and prim in body:
            key = f"{kind}:{body[prim]}"
        else:
            key = f"{kind}:{abs(hash(repr(body))) & 0xFFFFFFFF:x}"
        return key
    except Exception:
        return f"ue:{abs(hash(repr(ueid))) & 0xFFFFFFFF:x}"


def describe_ueid(ueid) -> dict:
    """Flat, JSON-friendly summary of a UEID."""
    kind, body = ueid
    out: dict[str, Any] = {"type": kind}
    for k, v in body.items():
        if k in ("guami", "gUMMEI"):
            out["plmn"] = bytes_to_plmn(v.get("pLMNIdentity", v.get("pLMN-Identity", b"")))
            continue
        if k == "ran-UEID" and isinstance(v, (bytes, bytearray)):
            out["ran_ueid"] = int.from_bytes(v, "big")
            continue
        if isinstance(v, list) and v and isinstance(v[0], dict) and len(v[0]) == 1:
            out[k] = [next(iter(x.values())) for x in v]
            continue
        out[k] = to_jsonable(v)
    return out


# --------------------------------------------------------------------------
# Cells
# --------------------------------------------------------------------------

def nr_cgi(plmn: str, nci: int) -> tuple:
    """Build a CGI CHOICE for an NR cell (36-bit NR cell identity)."""
    return ("nR-CGI", {"pLMNIdentity": plmn_to_bytes(plmn), "nRCellIdentity": (nci, 36)})


def eutra_cgi(plmn: str, eci: int) -> tuple:
    return ("eUTRA-CGI", {"pLMNIdentity": plmn_to_bytes(plmn), "eUTRACellIdentity": (eci, 28)})


def describe_cgi(cgi) -> dict:
    kind, body = cgi
    if kind == "nR-CGI":
        return {"type": "nr", "plmn": bytes_to_plmn(body["pLMNIdentity"]),
                "nci": body["nRCellIdentity"][0]}
    return {"type": "eutra", "plmn": bytes_to_plmn(body["pLMNIdentity"]),
            "eci": body["eUTRACellIdentity"][0]}


def cgi_key(cgi) -> str:
    d = describe_cgi(cgi)
    return f"{d['type']}:{d['plmn']}:{d.get('nci', d.get('eci')):x}"


def ran_function_name(body: dict) -> dict:
    return {"short_name": body.get("ranFunction-ShortName"),
            "oid": body.get("ranFunction-E2SM-OID"),
            "description": body.get("ranFunction-Description"),
            "instance": body.get("ranFunction-Instance")}


def encode_ueid(ueid) -> bytes:
    """APER encode a UEID CHOICE (used to carry UE identities over the xApp interface)."""
    from ._gen import e2sm_kpm_asn as _K
    from .asn import aper_encode
    return aper_encode(_K.E2SM_COMMON_IEs.UEID, ueid)


def decode_ueid(data: bytes):
    from ._gen import e2sm_kpm_asn as _K
    from .asn import aper_decode
    return aper_decode(_K.E2SM_COMMON_IEs.UEID, data)
