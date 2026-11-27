"""OSC / srsRAN style xApp API (``xAppBase``, ``e2sm_kpm``, ``e2sm_rc``).

This mirrors the python xApp framework of the srsRAN ``oran-sc-ric`` project
(``xApps/python/lib``), so xApps written for it port with minimal changes::

    from uric.xapp import xAppBase

    class MyXapp(xAppBase):
        def __init__(self, config, ric_address=None):
            super().__init__(config, ric_address)

        def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
            hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
            meas = self.e2sm_kpm.extract_meas_data(indication_msg)
            print(hdr["colletStartTime"], meas["measData"])

        @xAppBase.start_function
        def start(self, e2_node_id, metric_names):
            self.e2sm_kpm.subscribe_report_service_style_1(e2_node_id, 1000, metric_names, 1000,
                                                           self.my_subscription_callback)

    if __name__ == "__main__":
        xapp = MyXapp("")
        signal.signal(signal.SIGINT, xapp.signal_handler)
        xapp.start("gnb_00101_e00", ["DRB.UEThpDl"])

Differences with the OSC framework:

* there is no RMR / subscription manager: a standalone xApp connects to the
  RIC's xApp interface (``ric_address`` argument, ``URIC_RIC`` environment
  variable, default ``127.0.0.1:4560``), so the constructor takes no
  ``http_server_port`` / ``rmr_port`` / ``rmr_flags`` arguments:
  ``xAppBase(config=None, ric_address=None)``;
* the same class can be installed as a package (``uric.xapps`` entry point)
  and run *inside* the RIC; ``start()`` is then called with the
  ``start_args`` of its entry in the RIC configuration file (next to
  ``config``; ``config["start_args"]`` is also accepted);
* RAN function IDs are discovered from the E2 node capabilities (use
  ``set_ran_func_id`` to force one);
* UE identifiers may be a UE key (``"gNB-UEID:7"``), a UEID CHOICE tuple or an
  integer (looked up in the RIC UE registry);
* control requests wait for the acknowledgement and return
  ``{"success", "cause", "outcome"}``;
* KPM decoding is complete (all metrics of format 2, every UE id type).
"""

from __future__ import annotations

import datetime
import functools
import json
import logging
import os
import queue
import sys
import threading
from enum import Enum
from typing import Any, Callable, Optional, Sequence

from ..codec import kpm as K
from ..codec import rc as R
from ..codec.asn import aper_decode
from ..codec.e2ap import plmn_to_bytes  # noqa: F401  (re-export convenience)
from ..codec.e2sm import NTP_EPOCH_OFFSET, gnb_du_ueid, gnb_ueid, nr_cgi, ue_key
from ..core.ric import RICError
from .links import Link, RemoteLink

log = logging.getLogger("uric.xapp")

_embed = threading.local()


class e2sm_types(Enum):
    E2SM_UNKNOWN = 0
    E2SM_KPM = 1
    E2SM_RC = 2


class SubscriptionWrapper:
    def __init__(self):
        self.e2sm_type = e2sm_types.E2SM_UNKNOWN
        self.subscription_id: Optional[str] = None
        self.e2_node_id: Optional[str] = None
        self.callback_func: Optional[Callable] = None


def ntp_ts_to_datetime(ntp_timestamp: bytes) -> datetime.datetime:
    sec = int.from_bytes(ntp_timestamp[:4], "big")
    frac = int.from_bytes(ntp_timestamp[4:8], "big")
    unix = sec - NTP_EPOCH_OFFSET + frac / (1 << 32) if sec > NTP_EPOCH_OFFSET else float(
        int.from_bytes(ntp_timestamp, "big"))
    return datetime.datetime.fromtimestamp(unix, datetime.timezone.utc).replace(tzinfo=None)


class xAppBase:
    """Base class of OSC style xApps (see the module documentation)."""

    #: optional explicit name (defaults to the class name)
    name: Optional[str] = None
    #: optional version and one-line description, shown in the RIC dashboard
    version: Optional[str] = None
    description: Optional[str] = None

    def __init__(self, config: Any = None, ric_address: Optional[str] = None, *, name: Optional[str] = None):
        """``config``: dict or path to a JSON file; ``ric_address``: ``host:port`` of
        the RIC xApp interface (standalone mode only)."""
        self.config = self._load_config(config)
        self.xapp_name = name or self.config.get("name") or type(self).name or type(self).__name__
        self.log = logging.getLogger(f"xapp.{self.xapp_name}")
        self.running = False
        self.xapp_thread: Optional[threading.Thread] = None
        self.my_subscriptions: dict[str, SubscriptionWrapper] = {}
        self._inbox: queue.Queue = queue.Queue()
        self._stopped = threading.Event()
        link = getattr(_embed, "link", None)
        self._link: Link = link if link is not None else RemoteLink(
            ric_address or self.config.get("ric_address"), self.xapp_name,
            version=type(self).version, description=type(self).description)
        self._link.on_indication = self._inbox.put
        self.e2sm_kpm = e2sm_kpm_module(self)
        self.e2sm_rc = e2sm_rc_module(self)

    @staticmethod
    def _load_config(config) -> dict:
        if isinstance(config, dict):
            return dict(config)
        if isinstance(config, str) and config and os.path.isfile(config):
            with open(config) as f:
                return json.load(f)
        return {}

    @property
    def embedded(self) -> bool:
        """True when running inside the RIC process."""
        return self._link.embedded

    # ------------------------------------------------------------------ lifecycle

    @classmethod
    def start_function(cls, fun):
        """Decorator for the xApp entry point.

        The decorated function runs in a background thread while the calling
        thread dispatches RIC indications to subscription callbacks until
        :meth:`stop` is called.
        """
        @functools.wraps(fun)
        def wrapper(self, *args, **kwargs):
            self.running = True
            self.xapp_thread = threading.Thread(target=self._guard, args=(fun, self, *args), kwargs=kwargs,
                                                name=f"xapp-{self.xapp_name}", daemon=True)
            self.xapp_thread.start()
            self._run()
        wrapper._uric_start_function = True  # type: ignore[attr-defined]
        return wrapper

    def _guard(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception:
            self.log.exception("xApp start function failed")

    def _run(self) -> None:
        while self.running:
            try:
                item = self._inbox.get(timeout=0.1)
            except queue.Empty:
                continue
            self._dispatch(item)

    def _dispatch(self, ind: dict) -> None:
        sub = self.my_subscriptions.get(ind["sub"])
        if sub is None or sub.callback_func is None:
            return
        hdr, msg = ind["header"], ind["message"]
        try:
            if sub.e2sm_type == e2sm_types.E2SM_KPM:
                hdr, msg = self.e2sm_kpm.unpack_ric_indication(hdr, msg)
            elif sub.e2sm_type == e2sm_types.E2SM_RC:
                hdr, msg = self.e2sm_rc.unpack_ric_indication(hdr, msg)
            sub.callback_func(ind["node"], ind["sub"], hdr, msg)
        except Exception:
            self.log.exception("subscription callback failed")

    def stop(self) -> None:
        """Delete every subscription and stop the xApp."""
        try:
            self.unsubscribe_all()
        except Exception as exc:
            self.log.warning("unsubscribe failed: %s", exc)
        self.running = False
        self._stopped.set()
        t = self.xapp_thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5)
        if not self.embedded:
            self._link.close()
            sys.exit(0)

    def signal_handler(self, sig, frame) -> None:
        self.stop()

    def sleep(self, seconds: float) -> bool:
        """Sleep, waking up early when the xApp is stopped. Returns ``self.running``."""
        self._stopped.wait(seconds)
        return self.running

    # ------------------------------------------------------------------ RIC access

    def subscribe(self, e2_node_id: str, ran_function_id: int, event_trigger_def: bytes, action_def: bytes,
                  indication_callback: Callable, e2sm_type: e2sm_types = e2sm_types.E2SM_UNKNOWN,
                  description: Optional[dict] = None) -> str:
        """Generic RIC subscription with one REPORT action (id 1). Returns the subscription id."""
        res = self._link.subscribe(e2_node_id, ran_function_id, event_trigger_def,
                                   [{"id": 1, "type": "report", "definition": action_def}], description)
        w = SubscriptionWrapper()
        w.e2sm_type, w.subscription_id, w.e2_node_id, w.callback_func = (
            e2sm_type, res["sub"], e2_node_id, indication_callback)
        self.my_subscriptions[res["sub"]] = w
        self.log.info("subscribed %s on %s", res["sub"], e2_node_id)
        return res["sub"]

    def unsubscribe(self, subscription_id: str) -> None:
        self._link.unsubscribe(subscription_id)
        self.my_subscriptions.pop(subscription_id, None)

    def unsubscribe_all(self) -> None:
        for sid in list(self.my_subscriptions):
            try:
                self.unsubscribe(sid)
            except RICError as exc:
                self.log.warning("unsubscribe %s: %s", sid, exc)
                self.my_subscriptions.pop(sid, None)

    def send_control_request(self, e2_node_id: str, ran_function_id: int, control_header: bytes,
                             control_msg: bytes, ack_request: int = 1,
                             call_process_id: Optional[bytes] = None) -> dict:
        """Raw RIC control request; returns ``{"success", "cause", "outcome"}``."""
        return self._link.control(e2_node_id, ran_function_id, control_header, control_msg,
                                  ack=bool(ack_request), call_process_id=call_process_id)

    def get_e2_nodes(self) -> list[dict]:
        """Connected E2 nodes (with RAN functions and decoded capabilities)."""
        return self._link.nodes()

    def get_e2_node_ids(self) -> list[str]:
        return [n["id"] for n in self.get_e2_nodes()]

    def get_ues(self, e2_node_id: Optional[str] = None) -> list[dict]:
        """UEs known by the RIC (``key``, ``node_id``, ``cell``, ``metrics``, ``ue_id_raw`` ...)."""
        return self._link.ues(e2_node_id)

    def set_status(self, **status) -> None:
        """Publish status information shown in the RIC dashboard."""
        self._link.status(status)

    # helpers used by the E2SM modules ----------------------------------------------

    def _ran_function(self, e2_node_id: str, sm: str) -> dict:
        for n in self.get_e2_nodes():
            if n["id"] == e2_node_id:
                for f in n["ran_functions"]:
                    if f["service_model"] == sm:
                        return f
                raise RICError(f"E2 node {e2_node_id} does not support E2SM-{sm}")
        raise RICError(f"E2 node {e2_node_id} not connected")

    def _resolve_ue(self, e2_node_id: str, ue_id: Any):
        if isinstance(ue_id, tuple):
            return ue_id
        ues = self.get_ues()
        if isinstance(ue_id, str):
            for u in ues:
                if u["key"] == ue_id:
                    return u["ue_id_raw"]
            raise RICError(f"unknown UE {ue_id!r}")
        if isinstance(ue_id, int):
            for scope in ([u for u in ues if u["node_id"] == e2_node_id], ues):
                for u in scope:
                    if u["key"].rsplit(":", 1)[-1] == str(ue_id):
                        return u["ue_id_raw"]
            return gnb_du_ueid(ue_id)  # same fallback as the OSC framework
        raise RICError(f"invalid UE identifier {ue_id!r}")


class e2sm_kpm_module:
    """E2SM-KPM helper (``self.e2sm_kpm``)."""

    def __init__(self, parent: xAppBase):
        self.parent = parent
        self.ran_func_id: Optional[int] = None

    def set_ran_func_id(self, ran_func_id: int) -> None:
        self.ran_func_id = ran_func_id

    def _rf(self, e2_node_id: str) -> int:
        return self.ran_func_id if self.ran_func_id is not None else \
            self.parent._ran_function(e2_node_id, "KPM")["id"]

    def _subscribe(self, e2_node_id, reportingPeriod, adef, cb, style, metric_names):
        return self.parent.subscribe(e2_node_id, self._rf(e2_node_id), K.event_trigger(reportingPeriod), adef,
                                     cb, e2sm_types.E2SM_KPM,
                                     {"sm": "KPM", "style": style, "period_ms": reportingPeriod,
                                      "metrics": [str(m) for m in metric_names]})

    # capabilities -----------------------------------------------------------

    def get_capabilities(self, e2_node_id: str) -> dict:
        return self.parent._ran_function(e2_node_id, "KPM").get("capabilities") or {}

    def get_report_styles(self, e2_node_id: str) -> list[int]:
        return [s["type"] for s in self.get_capabilities(e2_node_id).get("report_styles", [])]

    def get_supported_metrics(self, e2_node_id: str, report_style: Optional[int] = None) -> list[str]:
        out: list[str] = []
        for s in self.get_capabilities(e2_node_id).get("report_styles", []):
            if report_style is None or s["type"] == report_style:
                out += [m["name"] for m in s["metrics"] if m["name"] not in out]
        return out

    # subscriptions (same signatures as the OSC framework) -------------------------

    def subscribe_report_service_style_1(self, e2_node_id, reportingPeriod, metric_names, granulPeriod,
                                         indication_callback):
        """E2 node level measurements."""
        return self._subscribe(e2_node_id, reportingPeriod, K.action_format1(metric_names, granulPeriod),
                               indication_callback, 1, metric_names)

    def subscribe_report_service_style_2(self, e2_node_id, reportingPeriod, ue_id, metric_names, granulPeriod,
                                         indication_callback):
        """Measurements of a single UE."""
        ue = self.parent._resolve_ue(e2_node_id, ue_id)
        return self._subscribe(e2_node_id, reportingPeriod, K.action_format2(ue, metric_names, granulPeriod),
                               indication_callback, 2, metric_names)

    def subscribe_report_service_style_3(self, e2_node_id, reportingPeriod, matchingConds, metric_names,
                                         granulPeriod, indication_callback):
        """Condition based UE level measurements; ``matchingConds`` is a list of
        MatchingCondItem dicts (``{"matchingCondChoice": ("testCondInfo", {...})}``)
        applied to every metric."""
        conds = [c["matchingCondChoice"] if isinstance(c, dict) and "matchingCondChoice" in c else c
                 for c in matchingConds]
        return self._subscribe(e2_node_id, reportingPeriod,
                               K.action_format3([(m, conds) for m in metric_names], granulPeriod),
                               indication_callback, 3, metric_names)

    def subscribe_report_service_style_4(self, e2_node_id, reportingPeriod, matchingUeConds, metric_names,
                                         granulPeriod, indication_callback):
        """Measurements of every UE matching ``matchingUeConds``
        (``[{"testCondInfo": {"testType": ("sNSSAI", "true"), "testExpr": "equal",
        "testValue": ("valueInt", 1)}}]``)."""
        conds = [c["testCondInfo"] if isinstance(c, dict) and "testCondInfo" in c else c for c in matchingUeConds]
        return self._subscribe(e2_node_id, reportingPeriod, K.action_format4(conds, metric_names, granulPeriod),
                               indication_callback, 4, metric_names)

    def subscribe_report_service_style_5(self, e2_node_id, reportingPeriod, ue_ids, metric_names, granulPeriod,
                                         indication_callback):
        """Measurements of a list of UEs (padded with a dummy UE when fewer than two)."""
        ues = [self.parent._resolve_ue(e2_node_id, u) for u in ue_ids]
        while len(ues) < 2:
            ues.append(gnb_du_ueid(0))
        return self._subscribe(e2_node_id, reportingPeriod, K.action_format5(ues, metric_names, granulPeriod),
                               indication_callback, 5, metric_names)

    # decoding -----------------------------------------------------------------

    def unpack_ric_indication(self, header: bytes, message: bytes):
        """Decode the indication header (format 1 body) and message (``{"indicationMessage-formats": ..}``)."""
        hdr = aper_decode(K.IndicationHeader, header)["indicationHeader-formats"][1]
        msg = aper_decode(K.IndicationMessage, message)
        return dict(hdr), msg

    def extract_hdr_info(self, indication_hdr: dict) -> dict:
        ts = indication_hdr.get("colletStartTime")
        if isinstance(ts, (bytes, bytearray)):
            indication_hdr["colletStartTime"] = ntp_ts_to_datetime(ts)
        return indication_hdr

    def extract_meas_data(self, indication_msg: dict) -> dict:
        """Return (same layout as the OSC framework):

        * format 1: ``{"measData": {metric: [values]}, "granulPeriod": ms}``
        * format 2: ``{"measData": {...}, "ueMeasData": {ue: {"measData": {...}}}, "matchingCond": [...]}``
        * format 3: ``{"ueMeasData": {ue: {"measData": {...}, "granulPeriod": ms}}}``

        UEs are identified by their uric UE key (e.g. ``"gNB-UEID:7"``).
        """
        fmt, body = indication_msg["indicationMessage-formats"]
        out: dict[str, Any] = {}
        if fmt == "indicationMessage-Format1":
            out["measData"] = K._decode_format1(body)
            if "granulPeriod" in body:
                out["granulPeriod"] = body["granulPeriod"]
        elif fmt == "indicationMessage-Format2":
            names = [K._meas_name(c["measType"]) for c in body["measCondUEidList"]]
            meas: dict[str, list] = {}
            for data in body.get("measData", []):
                for i, item in enumerate(data["measRecord"]):
                    meas.setdefault(names[i] if i < len(names) else f"#{i}", []).append(K._value(item))
            out["measData"] = meas
            ue_meas: dict[str, dict] = {}
            for i, c in enumerate(body["measCondUEidList"]):
                for u in c.get("matchingUEidList") or []:
                    d = ue_meas.setdefault(ue_key(u["ueID"]), {"measData": {}})
                    d["measData"][names[i]] = meas.get(names[i], [])
            out["ueMeasData"] = ue_meas
            out["matchingCond"] = [c["matchingCond"] for c in body["measCondUEidList"]]
            if "granulPeriod" in body:
                out["granulPeriod"] = body["granulPeriod"]
        elif fmt == "indicationMessage-Format3":
            ue_meas = {}
            for item in body["ueMeasReportList"]:
                mr = item["measReport"]
                d: dict[str, Any] = {"measData": K._decode_format1(mr)}
                if "granulPeriod" in mr:
                    d["granulPeriod"] = mr["granulPeriod"]
                ue_meas[ue_key(item["ueID"])] = d
            out["ueMeasData"] = ue_meas
        return out


class e2sm_rc_module:
    """E2SM-RC helper (``self.e2sm_rc``)."""

    def __init__(self, parent: xAppBase):
        self.parent = parent
        self.ran_func_id: Optional[int] = None

    def set_ran_func_id(self, ran_func_id: int) -> None:
        self.ran_func_id = ran_func_id

    def _rf(self, e2_node_id: str) -> int:
        return self.ran_func_id if self.ran_func_id is not None else \
            self.parent._ran_function(e2_node_id, "RC")["id"]

    def get_capabilities(self, e2_node_id: str) -> dict:
        return self.parent._ran_function(e2_node_id, "RC").get("capabilities") or {}

    def get_control_styles(self, e2_node_id: str) -> dict[int, list[int]]:
        """``{style: [action ids]}`` advertised by the node."""
        return {s["type"]: [a["id"] for a in s["actions"]]
                for s in self.get_capabilities(e2_node_id).get("control_styles", [])}

    # generic control ----------------------------------------------------------

    def send_control_request(self, e2_node_id: str, ue_id: Any, control_style: int, control_action_id: int,
                             ran_parameters: dict, ack_request: int = 1) -> dict:
        """Control header/message format 1. ``ran_parameters`` is a ``{id: value}``
        dict (dict = STRUCTURE, list = LIST, :class:`uric.codec.rc.Key` = key flag)."""
        ue = self.parent._resolve_ue(e2_node_id, ue_id)
        return self.parent.send_control_request(
            e2_node_id, self._rf(e2_node_id), R.control_header(ue, control_style, control_action_id),
            R.control_message(ran_parameters), ack_request)

    # OSC framework compatible helpers ------------------------------------------

    def send_control_request_style_2_action_6(self, e2_node_id, ue_id, min_prb_ratio, max_prb_ratio,
                                              dedicated_prb_ratio, ack_request=1, sst=1, sd=None, plmn=None):
        """Slice level PRB quota (ratios in percent, clamped to 0..100)."""
        clamp = lambda v: max(0, min(100, int(v)))  # noqa: E731
        mn, mx, ded = clamp(min_prb_ratio), clamp(max_prb_ratio), clamp(dedicated_prb_ratio)
        if mx < mn:
            self.parent.log.error("max_prb_ratio (%d) must be >= min_prb_ratio (%d)", mx, mn)
            return None
        params = R.slice_prb_quota_params([{"sst": sst, "sd": sd, "min": mn, "max": mx, "dedicated": ded}],
                                          plmn or "00101")
        return self.send_control_request(e2_node_id, ue_id, 2, 6, params, ack_request)

    def send_control_request_style_3_action_1(self, e2_node_id, amf_ue_ngap_id, gnb_cu_ue_f1ap_id, plmn_string,
                                              target_nr_cell_id, ack_request=1):
        """Handover of the UE identified by its AMF UE NGAP ID to an NR cell."""
        try:
            ue = self.parent._resolve_ue(e2_node_id, f"gNB-UEID:{amf_ue_ngap_id}")
        except RICError:
            ue = gnb_ueid(amf_ue_ngap_id, plmn_string, cu_f1ap_id=gnb_cu_ue_f1ap_id)
        return self.send_control_request(e2_node_id, ue, 3, 1,
                                         R.handover_params(nr_cgi(plmn_string, target_nr_cell_id)), ack_request)

    control_slice_level_prb_quota = send_control_request_style_2_action_6
    control_handover = send_control_request_style_3_action_1

    def handover(self, e2_node_id: str, ue_id: Any, target_nr_cell_id: int, plmn: str = "00101",
                 ack_request: int = 1) -> dict:
        """Handover with any UE identifier (key, UEID or int)."""
        return self.send_control_request(e2_node_id, ue_id, 3, 1,
                                         R.handover_params(nr_cgi(plmn, target_nr_cell_id)), ack_request)

    # report service ---------------------------------------------------------------

    def subscribe_report_service_style_4(self, e2_node_id: str, indication_callback: Callable,
                                         ran_param_ids: Sequence[int] = (202,)):
        """UE information reports, triggered on RRC state changes."""
        return self.parent.subscribe(e2_node_id, self._rf(e2_node_id), R.event_trigger_ue_info_change(),
                                     R.action_definition_report(4, list(ran_param_ids)), indication_callback,
                                     e2sm_types.E2SM_RC, {"sm": "RC", "style": 4})

    def unpack_ric_indication(self, header: bytes, message: bytes):
        """Decoded as by :func:`uric.codec.rc.decode_indication` (header, full dict)."""
        d = R.decode_indication(header, message)
        return d["header"], d
