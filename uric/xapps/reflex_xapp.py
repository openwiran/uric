"""Reflex: one RC control (slice PRB quota) for every KPM indication.

The minimal closed loop used to measure the near-RT control-loop latency of the
RIC (``tools/bench_latency.py``): it subscribes to E2SM-KPM style 1 reports on
every E2 node and answers each report, from inside the subscription callback,
with an E2SM-RC style 2 / action 6 control (slice level PRB quota, max ratio
cycling over 30/50/70/100 %). No RIC query is made on the hot path: RAN
function IDs are looked up once at start and the control is encoded in the
callback.

Inside the RIC (``--xapp uric.xapps.reflex_xapp``, config ``period_ms``) or
standalone::

    python -m uric.xapps.reflex_xapp --ric 127.0.0.1:4560 --period-ms 100
"""

from __future__ import annotations

import argparse
import signal
import threading
import time

from uric.codec import kpm as K
from uric.codec import rc as R
from uric.codec.e2sm import gnb_du_ueid
from uric.xapp import RICError, xAppBase
from uric.xapp.xappbase import e2sm_types

MAX_RATIOS = (30, 50, 70, 100)


class Reflex(xAppBase):
    name = "reflex"
    version = "1.0.0"
    description = "One RC PRB quota control per KPM indication (latency benchmark)"

    # ``config`` must stay a required positional parameter: the xApp manager passes the
    # configuration only to required constructor parameters
    def __init__(self, config, ric_address=None):
        super().__init__(config, ric_address)
        self.period_ms = int(self.config.get("period_ms", 100))
        self.ack = int(self.config.get("ack", 1))
        self.rc_id: dict[str, int] = {}
        self.handled = 0
        self.controls = 0
        self.failures = 0
        self.errors = 0
        self._lock = threading.Lock()
        self._ue = gnb_du_ueid(0)
        self._hdr = R.control_header(self._ue, 2, 6)

    def on_kpm(self, e2_node_id, subscription_id, indication_hdr, indication_msg):
        with self._lock:
            self.handled += 1
            n = self.handled
        mx = MAX_RATIOS[n % len(MAX_RATIOS)]
        msg = R.control_message(R.slice_prb_quota_params(
            [{"sst": 1, "sd": 0xFFFFFF, "min": 10, "max": mx, "dedicated": 100}], "00101"))
        try:
            res = self.send_control_request(e2_node_id, self.rc_id[e2_node_id], self._hdr, msg, self.ack)
            with self._lock:
                self.controls += 1
                if not res.get("success"):
                    self.failures += 1
        except (RICError, KeyError) as exc:
            with self._lock:
                self.errors += 1
            self.log.debug("control to %s failed: %s", e2_node_id, exc)

    def counters(self) -> dict:
        with self._lock:
            return {"handled": self.handled, "controls": self.controls, "failures": self.failures,
                    "errors": self.errors, "cpu_s": time.process_time()}

    @xAppBase.start_function
    def start(self, period_ms=None, status_period_s=1.0):
        if period_ms:
            self.period_ms = int(period_ms)
        for node in self.get_e2_nodes():
            kpm = [f["id"] for f in node["ran_functions"] if f["service_model"] == "KPM"]
            rcf = [f["id"] for f in node["ran_functions"] if f["service_model"] == "RC"]
            if not kpm or not rcf:
                continue
            self.rc_id[node["id"]] = rcf[0]
            self.subscribe(node["id"], kpm[0], K.event_trigger(self.period_ms),
                           K.action_format1(["DRB.UEThpDl"], self.period_ms), self.on_kpm, e2sm_types.E2SM_KPM,
                           {"sm": "KPM", "style": 1, "period_ms": self.period_ms, "metrics": ["DRB.UEThpDl"]})
        while self.running:
            try:
                self.set_status(**self.counters(), nodes=sorted(self.rc_id), period_ms=self.period_ms)
            except RICError:
                pass
            self.sleep(float(status_period_s))


if __name__ == "__main__":
    import logging

    p = argparse.ArgumentParser(description="uric reflex xApp (latency benchmark)")
    p.add_argument("--ric", default=None, help="RIC xApp interface host:port (default 127.0.0.1:4560)")
    p.add_argument("--period-ms", type=int, default=100, help="KPM report period [ms]")
    p.add_argument("--no-ack", action="store_true", help="send controls without requesting an ack")
    p.add_argument("--log-level", default="WARNING")
    a = p.parse_args()
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    x = Reflex({"period_ms": a.period_ms, "ack": 0 if a.no_ack else 1}, ric_address=a.ric)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, x.signal_handler)
    x.start()
