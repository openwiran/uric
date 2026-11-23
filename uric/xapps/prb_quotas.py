"""PRB quotas xApp (srsRAN oran-sc-ric style).

For every UE it cycles the slice level max PRB quota (E2SM-RC control style 2 /
action 6) every ``period_s`` seconds (default 5) through ``max_prb_ratios``
(default 30, 50, 70, 100).

UEs are discovered by the xApp itself, without relying on other xApps:

* E2SM-KPM report style 4, one subscription per slice (S-NSSAI matching
  condition), which also tells the slice (SST) of each UE;
* E2SM-RC report style 4 (UE information), when supported.

When an E2 node disconnects, its UEs are forgotten. When it reconnects
(detected through a change of its connection time, even if it reconnected
between two polls) the subscriptions are created again and its UEs are
discovered and tracked again. UEs that stop being reported for
``ue_timeout_s`` seconds are dropped; UEs moving to another node follow it.

Run inside the RIC (``uric --xapp uric.xapps.prb_quotas``) or standalone::

    python -m uric.xapps.prb_quotas --ric 127.0.0.1:4560
"""

import argparse
import datetime
import signal
import threading
import time

from uric.xapp import RICError, xAppBase


class PRBQuotas(xAppBase):
    name = "prb-quotas"
    version = "1.0.0"
    description = "Cycles the max PRB quota of every UE through 30/50/70/100% every 5 s (KPM/RC UE discovery)"

    def __init__(self, config, ric_address=None):
        super().__init__(config, ric_address)
        self.period_s = float(self.config.get("period_s", 5.0))
        self.max_prb_ratios = [int(v) for v in self.config.get("max_prb_ratios", [30, 50, 70, 100])]
        self.min_prb_ratio = int(self.config.get("min_prb_ratio", 10))
        self.dedicated_prb_ratio = int(self.config.get("dedicated_prb_ratio", 100))
        self.slices = [int(s) for s in self.config.get("slices", [1, 2])]
        self.report_period = int(self.config.get("report_period_ms", 1000))
        self.ue_timeout_s = float(self.config.get("ue_timeout_s", 10.0))
        self.poll_s = float(self.config.get("poll_s", 0.5))
        self.lock = threading.Lock()
        # e2_node_id -> {"connected_at": ts, "subscriptions": [...], "errors": [...]}
        self.nodes = {}
        # ue key -> {"e2_node_id", "sst", "last_seen", "index", "next_change", "max_prb_ratio", "last_result"}
        self.ues = {}
        self.controls_sent = 0
        self.controls_failed = 0

    # ------------------------------------------------------------------ UE discovery

    def track_ue(self, ue_id, e2_node_id, sst=None):
        now = time.time()
        with self.lock:
            ue = self.ues.get(ue_id)
            if ue is None:
                self.ues[ue_id] = {"e2_node_id": e2_node_id, "sst": sst or 1, "last_seen": now, "index": 0,
                                   "next_change": now, "max_prb_ratio": None, "last_result": None}
                self.log.info("tracking UE %s on %s (SST %s)", ue_id, e2_node_id, sst or 1)
                return
            if ue["e2_node_id"] != e2_node_id:
                self.log.info("UE %s moved %s -> %s", ue_id, ue["e2_node_id"], e2_node_id)
                ue["e2_node_id"] = e2_node_id
            if sst is not None:
                ue["sst"] = sst
            ue["last_seen"] = now

    def kpm_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, sst):
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)
        for ue_id in meas_data.get("ueMeasData", {}):
            self.track_ue(ue_id, e2_agent_id, sst)

    def rc_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        for ue_id in indication_msg.get("ues", {}):
            self.track_ue(ue_id, e2_agent_id)

    def subscribe_e2_node(self, e2_node):
        e2_node_id = e2_node["id"]
        info = {"connected_at": e2_node["connected_at"], "subscriptions": [], "errors": []}
        self.nodes[e2_node_id] = info
        service_models = {f["service_model"] for f in e2_node["ran_functions"]}

        if "RC" not in service_models or 6 not in self.e2sm_rc.get_control_styles(e2_node_id).get(2, []):
            info["errors"].append("RC control style 2 / action 6 (slice level PRB quota) not supported")
            self.log.warning("%s: slice level PRB quota control not supported, ignoring node", e2_node_id)
            return

        if "KPM" in service_models and 4 in self.e2sm_kpm.get_report_styles(e2_node_id):
            metric_names = self.e2sm_kpm.get_supported_metrics(e2_node_id, 4)[:1]
            for sst in self.slices:
                matchingUeConds = [{"testCondInfo": {"testType": ("sNSSAI", "true"), "testExpr": "equal",
                                                     "testValue": ("valueInt", sst)}}]
                subscription_callback = lambda agent, sub, hdr, msg, sst=sst: \
                    self.kpm_subscription_callback(agent, sub, hdr, msg, sst)
                try:
                    sub_id = self.e2sm_kpm.subscribe_report_service_style_4(
                        e2_node_id, self.report_period, matchingUeConds, metric_names, self.report_period,
                        subscription_callback)
                    info["subscriptions"].append({"style": f"kpm 4 sst={sst}", "id": sub_id})
                except RICError as e:
                    info["errors"].append(f"kpm 4 sst={sst}: {e}")

        rc_report_styles = [s["type"] for s in self.e2sm_rc.get_capabilities(e2_node_id).get("report_styles", [])]
        if 4 in rc_report_styles:
            try:
                sub_id = self.e2sm_rc.subscribe_report_service_style_4(e2_node_id, self.rc_subscription_callback)
                info["subscriptions"].append({"style": "rc 4", "id": sub_id})
            except RICError as e:
                info["errors"].append(f"rc 4: {e}")

        if not info["subscriptions"]:
            self.log.warning("%s: no UE discovery possible (%s)", e2_node_id, "; ".join(info["errors"]))
        self.log.info("%s: subscribed (%s)", e2_node_id, ", ".join(s["style"] for s in info["subscriptions"]))

    def forget_e2_node(self, e2_node_id, reason):
        info = self.nodes.pop(e2_node_id, None)
        if info:
            for sub in info["subscriptions"]:
                self.my_subscriptions.pop(sub["id"], None)  # already deleted by the RIC
        with self.lock:
            gone = [k for k, ue in self.ues.items() if ue["e2_node_id"] == e2_node_id]
            for k in gone:
                del self.ues[k]
        self.log.info("%s %s: forgot %d UE(s)", e2_node_id, reason, len(gone))

    def sync_e2_nodes(self):
        e2_nodes = {n["id"]: n for n in self.get_e2_nodes()}
        for e2_node_id in list(self.nodes):
            if e2_node_id not in e2_nodes:
                self.forget_e2_node(e2_node_id, "disconnected")
            elif e2_nodes[e2_node_id]["connected_at"] != self.nodes[e2_node_id]["connected_at"]:
                self.forget_e2_node(e2_node_id, "reconnected")
        for e2_node_id, e2_node in e2_nodes.items():
            if e2_node_id not in self.nodes:
                self.subscribe_e2_node(e2_node)

    def expire_ues(self):
        now = time.time()
        with self.lock:
            for k in [k for k, ue in self.ues.items() if now - ue["last_seen"] > self.ue_timeout_s]:
                self.log.info("UE %s not reported for %.0f s, no longer tracked", k, self.ue_timeout_s)
                del self.ues[k]

    # ------------------------------------------------------------------ PRB quota cycling

    def cycle_prb_quotas(self):
        now = time.time()
        with self.lock:
            due = [(k, dict(ue)) for k, ue in self.ues.items() if ue["next_change"] <= now]
        for ue_id, ue in due:
            max_prb_ratio = self.max_prb_ratios[ue["index"] % len(self.max_prb_ratios)]
            current_time = datetime.datetime.now()
            self.log.info("%s Send RIC Control Request to E2 node ID: %s for UE ID: %s, SST: %d, PRB_min_ratio: %d, "
                          "PRB_max_ratio: %d", current_time.strftime("%H:%M:%S"), ue["e2_node_id"], ue_id,
                          ue["sst"], self.min_prb_ratio, max_prb_ratio)
            try:
                res = self.e2sm_rc.control_slice_level_prb_quota(
                    ue["e2_node_id"], ue_id, min_prb_ratio=self.min_prb_ratio, max_prb_ratio=max_prb_ratio,
                    dedicated_prb_ratio=self.dedicated_prb_ratio, ack_request=1, sst=ue["sst"])
                ok = bool(res and res["success"])
                result = "ack" if ok else f"failure: {res and res['cause']}"
            except RICError as e:
                ok, result = False, f"error: {e}"
            self.controls_sent += 1
            self.controls_failed += 0 if ok else 1
            if not ok:
                self.log.warning("UE %s: %s", ue_id, result)
            with self.lock:
                cur = self.ues.get(ue_id)
                if cur is not None:
                    cur["index"] = ue["index"] + 1
                    cur["next_change"] = now + self.period_s
                    cur["max_prb_ratio"] = max_prb_ratio
                    cur["last_result"] = result

    # ------------------------------------------------------------------ main loop

    @xAppBase.start_function
    def start(self):
        while self.running:
            try:
                self.sync_e2_nodes()
                self.expire_ues()
                self.cycle_prb_quotas()
                with self.lock:
                    ues = {k: {"e2_node_id": u["e2_node_id"], "sst": u["sst"], "max_prb_ratio": u["max_prb_ratio"],
                               "last_result": u["last_result"]} for k, u in self.ues.items()}
                self.set_status(nodes=self.nodes, ues=ues, controls_sent=self.controls_sent,
                                controls_failed=self.controls_failed)
            except RICError as e:
                self.log.warning("%s", e)
            self.sleep(self.poll_s)


if __name__ == "__main__":
    import logging

    parser = argparse.ArgumentParser(description="uric PRB quotas xApp")
    parser.add_argument("--config", type=str, default="", help="xApp config file path (JSON)")
    parser.add_argument("--ric", type=str, default=None, help="RIC xApp interface host:port (default 127.0.0.1:4560)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    myXapp = PRBQuotas(args.config, ric_address=args.ric)

    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    myXapp.start()
