"""Built-in xApp: KPM monitor / UE discovery (srsRAN oran-sc-ric style).

For every E2 node it subscribes to

* E2SM-KPM report style 1: node level measurements,
* E2SM-KPM report style 4: UE level measurements, one subscription per slice
  (S-NSSAI matching condition),
* E2SM-RC report style 4: UE information (serving cell), when supported.

The RIC itself decodes every indication to maintain its UE registry and node
KPIs (what the dashboard shows); this xApp keeps the latest decoded values in
its status. It runs inside the RIC (started by default) or standalone::

    python -m uric.xapps.kpm_monitor --ric 127.0.0.1:4560
"""

import argparse
import signal

from uric.xapp import RICError, xAppBase


class KPMMonitor(xAppBase):
    name = "kpm-monitor"
    version = "3.0.0"
    description = "Collects node and UE level KPM metrics and discovers UEs on every E2 node"

    def __init__(self, config, ric_address=None):
        super().__init__(config, ric_address)
        self.report_period = int(self.config.get("period_ms", 1000))
        self.granul_period = int(self.config.get("granul_period_ms", self.report_period))
        self.node_metrics = self.config.get("node_metrics")      # None = all metrics of style 1
        self.ue_metrics = self.config.get("ue_metrics")          # None = all metrics of style 4
        self.ue_slices = self.config.get("ue_slices", [1, 2])    # SSTs used as matching conditions
        self.rc_ue_info = self.config.get("rc_ue_info", True)
        self.poll_period = float(self.config.get("poll_s", 1.0))
        self.nodes = {}       # e2_node_id -> {"subscriptions": [...], "errors": [...]}
        self.node_kpis = {}   # e2_node_id -> {metric: value}
        self.ue_kpis = {}     # ue key -> {metric: value}
        self.indications = 0

    # ------------------------------------------------------------------ callbacks

    def node_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        """KPM report style 1 (indication message format 1)."""
        self.indications += 1
        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)
        kpis = self.node_kpis.setdefault(e2_agent_id, {})
        for metric_name, value in meas_data.get("measData", {}).items():
            kpis[metric_name] = value[-1] if value else None
        kpis["colletStartTime"] = str(indication_hdr["colletStartTime"])

    def ue_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, sst):
        """KPM report style 4 (indication message format 3)."""
        self.indications += 1
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)
        for ue_id, ue_meas_data in meas_data.get("ueMeasData", {}).items():
            kpis = self.ue_kpis.setdefault(ue_id, {})
            kpis["e2_node_id"], kpis["sst"] = e2_agent_id, sst
            for metric_name, value in ue_meas_data["measData"].items():
                kpis[metric_name] = value[-1] if value else None

    def rc_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        """RC report style 4 (UE information)."""
        self.indications += 1
        for ue_id in indication_msg.get("ues", {}):
            self.ue_kpis.setdefault(ue_id, {})["e2_node_id"] = e2_agent_id

    # ------------------------------------------------------------------ subscriptions

    def subscribe_e2_node(self, e2_node):
        e2_node_id = e2_node["id"]
        info = self.nodes[e2_node_id] = {"subscriptions": [], "errors": []}
        service_models = {f["service_model"] for f in e2_node["ran_functions"]}

        if "KPM" in service_models:
            report_styles = self.e2sm_kpm.get_report_styles(e2_node_id)

            if 1 in report_styles:
                metric_names = self.node_metrics or self.e2sm_kpm.get_supported_metrics(e2_node_id, 1)
                print_subscription(self, e2_node_id, 1, metric_names)
                try:
                    sub_id = self.e2sm_kpm.subscribe_report_service_style_1(
                        e2_node_id, self.report_period, metric_names, self.granul_period,
                        self.node_subscription_callback)
                    info["subscriptions"].append({"style": "kpm 1", "id": sub_id})
                except RICError as e:
                    info["errors"].append(f"kpm 1: {e}")

            if 4 in report_styles:
                metric_names = self.ue_metrics or self.e2sm_kpm.get_supported_metrics(e2_node_id, 4)
                for sst in self.ue_slices or [1]:
                    # matching UE condition: every UE of slice SST
                    matchingUeConds = [{"testCondInfo": {"testType": ("sNSSAI", "true"), "testExpr": "equal",
                                                         "testValue": ("valueInt", int(sst))}}]
                    subscription_callback = lambda agent, sub, hdr, msg, sst=sst: \
                        self.ue_subscription_callback(agent, sub, hdr, msg, sst)
                    print_subscription(self, e2_node_id, 4, metric_names, f"SST {sst}")
                    try:
                        sub_id = self.e2sm_kpm.subscribe_report_service_style_4(
                            e2_node_id, self.report_period, matchingUeConds, metric_names, self.granul_period,
                            subscription_callback)
                        info["subscriptions"].append({"style": f"kpm 4 sst={sst}", "id": sub_id})
                    except RICError as e:
                        info["errors"].append(f"kpm 4 sst={sst}: {e}")

        if "RC" in service_models and self.rc_ue_info:
            rc_report_styles = [s["type"] for s in self.e2sm_rc.get_capabilities(e2_node_id).get("report_styles", [])]
            if 4 in rc_report_styles:
                try:
                    sub_id = self.e2sm_rc.subscribe_report_service_style_4(e2_node_id, self.rc_subscription_callback)
                    info["subscriptions"].append({"style": "rc 4", "id": sub_id})
                except RICError as e:
                    info["errors"].append(f"rc 4: {e}")

        for err in info["errors"]:
            self.log.warning("%s: %s", e2_node_id, err)

    # ------------------------------------------------------------------ main loop

    @xAppBase.start_function
    def start(self):
        while self.running:
            try:
                e2_nodes = {n["id"]: n for n in self.get_e2_nodes()}
                for e2_node_id in set(self.nodes) - set(e2_nodes):   # node gone: forget it
                    self.nodes.pop(e2_node_id)
                    self.node_kpis.pop(e2_node_id, None)
                for e2_node_id, e2_node in e2_nodes.items():
                    if e2_node_id not in self.nodes:
                        self.subscribe_e2_node(e2_node)
                known_ues = {u["key"] for u in self.get_ues()}
                for ue_id in set(self.ue_kpis) - known_ues:            # UE expired in the RIC
                    self.ue_kpis.pop(ue_id)
                self.set_status(nodes=self.nodes, indications=self.indications,
                                node_kpis=self.node_kpis, ue_kpis=self.ue_kpis)
            except RICError as e:
                self.log.warning("%s", e)
            self.sleep(self.poll_period)


def print_subscription(xapp, e2_node_id, style, metric_names, extra=""):
    xapp.log.info("Subscribe to E2 node ID: %s, RAN func: e2sm_kpm, Report Style: %d%s, metrics: %s",
                  e2_node_id, style, f", {extra}" if extra else "", metric_names)


if __name__ == "__main__":
    import logging

    parser = argparse.ArgumentParser(description="uric KPM monitor xApp")
    parser.add_argument("--config", type=str, default="", help="xApp config file path (JSON)")
    parser.add_argument("--ric", type=str, default=None, help="RIC xApp interface host:port (default 127.0.0.1:4560)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    # Create the xApp.
    myXapp = KPMMonitor(args.config, ric_address=args.ric)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start the xApp (unsubscribes everything at exit).
    myXapp.start()
