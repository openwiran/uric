"""PRB Quota.

For every E2 node it cycles the max PRB quota assigned to the
associated UEs.

It runs inside the RIC (not started by default) or standalone::

    python -m uric.xapps.prb_quota --ric 127.0.0.1:4560
"""

import argparse
import signal
import threading
import time

from uric.xapp import RICError, xAppBase


# PRB quota schedule applied cyclically: (min_prb_ratio, max_prb_ratio)
PRB_SCHEDULE = [(10, 30), (10, 50), (10, 70), (10, 100)]


class PRBQuota(xAppBase):
    name = "prb-quota"
    version = "1.0.0"
    description = "Cycle the max PRB quotas assigned to a UE"

    def __init__(self, config, ue_timeout_s, report_period, granul_period, step_period, ric_address):
        super().__init__(config, ric_address)
        self.ue_timeout_s = ue_timeout_s or float(self.config.get("ue_timeout_s", 5.0))
        self.report_period = report_period or int(self.config.get("ue_timeout_s", 1000))
        self.granul_period = granul_period or int(self.config.get("granul_period", 1000))
        self.step_period = step_period or float(self.config.get("step_period", 5.0))
        self.ues = {}      
        self.nodes = {}          
        self.ues_lock = threading.Lock()  

    def send_prb_quota(self, e2_node_id, ue_id, min_prb_ratio, max_prb_ratio):
        self.log.info("Send RIC Control Request to E2 node ID: %s for UE ID: %s, PRB_min_ratio: %u, PRB_max_ratio: %s", 
                      e2_node_id, ue_id, min_prb_ratio, max_prb_ratio)
        self.e2sm_rc.control_slice_level_prb_quota(e2_node_id, ue_id, min_prb_ratio, max_prb_ratio,
                                                   dedicated_prb_ratio=100, ack_request=1)
                                                   
    def active_ues(self):
        """UEs reported within the last ue_timeout seconds; expired ones are removed."""
        now = time.monotonic()
        with self.ues_lock:
            for ue_id in [u for u, t in self.ues.items() if now - t > self.ue_timeout_s]:
                self.log.info("UE ID: %s not reported for %ss, removing", ue_id, self.ue_timeout_s)
                del self.ues[ue_id]
            return sorted(self.ues)

    # ------------------------------------------------------------------ callbacks
    
    def kpm_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)
        now = time.monotonic()
        with self.ues_lock:
            for ue_id in meas_data.get("ueMeasData", {}):
                if ue_id not in self.ues:
                    self.log.info("Discovered UE ID: %s on E2 node: %s", ue_id, e2_agent_id)
                self.ues[ue_id] = now

    # ------------------------------------------------------------------ subscriptions
                    
    def subscribe_ue_discovery(self, e2_node_id):
        info = self.nodes[e2_node_id] = {"subscriptions": [], "errors": []}
        # Condition always true for attached UEs -> every UE is reported.
        matching_ue_conds = [{'testCondInfo': {'testType': ('ul-rSRP', 'true'),
                                               'testExpr': 'lessthan',
                                               'testValue': ('valueInt', 1000)}}]
        self.log.info("Subscribing to E2 node ID: %s (KPM style 4, metric: DRB.UEThpDl) for UE discovery", e2_node_id)
        try:
            sub_id = self.e2sm_kpm.subscribe_report_service_style_4(e2_node_id, self.report_period, matching_ue_conds,
                                                                    ["DRB.UEThpDl"], self.granul_period, self.kpm_callback)
            info["subscriptions"].append({"style": "kpm 4", "id": sub_id})
        except RICError as e:
            info["errors"].append(f"kpm 1: {e}")
                    
    # ------------------------------------------------------------------ main loop
                                                           
    @xAppBase.start_function
    def start(self):
        step = 0
        while self.running:
            try:
                e2_nodes = {n["id"]: n for n in self.get_e2_nodes()}
                for e2_node_id in set(self.nodes) - set(e2_nodes):   # node gone: forget it
                    self.nodes.pop(e2_node_id)
                for e2_node_id, e2_node in e2_nodes.items():
                    if e2_node_id not in self.nodes:
                        self.subscribe_ue_discovery(e2_node_id)                      
                ue_ids = self.active_ues()
                if not ue_ids:
                    self.log.info("No UEs discovered yet, waiting...")
                    time.sleep(1)
                    continue
                min_prb_ratio, max_prb_ratio = PRB_SCHEDULE[step % len(PRB_SCHEDULE)]
                for ue_id in ue_ids:
                    self.send_prb_quota(e2_node_id, ue_id, min_prb_ratio, max_prb_ratio)
                step += 1
                self.set_status(ues=self.ues, nodes=self.nodes)
            except RICError as e:
                self.log.warning("%s", e)
            self.sleep(self.step_period)


if __name__ == "__main__":
    import logging

    parser = argparse.ArgumentParser(description="uric PRB quota xApp")
    parser.add_argument("--config", type=str, default="", help="xApp config file path (JSON)")
    parser.add_argument("--ric", type=str, help="RIC xApp interface host:port (default 127.0.0.1:4560)")
    parser.add_argument("--ue_timeout_s", type=float, help="UE timeout [s] (default 5.0)")
    parser.add_argument("--report_period", type=int, help="KPM report period [ms] (default 1000)")
    parser.add_argument("--granul_period", type=int, help="KPM granularity period [ms] (default 1000)")
    parser.add_argument("--step_period", type=float, help="Seconds between PRB quota changes (default 5.0)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    # Create the xApp.
    myXapp = PRBQuota(config=args.config, ue_timeout_s=args.ue_timeout_s, report_period=args.report_period, 
                      granul_period=args.granul_period, step_period=args.step_period, ric_address=args.ric)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start the xApp (unsubscribes everything at exit).
    myXapp.start()
