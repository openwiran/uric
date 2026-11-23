"""OSC style xAppBase API: embedded (inside the RIC) and standalone (xApp interface)."""

import asyncio
import threading
import unittest

from uric.core.northbound import NorthboundServer
from uric.core.ric import NearRTRIC, RICConfig
from uric.sim.e2node import SimE2Node, World
from uric.xapp import xAppBase
from uric.xapp.manager import XAppManager



class KpmProbe(xAppBase):
    """Collects style 1 and style 4 reports, decoded with the OSC helpers."""

    def __init__(self, config, ric_address=None):
        super().__init__(config, ric_address)
        self.node_meas, self.ue_meas, self.hdrs = [], [], []

    def cb1(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        self.hdrs.append(self.e2sm_kpm.extract_hdr_info(indication_hdr))
        self.node_meas.append((e2_agent_id, self.e2sm_kpm.extract_meas_data(indication_msg)))

    def cb4(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        self.ue_meas.append((e2_agent_id, self.e2sm_kpm.extract_meas_data(indication_msg)))

    @xAppBase.start_function
    def start(self, e2_node_id=None, metric_names=("DRB.UEThpDl",)):
        node = e2_node_id or self.get_e2_node_ids()[0]
        self.e2sm_kpm.subscribe_report_service_style_1(node, 100, list(metric_names), 100, self.cb1)
        conds = [{"testCondInfo": {"testType": ("sNSSAI", "true"), "testExpr": "equal",
                                   "testValue": ("valueInt", 1)}}]
        self.e2sm_kpm.subscribe_report_service_style_4(node, 100, conds, ["RSRP", "CQI"], 100, self.cb4)


class SliceCtl(xAppBase):
    """Periodically sets the slice PRB quota of every E2 node (RC style 2 / action 6)."""
    name = "slice-ctl"

    def __init__(self, config):
        super().__init__(config)

    @xAppBase.start_function
    def start(self):
        while self.running:
            for node in self.get_e2_node_ids():
                self.e2sm_rc.control_slice_level_prb_quota(node, 0, 10, int(self.config.get("max", 60)), 100)
            self.sleep(float(self.config.get("interval_s", 0.2)))


async def wait_for(cond, timeout=5.0):
    for _ in range(int(timeout / 0.05)):
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


class TestXAppBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ric = NearRTRIC(RICConfig(e2_port=0, e2_transports=("tcp",), e2_host="127.0.0.1"))
        await self.ric.start()
        self.mgr = XAppManager(self.ric)
        self.nb = NorthboundServer(self.ric, self.mgr, "127.0.0.1", 0)
        await self.nb.start()
        self.world = World()
        self.nodes = [SimE2Node(0xB00 + i, n_ues=3, world=self.world) for i in range(2)]
        port = self.ric.listeners[0]["port"]
        self.tasks = [asyncio.create_task(n.run("127.0.0.1", port, "tcp", reconnect=False)) for n in self.nodes]
        self.assertTrue(await wait_for(lambda: len(self.ric.connected_nodes()) == 2))
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor", config={"period_ms": 100}).name)
        self.assertTrue(await wait_for(lambda: len(self.ric.ues) == 6))

    async def asyncTearDown(self):
        await self.mgr.stop_all()
        for n in self.nodes:
            await n.stop()
        for t in self.tasks:
            t.cancel()
        await self.nb.stop()
        await self.ric.stop()

    async def test_embedded(self):
        rec = self.mgr.load(f"{__name__}:KpmProbe", config={"start_args": {"e2_node_id": "gnb_00101_b00"}})
        rec = await self.mgr.start(rec.name)
        self.assertEqual(rec.state, "running")
        x = rec.instance
        self.assertTrue(await wait_for(lambda: x.node_meas and x.ue_meas))
        node, meas = x.node_meas[-1]
        self.assertEqual(node, "gnb_00101_b00")
        self.assertIn("DRB.UEThpDl", meas["measData"])
        self.assertEqual(type(x.hdrs[-1]["colletStartTime"]).__name__, "datetime")
        _, ue_meas = x.ue_meas[-1]
        self.assertTrue(all(set(d["measData"]) == {"RSRP", "CQI"} for d in ue_meas["ueMeasData"].values()))
        self.assertEqual(len(self.ric.subscriptions_of(owner=rec.name)), 2)
        await self.mgr.stop(rec.name)
        self.assertEqual(self.ric.subscriptions_of(owner=rec.name), [])

    async def test_embedded_rc(self):
        loop = asyncio.get_running_loop()
        rec = await self.mgr.start(self.mgr.load(f"{__name__}:SliceCtl").name)
        self.assertEqual(rec.state, "running", rec.error)
        self.assertTrue(await wait_for(lambda: self.nodes[0].controls and self.nodes[1].controls))
        x = rec.instance
        # OSC compatible helpers, called from a worker thread as an xApp would
        ue = self.ric.ues_of("gnb_00101_b00")[0]
        res = await loop.run_in_executor(None, lambda: x.e2sm_rc.control_slice_level_prb_quota(
            "gnb_00101_b00", ue.key, min_prb_ratio=10, max_prb_ratio=40, dedicated_prb_ratio=100))
        self.assertTrue(res["success"])
        amf = int(ue.key.split(":")[1])
        tgt = self.nodes[1].cells[0]
        res = await loop.run_in_executor(None, lambda: x.e2sm_rc.control_handover(
            "gnb_00101_b00", amf, amf, "00101", tgt.nci))
        self.assertTrue(res["success"])
        self.assertTrue(await wait_for(lambda: self.ric.ues[ue.key].node_id == "gnb_00101_b01"))
        await self.mgr.stop(rec.name)

    async def test_standalone(self):
        loop = asyncio.get_running_loop()
        addr = f"127.0.0.1:{self.nb.port}"
        x = await loop.run_in_executor(None, lambda: KpmProbe("", ric_address=addr))
        self.assertIn(x.xapp_name, self.mgr.xapps)
        self.assertEqual(self.mgr.xapps[x.xapp_name].source, "remote")
        exited = threading.Event()

        def run():
            try:
                x.start()
            except SystemExit:
                exited.set()
        threading.Thread(target=run, daemon=True).start()
        self.assertTrue(await wait_for(lambda: x.node_meas and x.ue_meas))
        ues = await loop.run_in_executor(None, x.get_ues)
        self.assertEqual(len(ues), 6)
        res = await loop.run_in_executor(None, lambda: x.e2sm_rc.handover(
            ues[0]["node_id"], ues[0]["key"], self.nodes[1].cells[1].nci))
        self.assertTrue(res["success"])
        self.assertEqual(len(self.ric.subscriptions_of(owner=x.xapp_name)), 2)

        def stop():
            try:
                x.stop()
            except SystemExit:
                pass
        await loop.run_in_executor(None, stop)
        self.assertTrue(await wait_for(lambda: x.xapp_name not in self.mgr.xapps))
        self.assertEqual(self.ric.subscriptions_of(owner=x.xapp_name), [])


if __name__ == "__main__":
    unittest.main()
