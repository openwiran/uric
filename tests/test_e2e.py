"""End-to-end test: RIC + simulated E2 nodes + xApps + REST API, over TCP."""

import asyncio
import json
import unittest

from uric.api.server import APIServer
from uric.core.ric import NearRTRIC, RICConfig
from uric.sim.e2node import SimE2Node, World
from uric.xapp import RICError, xAppBase
from uric.xapp.manager import XAppManager



class Probe(xAppBase):
    name = "probe"

    def __init__(self, config, http_server_port, rmr_port):
        super().__init__(config, http_server_port, rmr_port)
        self.reports = []

    def cb(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        self.reports.append((e2_agent_id, self.e2sm_kpm.extract_meas_data(indication_msg)))

    @xAppBase.start_function
    def start(self):
        for node in self.get_e2_node_ids():
            self.e2sm_kpm.subscribe_report_service_style_1(node, 100, ["DRB.UEThpDl"], 100, self.cb)


class SliceCtl(xAppBase):
    """Periodically sets the slice PRB quota of every E2 node (RC style 2 / action 6)."""
    name = "slice-ctl"

    def __init__(self, config, http_server_port, rmr_port):
        super().__init__(config, http_server_port, rmr_port)

    @xAppBase.start_function
    def start(self):
        while self.running:
            for node in self.get_e2_node_ids():
                self.e2sm_rc.control_slice_level_prb_quota(node, 0, 10, int(self.config.get("max", 60)), 100)
            self.sleep(float(self.config.get("interval_s", 0.2)))


async def http(port, method, path, body=None):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    data = json.dumps(body).encode() if body is not None else b""
    w.write(f"{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {len(data)}\r\n\r\n".encode() + data)
    await w.drain()
    raw = await r.read()
    w.close()
    head, _, payload = raw.partition(b"\r\n\r\n")
    return int(head.split()[1]), json.loads(payload or b"null")


async def wait_for(cond, timeout=5.0):
    for _ in range(int(timeout / 0.05)):
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


class TestEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ric = NearRTRIC(RICConfig(e2_port=0, e2_transports=("tcp",), e2_host="127.0.0.1"))
        await self.ric.start()
        self.port = self.ric.listeners[0]["port"]
        self.mgr = XAppManager(self.ric)
        self.api = APIServer(self.ric, self.mgr, "127.0.0.1", 0)
        await self.api.start()
        self.world = World()
        self.nodes = [SimE2Node(0xA00 + i, n_ues=3, world=self.world) for i in range(2)]
        self.tasks = [asyncio.create_task(n.run("127.0.0.1", self.port, "tcp", reconnect=False)) for n in self.nodes]
        self.assertTrue(await wait_for(lambda: len(self.ric.connected_nodes()) == 2))

    async def asyncTearDown(self):
        await self.mgr.stop_all()
        for n in self.nodes:
            await n.stop()
        for t in self.tasks:
            t.cancel()
        await self.api.stop()
        await self.ric.stop()

    async def test_setup_and_capabilities(self):
        node = self.ric.nodes["gnb_00101_a00"]
        self.assertEqual({f.service_model for f in node.ran_functions.values()}, {"KPM", "RC"})
        kpm = node.function("KPM").capabilities
        self.assertEqual([s["type"] for s in kpm["report_styles"]], [1, 2, 3, 4, 5])
        rcc = node.function("RC").capabilities
        self.assertEqual({s["type"] for s in rcc["control_styles"]}, {2, 3})

    async def test_kpm_monitor_discovers_ues(self):
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor", config={"period_ms": 100}).name)
        self.assertTrue(await wait_for(lambda: len(self.ric.ues) == 6))
        self.assertTrue(await wait_for(lambda: all("RSRP" in u.metrics for u in self.ric.ues.values())))
        self.assertTrue(all(u.cell is not None for u in self.ric.ues.values()))
        await self.mgr.stop("kpm-monitor")
        self.assertEqual(self.ric.subscriptions, {})

    async def test_custom_xapp_and_controls(self):
        rec = self.mgr.load(f"{__name__}:Probe")
        await self.mgr.start(rec.name)
        probe = rec.instance
        self.assertTrue(await wait_for(lambda: len({n for n, _ in probe.reports}) == 2))
        self.assertIn("DRB.UEThpDl", probe.reports[-1][1]["measData"])
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor", config={"period_ms": 100}).name)
        self.assertTrue(await wait_for(lambda: len(self.ric.ues) == 6))
        loop = asyncio.get_running_loop()
        ue = self.ric.ues_of("gnb_00101_a00")[0]
        target = self.nodes[1].cells[1]
        res = await loop.run_in_executor(None, lambda: probe.e2sm_rc.handover(
            "gnb_00101_a00", ue.key, target.nci))
        self.assertTrue(res["success"])
        self.assertTrue(await wait_for(lambda: self.ric.ues[ue.key].node_id == "gnb_00101_a01"))
        res = await loop.run_in_executor(None, lambda: probe.e2sm_rc.control_slice_level_prb_quota(
            "gnb_00101_a01", 0, 5, 30, 100))
        self.assertTrue(res["success"])
        self.assertEqual(self.nodes[1].slice_quota[1]["max"], 30)
        # an action the node does not implement is refused by the node
        res = await loop.run_in_executor(None, lambda: probe.e2sm_rc.send_control_request(
            "gnb_00101_a00", ue.key, 1, 1, {1: 1}))
        self.assertFalse(res["success"])
        # unknown nodes are reported as RIC errors
        with self.assertRaises(RICError):
            await loop.run_in_executor(None, lambda: probe.e2sm_rc.handover("nope", ue.key, 1))

    async def test_rest_api(self):
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor", config={"period_ms": 100}).name)
        self.assertTrue(await wait_for(lambda: len(self.ric.ues) == 6))
        p = self.api.port
        st, info = await http(p, "GET", "/api/ric")
        self.assertEqual((st, info["nodes"]), (200, 2))
        st, nodes = await http(p, "GET", "/api/nodes")
        self.assertEqual(len(nodes), 2)
        st, node = await http(p, "GET", "/api/nodes/gnb_00101_a00")
        self.assertEqual(node["ran_functions"][0]["capabilities"]["service_model"], "KPM")
        st, ues = await http(p, "GET", "/api/ues?node=gnb_00101_a00")
        self.assertEqual(len(ues), 3)
        st, subs = await http(p, "GET", "/api/subscriptions")
        sid = subs[0]["id"]
        st, _ = await http(p, "DELETE", "/api/subscriptions/" + sid)
        self.assertEqual(st, 200)
        st, r = await http(p, "POST", "/api/nodes/gnb_00101_a01/query")
        self.assertEqual(r["response"], "RICserviceUpdate")
        st, _ = await http(p, "POST", "/api/xapps", {"spec": "anything"})
        self.assertEqual(st, 405)  # loading xApps through the API is not allowed
        self.mgr.load(f"{__name__}:SliceCtl", config={"max": 55})
        st, r = await http(p, "POST", "/api/xapps/slice-ctl/start")
        self.assertEqual((st, r["state"]), (200, "running"))
        self.assertTrue(await wait_for(lambda: self.nodes[0].controls, timeout=5))
        self.assertEqual(self.nodes[0].slice_quota[1]["max"], 55)
        st, r = await http(p, "POST", "/api/xapps/slice-ctl/stop")
        self.assertEqual(r["state"], "stopped")
        st, page = await http(p, "GET", "/api/nope")
        self.assertEqual(st, 404)

    async def test_reset_and_disconnect(self):
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor", config={"period_ms": 100}).name)
        self.assertTrue(await wait_for(lambda: len(self.ric.ues) == 6))
        await self.ric.reset("gnb_00101_a00")
        self.assertEqual(self.ric.subscriptions_of(node_id="gnb_00101_a00"), [])
        await self.nodes[1].stop()
        self.assertTrue(await wait_for(lambda: not self.ric.nodes["gnb_00101_a01"].connected))
        self.assertEqual(self.ric.ues_of("gnb_00101_a01"), [])


if __name__ == "__main__":
    unittest.main()
