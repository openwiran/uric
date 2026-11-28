"""End-to-end test: RIC + simulated E2 nodes + xApps + REST API, over TCP."""

import asyncio
import json
import socket
import struct
import threading
import unittest

from uric.api.server import APIServer
from uric.codec import kpm as K
from uric.codec import rc as R
from uric.codec.e2sm import gnb_du_ueid
from uric.core.northbound import NorthboundServer
from uric.xapps.reflex_xapp import Reflex
from uric.core.ric import NearRTRIC, RICConfig
from uric.sim.e2node import SimE2Node, World
from uric.xapp import RICError, xAppBase
from uric.xapp.manager import XAppManager



class Probe(xAppBase):
    name = "probe"

    def __init__(self, config):
        super().__init__(config)
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

    def __init__(self, config):
        super().__init__(config)

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
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor_xapp", config={"period_ms": 100}).name)
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
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor_xapp", config={"period_ms": 100}).name)
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
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor_xapp", config={"period_ms": 100}).name)
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
        await self.mgr.start(self.mgr.load("uric.xapps.kpm_monitor_xapp", config={"period_ms": 100}).name)
        self.assertTrue(await wait_for(lambda: len(self.ric.ues) == 6))
        await self.ric.reset("gnb_00101_a00")
        self.assertEqual(self.ric.subscriptions_of(node_id="gnb_00101_a00"), [])
        await self.nodes[1].stop()
        self.assertTrue(await wait_for(lambda: not self.ric.nodes["gnb_00101_a01"].connected))
        self.assertEqual(self.ric.ues_of("gnb_00101_a01"), [])


class OldStyleClient:
    """A northbound client written against the original protocol: it never looks at
    ``trace`` / ``features`` and never sends ``timing``."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.ids = 0
        self.events = []

    def _send(self, obj):
        data = json.dumps(obj).encode()
        self.sock.sendall(struct.pack("!I", len(data)) + data)

    def _recv(self):
        hdr = b""
        while len(hdr) < 4:
            hdr += self.sock.recv(4 - len(hdr))
        n = struct.unpack("!I", hdr)[0]
        body = b""
        while len(body) < n:
            body += self.sock.recv(n - len(body))
        return json.loads(body)

    def call(self, op, **kw):
        self.ids += 1
        self._send({"id": self.ids, "op": op, **kw})
        while True:
            msg = self._recv()
            if msg.get("id") == self.ids:
                assert msg["ok"], msg
                return msg["result"]
            # only the keys the original client knew about are used
            self.events.append({k: msg.get(k) for k in ("event", "sub", "node", "header", "message")})

    def next_indication(self):
        while True:
            msg = self._recv()
            if msg.get("event") == "indication":
                return msg

    def close(self):
        self.sock.close()


class TestLatencyEndToEnd(unittest.IsolatedAsyncioTestCase):
    LATENCY = True

    async def asyncSetUp(self):
        self.ric = NearRTRIC(RICConfig(e2_port=0, e2_transports=("tcp",), e2_host="127.0.0.1",
                                       latency_enabled=self.LATENCY))
        await self.ric.start()
        self.mgr = XAppManager(self.ric)
        self.api = APIServer(self.ric, self.mgr, "127.0.0.1", 0)
        await self.api.start()
        self.nb = NorthboundServer(self.ric, self.mgr, "127.0.0.1", 0)
        await self.nb.start()
        self.world = World()
        self.nodes = [SimE2Node(0xC00 + i, n_ues=2, world=self.world) for i in range(2)]
        port = self.ric.listeners[0]["port"]
        self.tasks = [asyncio.create_task(n.run("127.0.0.1", port, "tcp", reconnect=False)) for n in self.nodes]
        self.assertTrue(await wait_for(lambda: len(self.ric.connected_nodes()) == 2))

    async def asyncTearDown(self):
        await self.mgr.stop_all()
        for n in self.nodes:
            await n.stop()
        for t in self.tasks:
            t.cancel()
        await self.nb.stop()
        await self.api.stop()
        await self.ric.stop()

    def assert_loop_stages(self, name):
        snap = self.ric.latency.snapshot(xapp=name)
        st = snap["stages"]
        for stage in ("sm_decode", "ric_dispatch", "loop", "control_encode", "e2_ack_rtt",
                      "xapp_queue", "xapp_callback", "xapp_control_call"):
            self.assertGreater(st.get(stage, {}).get("count", 0), 0, stage)
        self.assertGreater(snap["counters"].get("controls_triggered", 0), 0)
        # loop >= RIC dispatch, both measured from the same receive timestamp
        self.assertGreaterEqual(st["loop"]["max"], st["ric_dispatch"]["min"])
        return snap

    def ready(self, name, stages=("loop", "xapp_queue", "xapp_callback", "xapp_control_call")):
        st = self.ric.latency.snapshot(xapp=name)["stages"]
        return all(st.get(s, {}).get("count", 0) >= 3 for s in stages)

    async def test_embedded_reflex(self):
        rec = await self.mgr.start(self.mgr.load("uric.xapps.reflex_xapp", config={"period_ms": 50}).name)
        self.assertEqual(rec.state, "running", rec.error)
        self.assertTrue(await wait_for(lambda: self.ready("reflex"), timeout=8))
        self.assert_loop_stages("reflex")
        self.assertGreater(self.ric.latency.snapshot()["stages"]["e2ap_decode"]["count"], 0)
        x = rec.instance
        lat = await asyncio.get_running_loop().run_in_executor(None, x.get_latency)
        self.assertEqual(set(lat["by_xapp"]), {"reflex"})

        # a control sent from the start/worker thread carries no trace ID -> untriggered
        before = self.ric.latency.snapshot(xapp="reflex")["counters"].get("controls_untriggered", 0)
        node = "gnb_00101_c00"
        msg = R.control_message(R.slice_prb_quota_params([{"sst": 1, "min": 10, "max": 20, "dedicated": 100}]))
        res = await asyncio.get_running_loop().run_in_executor(
            None, lambda: x.send_control_request(node, x.rc_id[node], R.control_header(gnb_du_ueid(0), 2, 6), msg))
        self.assertTrue(res["success"])
        self.assertIsNone(x.current_trace())
        after = self.ric.latency.snapshot(xapp="reflex")["counters"].get("controls_untriggered", 0)
        self.assertEqual(after, before + 1)

    async def test_remote_reflex(self):
        loop = asyncio.get_running_loop()
        x = await loop.run_in_executor(None, lambda: Reflex({"period_ms": 50}, f"127.0.0.1:{self.nb.port}"))
        self.assertTrue(x._link.timing_enabled)

        def run():
            try:
                x.start()
            except SystemExit:
                pass
        threading.Thread(target=run, daemon=True).start()
        self.assertTrue(await wait_for(lambda: self.ready(x.xapp_name), timeout=8))
        self.assert_loop_stages(x.xapp_name)
        lat = await loop.run_in_executor(None, x.get_latency)
        self.assertGreater(lat["stages"]["loop"]["count"], 0)
        # explicit trace=None from outside the callback -> untriggered
        node = "gnb_00101_c01"
        msg = R.control_message(R.slice_prb_quota_params([{"sst": 1, "min": 10, "max": 20, "dedicated": 100}]))
        await loop.run_in_executor(None, lambda: x.send_control_request(
            node, x.rc_id[node], R.control_header(gnb_du_ueid(0), 2, 6), msg, trace=None))
        self.assertGreaterEqual(
            self.ric.latency.snapshot(xapp=x.xapp_name)["counters"].get("controls_untriggered", 0), 1)

        def stop():
            try:
                x.stop()
            except SystemExit:
                pass
        await loop.run_in_executor(None, stop)

    async def test_rest_latency(self):
        await self.mgr.start(self.mgr.load("uric.xapps.reflex_xapp", config={"period_ms": 50}).name)
        self.assertTrue(await wait_for(lambda: self.ready("reflex", ("loop",)), timeout=8))
        p = self.api.port
        st, snap = await http(p, "GET", "/api/latency")
        self.assertEqual(st, 200)
        self.assertEqual(snap["unit"], "ms")
        loop = snap["stages"]["loop"]
        self.assertGreater(loop["count"], 0)
        self.assertIsInstance(loop["p99"], float)
        self.assertIn("reflex", snap["by_xapp"])
        self.assertTrue(snap["by_node"])
        st, f = await http(p, "GET", "/api/latency?xapp=reflex&node=gnb_00101_c00&stage=loop")
        self.assertEqual(set(f["stages"]), {"loop"})
        self.assertEqual({(s["xapp"], s["node"]) for s in f["series"]}, {("reflex", "gnb_00101_c00")})
        st, f = await http(p, "GET", "/api/latency?xapp=nobody")
        self.assertEqual(f["stages"], {})
        await self.mgr.stop("reflex")   # flushes the xApp's buffered samples
        st, r = await http(p, "POST", "/api/latency/reset")
        self.assertEqual((st, r), (200, {"ok": True}))
        st, snap = await http(p, "GET", "/api/latency?xapp=reflex")
        self.assertEqual(snap["stages"], {})

    async def test_old_style_remote_client(self):
        loop = asyncio.get_running_loop()
        port = self.nb.port

        def scenario():
            c = OldStyleClient(port)
            try:
                c.call("hello", name="legacy")
                nodes = c.call("nodes")
                node = nodes[0]
                kpm = next(f["id"] for f in node["ran_functions"] if f["service_model"] == "KPM")
                rcf = next(f["id"] for f in node["ran_functions"] if f["service_model"] == "RC")
                c.call("subscribe", node=node["id"], ran_function_id=kpm,
                       event_trigger=K.event_trigger(50).hex(),
                       actions=[{"id": 1, "type": "report",
                                 "definition": K.action_format1(["DRB.UEThpDl"], 50).hex()}])
                ind = c.next_indication()
                msg = R.control_message(R.slice_prb_quota_params([{"sst": 1, "min": 10, "max": 40,
                                                                   "dedicated": 100}]))
                return c.call("control", node=ind["node"], ran_function_id=rcf,
                              header=R.control_header(gnb_du_ueid(0), 2, 6).hex(), message=msg.hex(),
                              ack=True)
            finally:
                c.close()
        res = await loop.run_in_executor(None, scenario)
        self.assertTrue(res["success"])
        snap = self.ric.latency.snapshot(xapp="legacy")
        self.assertEqual(snap["counters"].get("controls_untriggered"), 1)
        self.assertNotIn("loop", snap["stages"])
        self.assertGreater(snap["stages"]["ric_dispatch"]["count"], 0)

    async def test_new_client_old_ric(self):
        """A new xApp against a RIC without the latency features degrades silently."""
        import uric.core.northbound as nbmod
        loop = asyncio.get_running_loop()
        saved = nbmod.FEATURES
        nbmod.FEATURES = []   # hello answer without the optional features, as an old RIC
        try:
            x = await loop.run_in_executor(None, lambda: Reflex({"period_ms": 50}, f"127.0.0.1:{self.nb.port}"))
        finally:
            nbmod.FEATURES = saved
        self.assertFalse(x._link.timing_enabled)

        def run():
            try:
                x.start()
            except SystemExit:
                pass
        threading.Thread(target=run, daemon=True).start()
        self.assertTrue(await wait_for(lambda: x.counters()["controls"] >= 3, timeout=8))
        self.assertEqual(x.counters()["errors"], 0)
        self.assertEqual(await loop.run_in_executor(None, x.get_latency), {})
        snap = self.ric.latency.snapshot(xapp=x.xapp_name)
        self.assertNotIn("loop", snap["stages"])         # no trace IDs sent
        self.assertNotIn("xapp_callback", snap["stages"])  # no timing op sent

        def stop():
            try:
                x.stop()
            except SystemExit:
                pass
        await loop.run_in_executor(None, stop)


class TestLatencyDisabled(unittest.IsolatedAsyncioTestCase):
    """With ``latency_enabled=False`` nothing is recorded and behaviour is unchanged."""
    LATENCY = False
    asyncSetUp = TestLatencyEndToEnd.asyncSetUp
    asyncTearDown = TestLatencyEndToEnd.asyncTearDown

    async def test_disabled(self):
        rec = await self.mgr.start(self.mgr.load("uric.xapps.reflex_xapp", config={"period_ms": 50}).name)
        self.assertEqual(rec.state, "running", rec.error)
        self.assertTrue(await wait_for(lambda: rec.instance.counters()["controls"] >= 3, timeout=8))
        self.assertFalse(rec.instance._timing)
        snap = self.ric.latency.snapshot()
        self.assertEqual((snap["enabled"], snap["stages"], snap["counters"]), (False, {}, {}))
        st, snap = await http(self.api.port, "GET", "/api/latency")
        self.assertEqual((st, snap["enabled"]), (200, False))

if __name__ == "__main__":
    unittest.main()
