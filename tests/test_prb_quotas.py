"""prb_quotas xApp: UE discovery, per-UE PRB quota cycling and E2 node reconnection."""

import asyncio
import socket
import time
import unittest

from uric.codec.e2sm import ue_key
from uric.core.ric import NearRTRIC, RICConfig
from uric.sim.e2node import SimE2Node, World
from uric.xapp.manager import XAppManager

PERIOD = 0.4


async def wait_for(cond, timeout=8.0):
    for _ in range(int(timeout / 0.05)):
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


def quota(control):
    """(sst, max PRB ratio) of a slice level PRB quota control."""
    grp = control["params"][1][0][2]
    return grp[3][4][0][5][7][8][0], grp[11]


def per_ue(node, since=0.0):
    out = {}
    for c in node.controls:
        if c["style"] == 2 and c["action"] == 6 and c["ts"] >= since:
            out.setdefault(c["ue"], []).append(c)
    return out


class TestPRBQuotas(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ric = NearRTRIC(RICConfig(e2_port=0, e2_transports=("tcp",), e2_host="127.0.0.1"))
        await self.ric.start()
        self.mgr = XAppManager(self.ric)
        world = World()
        self.nodes = [SimE2Node(0xC00 + i, n_ues=3, world=world) for i in range(2)]
        port = self.ric.listeners[0]["port"]
        self.tasks = [asyncio.create_task(n.run("127.0.0.1", port, "tcp")) for n in self.nodes]
        self.assertTrue(await wait_for(lambda: len(self.ric.connected_nodes()) == 2))
        # no other xApp is started: prb-quotas must discover the UEs by itself
        rec = self.mgr.load("uric.xapps.prb_quotas", config={
            "period_s": PERIOD, "poll_s": 0.05, "report_period_ms": 100})
        self.rec = await self.mgr.start(rec.name)
        self.assertEqual(self.rec.state, "running", self.rec.error)

    async def asyncTearDown(self):
        await self.mgr.stop_all()
        for n in self.nodes:
            await n.stop()
        for t in self.tasks:
            t.cancel()
        await self.ric.stop()

    def sim_ues(self, node):
        return {ue_key(u.ueid): u for u in node.ues}

    async def test_cycles_every_ue(self):
        expected_ues = {k for n in self.nodes for k in self.sim_ues(n)}
        self.assertTrue(await wait_for(
            lambda: all(len(per_ue(n).get(k, [])) >= 5 for n in self.nodes for k in self.sim_ues(n))))
        for n in self.nodes:
            sims = self.sim_ues(n)
            for k, controls in per_ue(n).items():
                self.assertIn(k, expected_ues)
                values = [quota(c)[1] for c in controls[:5]]
                self.assertEqual(values, [30, 50, 70, 100, 30])
                self.assertTrue(all(quota(c)[0] == sims[k].sst for c in controls))  # slice of the UE
                gaps = [b["ts"] - a["ts"] for a, b in zip(controls, controls[1:5])]
                self.assertTrue(all(PERIOD * 0.8 <= g <= PERIOD * 2 for g in gaps), gaps)
        status = self.rec.session.status
        self.assertEqual(set(status["ues"]), expected_ues)
        self.assertEqual(status["controls_failed"], 0)

    async def test_rediscovers_ues_after_reconnection(self):
        n0 = self.nodes[0]
        node_id = "gnb_00101_c00"
        owner = self.rec.name
        self.assertTrue(await wait_for(lambda: all(k in per_ue(n0) for k in self.sim_ues(n0))))
        self.assertEqual(len(self.ric.subscriptions_of(owner=owner, node_id=node_id)), 3)  # kpm sst 1/2 + rc
        before = self.ric.nodes[node_id].connected_at

        n0.conn.sock.shutdown(socket.SHUT_RDWR)   # drop the E2 association; the simulator reconnects by itself
        self.assertTrue(await wait_for(lambda: self.ric.nodes[node_id].connected
                                       and self.ric.nodes[node_id].connected_at != before))
        reconnected = time.time()
        self.assertTrue(await wait_for(lambda: len(self.ric.subscriptions_of(owner=owner, node_id=node_id)) == 3))
        # every UE of the node is tracked again and its cycle restarts from 30%
        self.assertTrue(await wait_for(
            lambda: all(len(per_ue(n0, reconnected).get(k, [])) >= 2 for k in self.sim_ues(n0))))
        for k in self.sim_ues(n0):
            values = [quota(c)[1] for c in per_ue(n0, reconnected)[k][:2]]
            self.assertEqual(values, [30, 50])
        # the other node was not affected
        self.assertEqual(len(self.ric.subscriptions_of(owner=owner, node_id="gnb_00101_c01")), 3)


if __name__ == "__main__":
    unittest.main()
