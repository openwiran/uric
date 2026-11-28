"""Unit tests of the latency recorder (uric.core.latency)."""

import json
import os
import random
import tempfile
import threading
import time
import unittest

from uric.core import latency as L
from uric.core.latency import Histogram, LatencyRecorder


def exact_percentile(values, p):
    v = sorted(values)
    import math
    return v[max(1, int(math.ceil(p / 100 * len(v)))) - 1]


class TestHistogram(unittest.TestCase):
    def test_layout(self):
        self.assertLessEqual(L.MAX_REL_ERROR, 0.05)
        self.assertEqual(L.bucket_of(999), 0)
        self.assertEqual(L.bucket_of(L.HI_NS), L.N_BUCKETS - 1)
        for ns in (1_000, 1_234, 50_000, 7_777_777, 99_999_999_999):
            lo, hi = L.bucket_bounds(L.bucket_of(ns))
            self.assertTrue(lo * (1 - 1e-9) <= ns < hi * (1 + 1e-9), (ns, lo, hi))

    def check(self, values):
        h = Histogram()
        for v in values:
            h.add(v)
        self.assertEqual(h.count, len(values))
        self.assertEqual(h.sum, sum(values))
        self.assertEqual((h.min, h.max), (min(values), max(values)))
        for p in (50, 90, 95, 99, 99.9):
            exact = exact_percentile(values, p)
            got = h.percentile(p)
            self.assertLessEqual(abs(got - exact) / exact, L.MAX_REL_ERROR + 1e-9, (p, exact, got))

    def test_uniform(self):
        rnd = random.Random(1)
        self.check([rnd.randint(10_000, 5_000_000) for _ in range(20_000)])

    def test_lognormal(self):
        rnd = random.Random(2)
        self.check([max(1_000, int(rnd.lognormvariate(14, 1.2))) for _ in range(20_000)])

    def test_constant_and_bimodal(self):
        self.check([2_500_000] * 1000)
        self.check([100_000] * 900 + [40_000_000] * 100)

    def test_merge(self):
        a, b, c = Histogram(), Histogram(), Histogram()
        for i in range(1, 1000):
            (a if i % 2 else b).add(i * 1000)
            c.add(i * 1000)
        a.merge(b)
        self.assertEqual(a.counts, c.counts)
        self.assertEqual((a.count, a.sum, a.min, a.max), (c.count, c.sum, c.min, c.max))

    def test_stats_ms(self):
        h = Histogram()
        h.add(2_000_000)
        s = h.stats()
        self.assertEqual(s["count"], 1)
        self.assertAlmostEqual(s["mean"], 2.0)
        self.assertAlmostEqual(s["p50"], 2.0)
        self.assertAlmostEqual(s["max"], 2.0)


class TestRecorder(unittest.TestCase):
    def test_rollups_and_filters(self):
        r = LatencyRecorder()
        r.record("loop", 1_000_000, "a", "n1")
        r.record("loop", 3_000_000, "a", "n2")
        r.record("loop", 5_000_000, "b", "n1")
        r.record("e2ap_decode", 10_000, None, "n1")
        s = r.snapshot()
        self.assertEqual(s["stages"]["loop"]["count"], 3)
        self.assertEqual(s["by_xapp"]["a"]["loop"]["count"], 2)
        self.assertEqual(s["by_node"]["n1"]["loop"]["count"], 2)
        self.assertEqual(s["by_node"]["n1"]["e2ap_decode"]["count"], 1)
        self.assertNotIn(None, s["by_xapp"])
        self.assertEqual(len(s["series"]), 4)
        f = r.snapshot(xapp="a", node="n2")
        self.assertEqual(f["stages"]["loop"]["count"], 1)
        self.assertEqual(r.snapshot(stage="e2ap_decode")["stages"].keys(), {"e2ap_decode"})
        json.dumps(r.snapshot(buckets=True))  # JSON serialisable
        json.dumps(r.compact())

    def test_thread_safety(self):
        r = LatencyRecorder()
        n_threads, n = 8, 5000

        def work(i):
            for k in range(n):
                r.record("xapp_callback", 1000 + k, f"x{i % 2}", "n")
                r.count("controls_untriggered", f"x{i % 2}", "n")

        ts = [threading.Thread(target=work, args=(i,)) for i in range(n_threads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        s = r.snapshot()
        self.assertEqual(s["stages"]["xapp_callback"]["count"], n_threads * n)
        self.assertEqual(s["counters"]["controls_untriggered"], n_threads * n)
        self.assertEqual(sum(sum(v.counts) for v in r._hists.values()), n_threads * n)

    def test_reset(self):
        r = LatencyRecorder()
        r.record("loop", 1_000_000, "a", "n")
        r.count("controls_triggered", "a", "n")
        r.reset()
        s = r.snapshot()
        self.assertEqual((s["stages"], s["counters"], s["series"]), ({}, {}, []))

    def test_disabled_is_noop(self):
        r = LatencyRecorder(enabled=False)
        r.record("loop", 1, "a", "n")
        self.assertIsNone(r.new_trace(L.now_ns()))
        r.control_sent("a", "n", 1, 0, 10)
        self.assertEqual(r.snapshot()["stages"], {})

    def test_trace_ttl(self):
        r = LatencyRecorder(trace_ttl_s=0.05)
        t0 = L.now_ns()
        tid = r.new_trace(t0)
        self.assertEqual(r.trace_time(tid), t0)
        r.control_sent("a", "n", tid, t0, t0 + 2_000_000)
        time.sleep(0.08)
        self.assertIsNone(r.trace_time(tid))
        r.control_sent("a", "n", tid, t0, L.now_ns())
        r.control_sent("a", "n", None, t0, L.now_ns())
        s = r.snapshot()
        self.assertEqual(s["stages"]["loop"]["count"], 1)
        self.assertAlmostEqual(s["stages"]["loop"]["max"], 2.0)
        self.assertEqual(s["counters"], {"controls_triggered": 1, "controls_trace_expired": 1,
                                         "controls_untriggered": 1})
        self.assertEqual(s["stages"]["control_encode"]["count"], 3)
        # expired entries are purged when new traces are allocated
        r.new_trace(L.now_ns())
        self.assertEqual(r.snapshot()["traces"]["active"], 1)

    def test_trace_map_bounded(self):
        r = LatencyRecorder(max_traces=100)
        t = L.now_ns()
        ids = [r.new_trace(t) for _ in range(1000)]
        self.assertEqual(len(r._traces), 100)
        self.assertIsNone(r.trace_time(ids[0]))
        self.assertEqual(r.trace_time(ids[-1]), t)

    def test_raw_sink(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "lat.jsonl")
            r = LatencyRecorder(log_file=path)
            r.record("loop", 1234, "a", "n", 7)
            r.record("sm_decode", 99, "a", "n")
            r.close()
            with open(path) as f:
                lines = [json.loads(x) for x in f]
            self.assertEqual(len(lines), 2)
            self.assertEqual({k: lines[0][k] for k in ("stage", "ns", "xapp", "node", "trace")},
                             {"stage": "loop", "ns": 1234, "xapp": "a", "node": "n", "trace": 7})
            self.assertIn("T", lines[0]["t"])

    def test_full_sink_drops_instead_of_blocking(self):
        with tempfile.TemporaryDirectory() as d:
            r = LatencyRecorder(log_file=os.path.join(d, "lat.jsonl"), sink_queue=10)
            sink = r._sink
            gate = threading.Event()
            # stall the writer thread so the queue fills up
            orig_get = sink.q.get

            def slow_get(*a, **kw):
                gate.wait(5)
                return orig_get(*a, **kw)
            sink.q.get = slow_get
            t0 = time.perf_counter()
            for i in range(10_000):
                r.record("loop", 1000 + i, "a", "n")
            elapsed = time.perf_counter() - t0
            self.assertLess(elapsed, 2.0)
            self.assertGreater(sink.dropped, 9_000)
            self.assertEqual(r.snapshot()["stages"]["loop"]["count"], 10_000)  # aggregates unaffected
            self.assertGreater(r.snapshot()["raw_log"]["dropped"], 0)
            gate.set()
            r.close()

    def test_errors_are_swallowed(self):
        r = LatencyRecorder()
        with self.assertLogs("uric.latency", "ERROR"):
            r.record("loop", "not a number", "a", "n")  # type: ignore[arg-type]
        r.record("loop", "again", "a", "n")  # logged once only, still no exception


if __name__ == "__main__":
    unittest.main()
