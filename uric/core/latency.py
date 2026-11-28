"""Control-loop latency instrumentation (standard library only).

The near-RT control loop is::

    E2 node --Indication--> RIC --> xApp callback --> RIC --Control Request--> E2 node --Ack--> RIC

:class:`LatencyRecorder` collects durations of the stages of that loop. Every
interval is computed from :func:`time.perf_counter_ns` timestamps taken *in the
same process*: the RIC measures its own stages, xApps (embedded or remote)
measure theirs and report plain durations.

RIC side stages:

==================  ===========================================================
``e2ap_decode``     PDU received -> E2AP decode done
``sm_decode``       E2SM-KPM/RC decode + UE registry update
``ric_dispatch``    PDU received -> indication handed to the xApp
``loop``            PDU received -> Control Request written to the E2 transport
                    (only for controls carrying the trace ID of the indication)
``control_encode``  ``control()`` entered -> Control Request written
``e2_ack_rtt``      Control Request written -> Acknowledge / Failure received
==================  ===========================================================

xApp side stages: ``xapp_queue``, ``xapp_callback``, ``xapp_control_call``.

Aggregates are kept per ``(stage, xapp, node)`` in fixed log-spaced histograms
(1 us .. 100 s, ratio 1.1 -> <= 4.9 % relative error on percentiles), with
count, sum, min and max. Optionally every sample is also appended to a JSONL
file by a background thread (bounded queue, samples are dropped - and counted -
rather than blocking the caller).

The recorder is thread safe: it is called from the asyncio loop and from xApp
threads. Its own failures are logged once and never propagate.
"""

from __future__ import annotations

import collections
import datetime
import json
import logging
import math
import queue
import threading
import time
from typing import Any, Iterable, Optional

log = logging.getLogger("uric.latency")

now_ns = time.perf_counter_ns

RIC_STAGES = ("e2ap_decode", "sm_decode", "ric_dispatch", "loop", "control_encode", "e2_ack_rtt")
XAPP_STAGES = ("xapp_queue", "xapp_callback", "xapp_control_call")
STAGES = RIC_STAGES + XAPP_STAGES

# histogram layout: bucket 0 = underflow (< LO), last = overflow (>= HI)
LO_NS = 1_000                 # 1 us
HI_NS = 100_000_000_000       # 100 s
RATIO = 1.1
_LOG_R = math.log(RATIO)
N_LOG = int(math.ceil(math.log(HI_NS / LO_NS) / _LOG_R))   # 194
N_BUCKETS = N_LOG + 2
_SQRT_R = math.sqrt(RATIO)
#: relative error bound of a percentile computed from a bucket (geometric midpoint)
MAX_REL_ERROR = _SQRT_R - 1.0
PERCENTILES = (50, 95, 99)


def bucket_of(ns: int) -> int:
    if ns < LO_NS:
        return 0
    if ns >= HI_NS:
        return N_BUCKETS - 1
    i = int(math.log(ns / LO_NS) / _LOG_R) + 1
    return i if i < N_BUCKETS - 1 else N_BUCKETS - 2


def bucket_bounds(i: int) -> tuple[float, float]:
    """``[lo, hi)`` of bucket ``i`` in ns."""
    if i == 0:
        return 0.0, float(LO_NS)
    if i == N_BUCKETS - 1:
        return float(HI_NS), math.inf
    return LO_NS * RATIO ** (i - 1), LO_NS * RATIO ** i


class Histogram:
    __slots__ = ("counts", "count", "sum", "min", "max")

    def __init__(self):
        self.counts = [0] * N_BUCKETS
        self.count = 0
        self.sum = 0
        self.min = 0
        self.max = 0

    def add(self, ns: int) -> None:
        self.counts[bucket_of(ns)] += 1
        if self.count == 0 or ns < self.min:
            self.min = ns
        if ns > self.max:
            self.max = ns
        self.count += 1
        self.sum += ns

    def merge(self, other: "Histogram") -> None:
        if other.count == 0:
            return
        c = self.counts
        for i, v in enumerate(other.counts):
            if v:
                c[i] += v
        if self.count == 0 or other.min < self.min:
            self.min = other.min
        if other.max > self.max:
            self.max = other.max
        self.count += other.count
        self.sum += other.sum

    def percentile(self, p: float) -> float:
        """Value (ns) at percentile ``p`` (0..100), from the buckets."""
        if self.count == 0:
            return 0.0
        rank = max(1, int(math.ceil(p / 100.0 * self.count)))
        acc = 0
        for i, v in enumerate(self.counts):
            acc += v
            if acc >= rank:
                if i == 0:
                    return float(self.min)
                if i == N_BUCKETS - 1:
                    return float(self.max)
                mid = LO_NS * RATIO ** (i - 1) * _SQRT_R
                return float(min(max(mid, self.min), self.max))
        return float(self.max)

    def stats(self, buckets: bool = False) -> dict:
        """JSON friendly summary, values in milliseconds."""
        n = self.count
        d: dict[str, Any] = {
            "count": n,
            "mean": (self.sum / n) / 1e6 if n else 0.0,
            "min": self.min / 1e6,
            "max": self.max / 1e6,
            "sum": self.sum / 1e6,
        }
        for p in PERCENTILES:
            d[f"p{p}"] = self.percentile(p) / 1e6
        if buckets:
            # cumulative-friendly sparse form: [[upper bound ms, count], ...]
            d["buckets"] = [[bucket_bounds(i)[1] / 1e6, v] for i, v in enumerate(self.counts) if v]
        return d


class _RawSink:
    """Background JSONL writer fed by a bounded queue."""

    def __init__(self, path: str, maxsize: int = 100_000):
        self.path = path
        self.q: queue.Queue = queue.Queue(maxsize)
        self.written = 0
        self.dropped = 0
        self._f = open(path, "a", buffering=1 << 16)
        self._t = threading.Thread(target=self._loop, name="uric-latency-sink", daemon=True)
        self._t.start()

    def put(self, item: tuple) -> None:
        try:
            self.q.put_nowait(item)
        except queue.Full:
            self.dropped += 1

    def _loop(self) -> None:
        f = self._f
        while True:
            try:
                item = self.q.get(timeout=1.0)
            except queue.Empty:
                f.flush()
                continue
            if item is None:
                break
            wall, stage, ns, xapp, node, trace = item
            try:
                f.write(json.dumps({
                    "t": datetime.datetime.fromtimestamp(wall, datetime.timezone.utc).isoformat(),
                    "stage": stage, "ns": ns, "xapp": xapp, "node": node, "trace": trace}) + "\n")
                self.written += 1
            except Exception:  # pragma: no cover - disk full etc.
                self.dropped += 1
        f.flush()

    def close(self) -> None:
        try:
            self.q.put(None, timeout=2.0)
        except queue.Full:
            pass
        self._t.join(timeout=5.0)
        try:
            self._f.close()
        except Exception:
            pass

    def info(self) -> dict:
        return {"file": self.path, "written": self.written, "dropped": self.dropped, "queued": self.q.qsize()}


class LatencyRecorder:
    """Thread safe latency histograms keyed by ``(stage, xapp, node)``."""

    def __init__(self, enabled: bool = True, log_file: Optional[str] = None, trace_ttl_s: float = 10.0,
                 max_traces: int = 10_000, sink_queue: int = 100_000):
        self.enabled = bool(enabled)
        self.trace_ttl_ns = int(float(trace_ttl_s) * 1e9)
        self.max_traces = int(max_traces)
        self._lock = threading.Lock()
        self._hists: dict[tuple, Histogram] = {}
        self._counters: dict[tuple, int] = {}
        self._traces: "collections.OrderedDict[int, int]" = collections.OrderedDict()
        self._tlock = threading.Lock()
        self._next_trace = 1
        self._expired = 0
        self._since = time.time()
        self._failed_once = False
        self._sink: Optional[_RawSink] = None
        if self.enabled and log_file:
            try:
                self._sink = _RawSink(log_file, sink_queue)
            except OSError as exc:
                log.error("latency log %s: %s (raw samples disabled)", log_file, exc)

    # ------------------------------------------------------------------ internals

    def _fail(self, what: str) -> None:
        if not self._failed_once:
            self._failed_once = True
            log.exception("latency instrumentation error in %s (reported once, ignored)", what)

    # ------------------------------------------------------------------ recording

    def record(self, stage: str, ns: int, xapp: Optional[str] = None, node: Optional[str] = None,
               trace: Optional[int] = None) -> None:
        """Record one duration (ns). Never raises."""
        if not self.enabled:
            return
        try:
            key = (stage, xapp, node)
            with self._lock:
                h = self._hists.get(key)
                if h is None:
                    h = self._hists[key] = Histogram()
                h.add(ns)
            sink = self._sink
            if sink is not None:
                sink.put((time.time(), stage, ns, xapp, node, trace))
        except Exception:
            self._fail("record")

    def record_many(self, xapp: Optional[str], samples: Iterable[tuple]) -> None:
        """Record ``(stage, ns, node, trace)`` samples of one xApp under a single lock
        acquisition (used by xApp threads, to keep contention with the RIC loop low)."""
        if not self.enabled:
            return
        try:
            hists = self._hists
            with self._lock:
                for stage, ns, node, _trace in samples:
                    key = (stage, xapp, node)
                    h = hists.get(key)
                    if h is None:
                        h = hists[key] = Histogram()
                    h.add(ns)
            sink = self._sink
            if sink is not None:
                wall = time.time()
                for stage, ns, node, trace in samples:
                    sink.put((wall, stage, ns, xapp, node, trace))
        except Exception:
            self._fail("record_many")

    def count(self, name: str, xapp: Optional[str] = None, node: Optional[str] = None, n: int = 1) -> None:
        if not self.enabled:
            return
        try:
            key = (name, xapp, node)
            with self._lock:
                self._counters[key] = self._counters.get(key, 0) + n
        except Exception:
            self._fail("count")

    # ------------------------------------------------------------------ traces

    def new_trace(self, rx_ns: int) -> Optional[int]:
        """Allocate a trace ID for an indication received at ``rx_ns``."""
        if not self.enabled:
            return None
        try:
            with self._tlock:
                tid = self._next_trace
                self._next_trace += 1
                tr = self._traces
                tr[tid] = rx_ns
                limit = rx_ns - self.trace_ttl_ns
                while tr:
                    k, v = next(iter(tr.items()))
                    if len(tr) > self.max_traces or v < limit:
                        tr.popitem(last=False)
                        self._expired += 1
                    else:
                        break
            return tid
        except Exception:
            self._fail("new_trace")
            return None

    def trace_time(self, trace: Optional[int], now: Optional[int] = None) -> Optional[int]:
        """Receive time of a live trace ID, or None (unknown / expired)."""
        if trace is None:
            return None
        try:
            with self._tlock:
                t = self._traces.get(trace)
            if t is None:
                return None
            if (now if now is not None else now_ns()) - t > self.trace_ttl_ns:
                return None
            return t
        except Exception:
            self._fail("trace_time")
            return None

    def control_sent(self, xapp: Optional[str], node: Optional[str], trace: Optional[int],
                     t_enter: int, t_sent: int) -> None:
        """Called by ``NearRTRIC.control`` once the Control Request is written."""
        if not self.enabled:
            return
        try:
            self.record("control_encode", t_sent - t_enter, xapp, node, trace)
            rx = self.trace_time(trace, t_sent)
            if trace is None:
                self.count("controls_untriggered", xapp, node)
            elif rx is None:
                # triggered, but the indication is older than the trace TTL (or evicted):
                # no loop sample, counted apart so overload does not look like "untriggered"
                self.count("controls_trace_expired", xapp, node)
            else:
                self.count("controls_triggered", xapp, node)
                self.record("loop", t_sent - rx, xapp, node, trace)
        except Exception:
            self._fail("control_sent")

    # ------------------------------------------------------------------ export

    def reset(self) -> None:
        with self._lock:
            self._hists.clear()
            self._counters.clear()
            self._since = time.time()
        with self._tlock:
            self._expired = 0

    def close(self) -> None:
        if self._sink is not None:
            self._sink.close()
            self._sink = None

    def snapshot(self, xapp: Optional[str] = None, node: Optional[str] = None, stage: Optional[str] = None,
                 series: bool = True, buckets: bool = False) -> dict:
        """JSON serialisable snapshot (milliseconds as floats).

        ``series`` lists every ``(stage, xapp, node)`` key; ``stages``,
        ``by_xapp`` and ``by_node`` are rollups. Filters apply to every part.
        """
        def keep(k: tuple) -> bool:
            return ((stage is None or k[0] == stage) and (xapp is None or k[1] == xapp)
                    and (node is None or k[2] == node))

        with self._lock:
            hists = [(k, h) for k, h in self._hists.items() if keep(k)]
            # copy under the lock so rollups are consistent
            copies = []
            for k, h in hists:
                c = Histogram()
                c.merge(h)
                copies.append((k, c))
            counters = [(k, v) for k, v in self._counters.items()
                        if (xapp is None or k[1] == xapp) and (node is None or k[2] == node)]
            since = self._since
        by_stage: dict[str, Histogram] = {}
        by_xapp: dict[str, dict[str, Histogram]] = {}
        by_node: dict[str, dict[str, Histogram]] = {}
        for (s, x, n), h in copies:
            by_stage.setdefault(s, Histogram()).merge(h)
            if x is not None:
                by_xapp.setdefault(x, {}).setdefault(s, Histogram()).merge(h)
            if n is not None:
                by_node.setdefault(n, {}).setdefault(s, Histogram()).merge(h)

        def order(d: dict) -> list:
            return sorted(d, key=lambda s: (STAGES.index(s) if s in STAGES else len(STAGES), s))

        def cnt_roll(idx: Optional[int]) -> dict:
            out: dict = {}
            for k, v in counters:
                if idx is None:
                    out[k[0]] = out.get(k[0], 0) + v
                elif k[idx] is not None:
                    d = out.setdefault(k[idx], {})
                    d[k[0]] = d.get(k[0], 0) + v
            return out

        with self._tlock:
            traces = {"active": len(self._traces), "expired": self._expired,
                      "ttl_s": self.trace_ttl_ns / 1e9, "max": self.max_traces}
        snap: dict[str, Any] = {
            "enabled": self.enabled,
            "unit": "ms",
            "since": since,
            "time": time.time(),
            "stages": {s: by_stage[s].stats(buckets) for s in order(by_stage)},
            "by_xapp": {x: {s: v[s].stats(buckets) for s in order(v)} for x, v in sorted(by_xapp.items())},
            "by_node": {n: {s: v[s].stats(buckets) for s in order(v)} for n, v in sorted(by_node.items())},
            "counters": cnt_roll(None),
            "counters_by_xapp": cnt_roll(1),
            "counters_by_node": cnt_roll(2),
            "traces": traces,
            "raw_log": self._sink.info() if self._sink is not None else None,
            "histogram": {"lo_ms": LO_NS / 1e6, "hi_ms": HI_NS / 1e6, "ratio": RATIO,
                          "max_rel_error": round(MAX_REL_ERROR, 4)},
        }
        if series:
            snap["series"] = [{"stage": s, "xapp": x, "node": n, **h.stats(buckets)}
                              for (s, x, n), h in sorted(copies, key=lambda kv: tuple(str(p) for p in kv[0]))]
        return snap

    def compact(self) -> dict:
        """Small rollup for the dashboard (SSE ``latency`` event)."""
        s = self.snapshot(series=False)
        keep = ("count", "mean", "p50", "p95", "p99", "max")
        slim = lambda d: {k: d[k] for k in keep}  # noqa: E731
        return {"enabled": s["enabled"], "since": s["since"], "time": s["time"],
                "stages": {k: slim(v) for k, v in s["stages"].items()},
                "by_xapp": {x: {k: slim(v) for k, v in st.items()} for x, st in s["by_xapp"].items()},
                "counters": s["counters"], "counters_by_xapp": s["counters_by_xapp"]}


def merge_samples(samples: Iterable[dict]) -> Histogram:
    """Histogram of a list of ``{"ns": ..}`` samples (offline analysis helper)."""
    h = Histogram()
    for s in samples:
        h.add(int(s["ns"]))
    return h
