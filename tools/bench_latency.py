#!/usr/bin/env python3
"""Control-loop latency benchmark for uric.

Starts, as separate processes, the RIC (``python -m uric``), the E2 node
simulator (``python -m uric.sim``) and a reflex xApp
(:mod:`uric.xapps.reflex_xapp`: one RC PRB quota control per KPM indication),
embedded in the RIC or remote (northbound interface). After a warm-up the RIC
latency histograms are reset, the loop runs for ``--duration`` seconds and the
tool prints the stage table and writes ``results.json``.

Throughput is the number of controls sent by the reflex xApp per second (= KPM
indications handled), from the RIC counters read at both ends of the window.
CPU cost is the CPU time of the RIC (and remote xApp) process per indication.

Examples::

    python tools/bench_latency.py --nodes 3 --ues 4 --period-ms 100 --duration 60 --mode embedded
    python tools/bench_latency.py --mode remote --compare --repeat 3      # instrumentation on vs off
    python tools/bench_latency.py --nodes 20 --period-ms 5 --min-period-ms 1 --compare   # saturation

Every run is reproducible: the simulator uses a fixed seed and no UE churn, the
TCP E2 transport is used (portable), and the full configuration (including the
interpreter, platform and git commit) is echoed and stored in the results.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from typing import Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGE_ORDER = ("e2ap_decode", "sm_decode", "ric_dispatch", "xapp_queue", "xapp_callback", "xapp_control_call",
               "control_encode", "loop", "e2_ack_rtt")
XAPP_NAME = "reflex"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(port: int, method: str, path: str, body: Optional[dict] = None, timeout: float = 10.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"null")


def wait_until(cond, timeout: float, what: str, step: float = 0.1):
    end = time.monotonic() + timeout
    last_exc = None
    while time.monotonic() < end:
        try:
            v = cond()
            if v:
                return v
        except Exception as exc:  # noqa: BLE001 - service still starting
            last_exc = exc
        time.sleep(step)
    raise RuntimeError(f"timeout waiting for {what}" + (f" ({last_exc})" if last_exc else ""))


def git_commit() -> Optional[str]:
    try:
        out = subprocess.run(["git", "-C", ROOT, "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5)
        dirty = subprocess.run(["git", "-C", ROOT, "status", "--porcelain"], capture_output=True, text=True,
                               timeout=5).stdout.strip()
        return out.stdout.strip() + ("-dirty" if dirty else "") if out.returncode == 0 else None
    except Exception:  # noqa: BLE001
        return None


class Procs:
    def __init__(self, logdir: str):
        self.logdir = logdir
        self.procs: list[tuple[str, subprocess.Popen]] = []

    def start(self, name: str, args: list[str]) -> subprocess.Popen:
        log = open(os.path.join(self.logdir, f"{name}.log"), "w")
        env = dict(os.environ, PYTHONPATH=ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""),
                   PYTHONHASHSEED="0")
        p = subprocess.Popen([sys.executable, *args], stdout=log, stderr=subprocess.STDOUT, cwd=ROOT, env=env)
        self.procs.append((name, p))
        return p

    def stop(self, name: str, timeout: float = 10.0) -> None:
        for n, p in self.procs:
            if n == name and p.poll() is None:
                p.send_signal(signal.SIGINT)
                try:
                    p.wait(timeout)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()

    def stop_all(self) -> None:
        for n, _ in reversed(self.procs):
            self.stop(n)

    def check(self) -> None:
        for n, p in self.procs:
            if p.poll() is not None:
                raise RuntimeError(f"{n} exited with code {p.returncode} (see {self.logdir}/{n}.log)")


def run_once(a, mode: str, latency: bool, idx: int, outdir: str) -> dict:
    tag = f"run{idx:02d}-{mode}-{'lat' if latency else 'nolat'}"
    logdir = os.path.join(outdir, tag)
    os.makedirs(logdir, exist_ok=True)
    e2p, httpp, xp = free_port(), free_port(), free_port()
    cfg = {
        "ric": {"e2_host": "127.0.0.1", "e2_port": e2p, "transport": "tcp", "procedure_timeout": 5.0,
                "ue_timeout": 0, "node_retention": 600.0},
        "http": {"host": "127.0.0.1", "port": httpp},
        "xapp_interface": {"host": "127.0.0.1", "port": xp},
        "latency": {"enabled": latency, "log_file": (os.path.abspath(a.latency_log) + f".{tag}.jsonl")
                    if (a.latency_log and latency) else None, "trace_ttl_s": a.trace_ttl_s},
        "log_level": a.log_level,
        "xapps": [{"spec": "uric.xapps.reflex_xapp", "name": XAPP_NAME,
                   "config": {"period_ms": a.period_ms, "ack": 0 if a.no_ack else 1},
                   "autostart": False}] if mode == "embedded" else [],
    }
    cfg_path = os.path.join(logdir, "uric.json")
    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2)
    procs = Procs(logdir)
    try:
        procs.start("ric", ["-m", "uric", "--config", cfg_path])
        wait_until(lambda: http(httpp, "GET", "/api/ric"), 15, "RIC API")
        procs.start("sim", ["-m", "uric.sim", "--ric", "127.0.0.1", "--port", str(e2p), "--transport", "tcp",
                            "--nodes", str(a.nodes), "--ues", str(a.ues), "--cells", str(a.cells),
                            "--churn", "0", "--seed", str(a.seed), "--min-period-ms", str(a.min_period_ms),
                            "--log-level", a.log_level])
        wait_until(lambda: http(httpp, "GET", "/api/ric")["nodes"] == a.nodes, 30, f"{a.nodes} E2 nodes")
        if mode == "embedded":
            r = http(httpp, "POST", f"/api/xapps/{XAPP_NAME}/start", {})
            if r.get("state") != "running":
                raise RuntimeError(f"embedded xApp failed: {r.get('error')}")
            xname = XAPP_NAME
        else:
            procs.start("xapp", ["-m", "uric.xapps.reflex_xapp", "--ric", f"127.0.0.1:{xp}",
                                 "--period-ms", str(a.period_ms), "--log-level", a.log_level]
                        + (["--no-ack"] if a.no_ack else []))
            xname = wait_until(lambda: next((x["name"] for x in http(httpp, "GET", "/api/xapps")
                                             if x["source"] == "remote" and x["state"] == "running"), None),
                               15, "remote xApp")
        wait_until(lambda: http(httpp, "GET", "/api/ric")["subscriptions"] >= a.nodes, 15, "subscriptions")
        wait_until(lambda: http(httpp, "GET", "/api/ric")["stats"]["controls"] > 0, 15, "first control")
        time.sleep(a.warmup)
        procs.check()

        def xstatus():
            return next(x for x in http(httpp, "GET", "/api/xapps") if x["name"] == xname)

        http(httpp, "POST", "/api/latency/reset")
        r0, x0, t0 = http(httpp, "GET", "/api/ric"), xstatus(), time.monotonic()
        time.sleep(a.duration)
        r1, x1, t1 = http(httpp, "GET", "/api/ric"), xstatus(), time.monotonic()
        snap = http(httpp, "GET", "/api/latency?buckets=1")
        procs.check()
    finally:
        procs.stop("xapp")
        procs.stop("sim")
        procs.stop("ric")

    win = t1 - t0
    d_ind = r1["stats"]["indications"] - r0["stats"]["indications"]
    d_ctl = r1["stats"]["controls"] - r0["stats"]["controls"]
    d_cpu = r1["cpu_s"] - r0["cpu_s"]
    res = {
        "tag": tag, "mode": mode, "latency_enabled": latency, "window_s": win,
        "indications": d_ind, "controls": d_ctl,
        "offered_ind_per_s": d_ind / win, "handled_ind_per_s": d_ctl / win,
        "ric_cpu_s": d_cpu, "ric_cpu_us_per_ind": d_cpu / d_ind * 1e6 if d_ind else None,
        "ric_cpu_util": d_cpu / win,
        "errors": r1["stats"]["errors"] - r0["stats"]["errors"],
        "xapp_status": x1.get("status"),
        "latency": snap,
    }
    s0, s1 = x0.get("status") or {}, x1.get("status") or {}
    if mode == "remote" and "cpu_s" in s0 and "cpu_s" in s1 and s1.get("handled", 0) > s0.get("handled", 0):
        res["xapp_cpu_us_per_ind"] = (s1["cpu_s"] - s0["cpu_s"]) / (s1["handled"] - s0["handled"]) * 1e6
    return res


def fmt(v, w=10):
    if not isinstance(v, (int, float)):
        return f"{'-':>{w}}"
    return f" {v:>{w - 1}.3f}" if v < 1e5 else f" {v:>{w - 1}.0f}"


def print_run(r: dict, xname: Optional[str] = None) -> None:
    lat = r["latency"]
    print(f"\n== {r['tag']}: {r['mode']}, instrumentation {'on' if r['latency_enabled'] else 'off'} ==")
    print(f"window {r['window_s']:.1f} s | offered {r['offered_ind_per_s']:.1f} ind/s | handled "
          f"{r['handled_ind_per_s']:.1f} ind/s | RIC CPU {r['ric_cpu_util'] * 100:.1f} % "
          f"({fmt(r['ric_cpu_us_per_ind'], 0)} us/ind)"
          + (f" | xApp CPU {r['xapp_cpu_us_per_ind']:.0f} us/ind" if r.get("xapp_cpu_us_per_ind") else ""))
    if not lat.get("enabled"):
        return
    stages = lat["stages"]
    print(f"{'stage':<18}{'count':>9}{'mean':>10}{'p50':>10}{'p95':>10}{'p99':>10}{'max':>10}   (ms)")
    for s in STAGE_ORDER:
        if s in stages:
            v = stages[s]
            print(f"{s:<18}{v['count']:>9}{fmt(v['mean'])}{fmt(v['p50'])}{fmt(v['p95'])}{fmt(v['p99'])}"
                  f"{fmt(v['max'])}")
    c = lat.get("counters", {})
    print(f"controls triggered {c.get('controls_triggered', 0)}, untriggered {c.get('controls_untriggered', 0)}"
          f", trace expired {c.get('controls_trace_expired', 0)}"
          + (f", raw samples dropped {lat['raw_log']['dropped']}" if lat.get("raw_log") else ""))


def summarize(runs: list[dict]) -> dict:
    out: dict = {}
    for mode in sorted({r["mode"] for r in runs}):
        on = [r for r in runs if r["mode"] == mode and r["latency_enabled"]]
        off = [r for r in runs if r["mode"] == mode and not r["latency_enabled"]]
        if not on or not off:
            continue
        mean = lambda xs: sum(xs) / len(xs)  # noqa: E731
        thr_on, thr_off = mean([r["handled_ind_per_s"] for r in on]), mean([r["handled_ind_per_s"] for r in off])
        cpu_on = mean([r["ric_cpu_us_per_ind"] for r in on if r["ric_cpu_us_per_ind"]])
        cpu_off = mean([r["ric_cpu_us_per_ind"] for r in off if r["ric_cpu_us_per_ind"]])
        out[mode] = {"runs_on": len(on), "runs_off": len(off),
                     "throughput_on": thr_on, "throughput_off": thr_off,
                     "throughput_delta_pct": (thr_on - thr_off) / thr_off * 100 if thr_off else None,
                     "ric_cpu_us_per_ind_on": cpu_on, "ric_cpu_us_per_ind_off": cpu_off,
                     "ric_cpu_delta_pct": (cpu_on - cpu_off) / cpu_off * 100 if cpu_off else None}
    return out


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nodes", type=int, default=3)
    p.add_argument("--ues", type=int, default=4, help="UEs per node")
    p.add_argument("--cells", type=int, default=2, help="cells per node")
    p.add_argument("--period-ms", type=int, default=100, help="KPM reporting period")
    p.add_argument("--min-period-ms", type=int, default=50, help="simulator lower bound of the period")
    p.add_argument("--duration", type=float, default=60.0, help="measurement window [s]")
    p.add_argument("--warmup", type=float, default=5.0, help="warm-up before the window [s]")
    p.add_argument("--mode", choices=("embedded", "remote", "both"), default="embedded")
    p.add_argument("--latency", action=argparse.BooleanOptionalAction, default=True,
                   help="RIC latency instrumentation (default on)")
    p.add_argument("--compare", action="store_true", help="run with and without instrumentation")
    p.add_argument("--repeat", type=int, default=1, help="repetitions of each configuration (interleaved)")
    p.add_argument("--no-ack", action="store_true", help="controls without ack request")
    p.add_argument("--seed", type=int, default=1, help="simulator seed")
    p.add_argument("--trace-ttl-s", type=float, default=10.0)
    p.add_argument("--latency-log", default=None, metavar="PREFIX",
                   help="write raw samples to PREFIX.<run>.jsonl")
    p.add_argument("--out-dir", default="bench-results", help="logs and per run configuration")
    p.add_argument("--out", default=None, help="results file (default OUT_DIR/results.json)")
    p.add_argument("--log-level", default="WARNING")
    a = p.parse_args(argv)

    modes = ("embedded", "remote") if a.mode == "both" else (a.mode,)
    lats = (True, False) if a.compare else (a.latency,)
    plan = [(m, l) for _ in range(a.repeat) for m in modes for l in lats]
    os.makedirs(a.out_dir, exist_ok=True)
    out = a.out or os.path.join(a.out_dir, "results.json")
    sys.path.insert(0, ROOT)
    from uric import __version__  # noqa: E402

    config = {"args": vars(a), "plan": [{"mode": m, "latency": l} for m, l in plan],
              "python": sys.version, "implementation": platform.python_implementation(),
              "platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count(),
              "uric_version": __version__, "git_commit": git_commit(),
              "started": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    print("configuration:")
    print(json.dumps(config, indent=2))

    runs = []
    for i, (mode, lat) in enumerate(plan, 1):
        print(f"\n[{i}/{len(plan)}] mode={mode} instrumentation={'on' if lat else 'off'} "
              f"({a.warmup:.0f} s warm-up + {a.duration:.0f} s)...", flush=True)
        r = run_once(a, mode, lat, i, a.out_dir)
        runs.append(r)
        print_run(r)
    summary = summarize(runs)
    if summary:
        print("\n== instrumentation overhead (on vs off) ==")
        for mode, s in summary.items():
            print(f"{mode:<9} throughput {s['throughput_on']:.2f} vs {s['throughput_off']:.2f} ind/s "
                  f"({s['throughput_delta_pct']:+.2f} %) | RIC CPU {s['ric_cpu_us_per_ind_on']:.0f} vs "
                  f"{s['ric_cpu_us_per_ind_off']:.0f} us/ind ({s['ric_cpu_delta_pct']:+.1f} %)")
    with open(out, "w") as f:
        json.dump({"config": config, "runs": runs, "overhead": summary}, f, indent=1)
    print(f"\nresults written to {out}")


if __name__ == "__main__":
    main()
