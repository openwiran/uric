# uric — a pure Python O-RAN near-RT RIC

uric is a near-real-time RAN Intelligent Controller that follows the O-RAN WG3 specifications:

* **E2AP** (v3+ ASN.1, as shipped by pycrate) over **SCTP** (port 36421, PPID 70)
* **E2SM-KPM v03.00** — every report style (1–5) and every indication format (1–3)
* **E2SM-RC v01.03** — control (format 1 header/message, outcome decoding), report subscriptions, insert replies, and full RAN function definition decoding
* support for **many E2 nodes and many UEs**, with a UE registry built from KPM and RC reports
* an offline **web dashboard** that shows nodes, their KPM/RC capabilities, UEs, subscriptions, xApps and live events
* an **xApp SDK** modelled on the OSC / srsRAN `oran-sc-ric` Python framework (`xAppBase`, `e2sm_kpm`, `e2sm_rc`). An xApp can run as a **separate process** or **inside the RIC**, and can be shipped as an ordinary **Python package** found through entry points
* an **E2 node simulator** (gNBs with UEs, KPM and RC) for development and tests

It runs on Python ≥ 3.9 and uses only the standard library plus **[pycrate](https://github.com/pycrate-org/pycrate)** for ASN.1 APER.

```
            +--------------------------- uric ----------------------------+
 xApps ---> | XAppManager (embedded xApps)  /  xApp interface TCP :4560   |
 (packages  |        \-------------> XAppContext <-------/                |
 or procs)  |                         |                                   |
            |  REST + SSE API  <-  NearRTRIC core  ->  UE registry        |
 browser -> |  dashboard (static)     |  subscription manager             |
            |                         |  E2 termination (E2AP codec)      |
            +-------------------------|-----------------------------------+
                                      | SCTP :36421 (or TCP for dev)
                     E2 nodes (gNB / CU / DU, or uric.sim)
```

## Quick start

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e .            # installs pycrate, the only dependency
```

Start the RIC. The dashboard is at <http://localhost:8080>:

```bash
uric                      # or: python -m uric
```

The RIC starts **no xApp on its own**. It terminates E2, decodes the node capabilities and serves the dashboard, but
it learns about UEs and KPIs only through the subscriptions that xApps make. To see UEs and node KPIs, start the
`kpm-monitor` xApp that ships in the uric package (`uric.xapps.kpm_monitor_xapp`). It is not registered automatically, so
name it explicitly. It subscribes to KPM node- and UE-level reports and to RC UE information on every node:

```bash
uric --xapp uric.xapps.kpm_monitor_xapp   # inside the RIC
python -m uric.xapps.kpm_monitor_xapp --ric 127.0.0.1:4560   # or as a separate process
```

In another terminal, start three simulated gNBs with 4 UEs each:

```bash
uric-sim --nodes 3 --ues 4    # or: python -m uric.sim ...
```

> **Transport.** E2 runs over SCTP. Linux has kernel SCTP, so `--transport auto` (the default) picks SCTP there.
> macOS and Windows have no kernel SCTP, so `auto` falls back to a **non-standard TCP transport** that prefixes each
> PDU with a 4-byte length. That fallback is only for development with `uric.sim`. Real gNBs (OAI, srsRAN, …) need SCTP,
> so run uric on Linux for them. `--transport both` listens on SCTP and TCP at the same time.

### Command line

```
uric [--config FILE] [--e2-host H] [--e2-port P] [--transport auto|sctp|tcp|both]
     [--plmn 00101] [--ric-id 1] [--http-host H] [--http-port 8080|-1] [--xapp-port 4560|-1]
     [--xapp SPEC ...] [--list-xapps] [--latency | --no-latency] [--latency-log FILE]
     [--log-level INFO]
```

`--xapp` accepts any of these:

* the entry point name of an installed xApp package
* `package.module[:Class]`
* a path to a `.py` file or a package directory

Only the xApps given with `--xapp`, or listed in the configuration file, are loaded. A configuration entry with
`"autostart": false` registers the xApp without starting it, so it can be started later from the dashboard. See
[examples/uric.json](examples/uric.json) for the configuration file format.

## Dashboard

The dashboard is plain HTML, CSS and JS under `uric/dashboard/`. It loads nothing from the internet, so it works offline. It polls the REST API and listens to the SSE event stream. A toggle in the top bar switches between the auto (follow the operating system), light and dark themes; the choice is remembered in the browser. It shows:

* **Overview:** counters, an E2 topology graph (RIC → nodes → UEs), the node list and recent events
* **E2 nodes:** each node's identity, peer and transport, E2AP statistics and E2 node components. For each **RAN function** it shows the decoded capabilities:
  * **KPM:** event trigger styles, and report styles with their formats and supported metrics
  * **RC:** event trigger styles, UE identification parameters, call process types, control styles with actions and **RAN parameter trees**, and report, insert and policy styles

  The node page also shows node KPIs with sparklines, the node's UEs and its subscriptions, plus E2 Service Query, Reset and Removal buttons.
* **UEs:** identity, serving node and cell, and the latest KPM metrics (with trends) and RC parameters
* **Subscriptions:** every RIC subscription, with its owner xApp, style, state and indication count. Each one has a delete button.
* **xApps:** the embedded and remote xApps, with their configuration and live status. Embedded xApps can be started and stopped, and remote ones disconnected. New xApps cannot be loaded from the dashboard: install them as packages, or name them on the command line or in the configuration file.
* **Events:** the live platform event log

## Writing an xApp

The xApp API follows the Python framework of srsRAN's
[oran-sc-ric](https://github.com/srsran/oran-sc-ric) (`xApps/python/lib`). The class names, method names, signatures,
callback arguments and decoded dict layouts are the same, so its xApps port with very few changes.

```python
import signal
from uric.xapp import xAppBase

class MyXapp(xAppBase):
    def __init__(self, config, ric_address=None):
        super().__init__(config, ric_address)

    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg):
        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)
        print(indication_hdr["colletStartTime"])
        for ue, data in meas_data["ueMeasData"].items():         # report style 4 -> format 3
            print(ue, data["measData"])                           # {"RSRP": [-98.5], ...}
            if data["measData"]["RSRP"][-1] < -110:
                self.e2sm_rc.control_slice_level_prb_quota(e2_agent_id, ue, min_prb_ratio=10,
                                                           max_prb_ratio=50, dedicated_prb_ratio=100)

    @xAppBase.start_function            # runs in a thread; the main thread dispatches indications
    def start(self, e2_node_id, metric_names):
        conds = [{"testCondInfo": {"testType": ("sNSSAI", "true"), "testExpr": "equal",
                                   "testValue": ("valueInt", 1)}}]
        self.e2sm_kpm.subscribe_report_service_style_4(e2_node_id, 1000, conds, metric_names, 1000,
                                                       self.my_subscription_callback)

if __name__ == "__main__":
    xapp = MyXapp("")              # config: dict or JSON file path
    signal.signal(signal.SIGINT, xapp.signal_handler)     # stop() deletes every subscription
    xapp.start("gnb_00101_e00", ["RSRP", "DRB.UEThpDl"])
```

### Two ways to run an xApp

* **Standalone process (like OSC).** Run the script. It connects to the RIC's **xApp interface**, which listens on TCP
  4560 by default. Pick the address with `ric_address=` or the `URIC_RIC=host:port` environment variable. uric has no
  RMR and no subscription manager: the xApp interface takes the place of both. A remote xApp shows up in the dashboard
  while it is connected. When it disconnects, its subscriptions are deleted.
* **Inside the RIC.** Install the xApp as a package and declare an entry point, or name it with `--xapp` or in the
  configuration file. Start it with `uric --xapp <name>`; once registered, the dashboard and REST API can start and
  stop it. The RIC builds the class with the entry's `config` as `config`, then calls `start(**start_args)` in a
  dedicated thread. `start_args` comes from the entry's `"start_args"`, which sits next to `"config"` (see below). The
  xApp code is the same in both modes.

  ```json
  "xapps": [
    {"spec": "uric.xapps.kpm_monitor_xapp", "config": {"period_ms": 1000}, "start_args": {}, "autostart": true}
  ]
  ```

  Allowed entry keys are `spec`, `name`, `config`, `start_args` and `autostart`. The RIC logs a warning for any other
  key. For compatibility, `start_args` nested inside `config` is also accepted. The REST call
  `POST /api/xapps/{name}/start` takes the same optional `config` and `start_args`.

```toml
# pyproject.toml
[project]
name = "my-xapp"
dependencies = ["uric"]

[project.entry-points."uric.xapps"]
my-xapp = "my_xapp.app:MyXapp"
```

### API

| `xAppBase` | |
|---|---|
| `@xAppBase.start_function` | decorates the entry point. It sets `self.running`, runs the function in a thread, and dispatches indications to callbacks |
| `subscribe(e2_node_id, ran_function_id, event_trigger_def, action_def, callback, e2sm_type)` | raw subscription. Returns the subscription ID |
| `unsubscribe(sub_id)`, `unsubscribe_all()`, `stop()`, `signal_handler(sig, frame)` | as in OSC |
| `get_e2_nodes()`, `get_e2_node_ids()`, `get_ues(e2_node_id=None)` | RIC state, including node capabilities and UE keys, cells and metrics *(uric extension)* |
| `send_control_request(node, ran_function_id, header, message, ack_request=1, call_process_id=None, *, trace=...)` | raw RIC control. `trace` defaults to the trace ID of the indication being handled *(uric extension)* |
| `current_trace()`, `use_trace(trace)`, `get_latency(e2_node_id=None, stage=None)` | control-loop latency, see [Latency instrumentation](#latency-instrumentation) *(uric extension)* |
| `set_status(**kw)`, `sleep(s)`, `self.config`, `self.log` | dashboard status, a stop-aware sleep, configuration, logging |

Callbacks are called as `callback(e2_agent_id, subscription_id, indication_hdr, indication_msg)`.

| `self.e2sm_kpm` | |
|---|---|
| `subscribe_report_service_style_1(e2_node_id, reportingPeriod, metric_names, granulPeriod, cb)` | node-level measurements |
| `subscribe_report_service_style_2(e2_node_id, reportingPeriod, ue_id, metric_names, granulPeriod, cb)` | one UE |
| `subscribe_report_service_style_3(e2_node_id, reportingPeriod, matchingConds, metric_names, granulPeriod, cb)` | condition-based, UE level |
| `subscribe_report_service_style_4(e2_node_id, reportingPeriod, matchingUeConds, metric_names, granulPeriod, cb)` | every UE that matches the conditions |
| `subscribe_report_service_style_5(e2_node_id, reportingPeriod, ue_ids, metric_names, granulPeriod, cb)` | a list of UEs |
| `extract_hdr_info(hdr)`, `extract_meas_data(msg)` | decode to `{"measData"}`, `{"ueMeasData"}` and `{"granulPeriod"}` |
| `get_supported_metrics(node, style)`, `get_report_styles(node)`, `set_ran_func_id(id)` | node capabilities. RAN function IDs are detected automatically |

| `self.e2sm_rc` | |
|---|---|
| `control_slice_level_prb_quota(e2_node_id, ue_id, min_prb_ratio, max_prb_ratio, dedicated_prb_ratio, ack_request=1)` | style 2 / action 6. Alias: `send_control_request_style_2_action_6` |
| `control_handover(e2_node_id, amf_ue_ngap_id, gnb_cu_ue_f1ap_id, plmn_string, target_nr_cell_id)` | style 3 / action 1. Alias: `send_control_request_style_3_action_1` |
| `handover(e2_node_id, ue_id, target_nr_cell_id, plmn)` | handover using any UE identifier |
| `send_control_request(e2_node_id, ue_id, style, action, ran_parameters)` | any control action. `ran_parameters` is a `{id: value}` tree: a dict is a STRUCTURE, a list is a LIST, `Key(v)` sets the key flag |
| `subscribe_report_service_style_4(e2_node_id, cb)`, `get_control_styles(node)` | UE information reports, and node capabilities |

A UE identifier can be a UE key (`"gNB-UEID:7"`, as returned by `extract_meas_data` and `get_ues`), a UEID CHOICE
tuple, or an integer. An integer is looked up in the RIC UE registry.

How uric differs from the OSC framework:

* The constructor is `xAppBase(config=None, ric_address=None)`. There is no RMR and no xApp HTTP server, so there are
  no `http_server_port`, `rmr_port` or `rmr_flags` arguments. When porting an OSC xApp, drop them from its `__init__`
  and its command-line options.
* RAN function IDs are discovered automatically.
* Control calls wait for the acknowledgement and return `{"success", "cause", "outcome"}`.
* `extract_meas_data` decodes every metric of indication format 2. The OSC framework handles only the first one.

## E2AP procedures

| Procedure | Direction | Support |
|---|---|---|
| E2 Setup | node → RIC | ✔ RAN functions, component configuration, setup failure |
| RIC Service Update / Query | node → RIC / RIC → node | ✔ |
| E2 Node Configuration Update | node → RIC | ✔ add / update / remove acknowledgements |
| RIC Subscription / Delete | RIC → node | ✔ admitted and not-admitted actions |
| RIC Subscription Delete Required | node → RIC | ✔ triggers deletion |
| RIC Indication | node → RIC | ✔ report and insert, decoded per service model |
| RIC Control | RIC → node | ✔ ack / noAck, outcome decoding, call process ID |
| Reset, E2 Removal | both directions | ✔ |
| Error Indication | both directions | ✔ |
| Subscription Modification, Query, Audit, E2 Connection Update | — | not implemented. Answered with an Error Indication. |

## REST API

| Method and path | Description |
|---|---|
| `GET /api/ric` | platform information and counters |
| `GET /api/nodes` | E2 nodes |
| `GET /api/nodes/{id}` | one node with its decoded capabilities, UEs and subscriptions |
| `POST /api/nodes/{id}/query`, `/reset`, `/remove` | E2 Service Query, Reset, Removal |
| `GET /api/ues[?node=]`, `GET /api/ues/{key}` | UE registry |
| `GET /api/subscriptions`, `DELETE /api/subscriptions/{node}/{req}-{inst}` | subscriptions |
| `GET /api/xapps` | xApps (installed, configured and remote) |
| `POST /api/xapps/{name}/start` and `/stop` | start or stop an embedded xApp, or disconnect a remote one |
| `GET /api/latency[?xapp=&node=&stage=&buckets=1]` | control-loop latency histograms in ms (see below) |
| `POST /api/latency/reset` | clear the latency histograms and counters |
| `GET /api/events/history`, `GET /api/events` | recent events, and the live stream (SSE). The stream also carries a `latency` event with the compact rollup every 5 s |

## Latency instrumentation

uric measures how long each stage of the near-RT control loop takes:

```
E2 node ──Indication──▶ RIC ──▶ xApp callback ──▶ RIC ──Control Request──▶ E2 node ──Ack──▶ RIC
```

Every interval uses `time.perf_counter_ns()` timestamps taken in **one process**. The RIC measures its own stages, and
the xApp (embedded or remote) measures its stages and reports the durations to the RIC. No timestamp ever crosses a
process or host boundary, so remote xApps on another machine give correct numbers without clock synchronisation.

| Stage | From → to | Measured by |
|---|---|---|
| `e2ap_decode` | PDU received → E2AP decode done (every PDU) | RIC |
| `sm_decode` | E2SM-KPM/RC decode and UE registry update | RIC |
| `ric_dispatch` | PDU received → indication handed to the xApp (embedded: queued; remote: frame written to the socket) | RIC |
| `loop` | **PDU received → Control Request written to the E2 transport**, for a control triggered by that indication | RIC |
| `control_encode` | `control()` entered → Control Request written | RIC |
| `e2_ack_rtt` | Control Request written → Acknowledge / Failure received (`ack_request=1` only) | RIC |
| `xapp_queue` | indication received by the xApp link → dequeued by the dispatcher | xApp |
| `xapp_callback` | subscription callback, including E2SM unpacking | xApp |
| `xapp_control_call` | `send_control_request()` call → result, as seen by the xApp | xApp |

**Linking a control to its indication.** Each indication gets an opaque integer trace ID, and the RIC keeps its receive
time in a bounded map (10 000 entries, `trace_ttl_s` seconds). While a subscription callback runs, the trace ID is held
in a thread-local, so `send_control_request()`, and with it every `e2sm_rc` helper, sends it with the control
automatically. A control sent from another thread, such as the `start()` thread, carries no trace ID. It is counted as
*untriggered* and gives no `loop` sample. A control whose trace ID is older than `trace_ttl_s` (an overloaded xApp
whose queue holds indications that long) is counted as `controls_trace_expired`, also with no `loop` sample. Check
this counter before reading `loop` percentiles under overload: they leave out the slowest loops. To link such a control anyway, take the ID with `self.current_trace()`
inside the callback, then pass it with `send_control_request(..., trace=tid)` or wrap the call in
`with self.use_trace(tid): ...`.

**Statistics.** Histograms are kept per `(stage, xApp, E2 node)`, with rollups per stage, per xApp and per node. The
buckets are log-spaced from 1 µs to 100 s with ratio 1.1, so a percentile is within 4.9 % of the exact value; count, sum,
min and max are exact. Samples are not kept in memory. Optionally, every raw sample is appended to a JSON-lines file
(`{"t", "stage", "ns", "xapp", "node", "trace"}`) by a background thread. When its bounded queue is full, samples are
dropped and counted (`raw_log.dropped`) rather than slowing the RIC down. xApp-side samples are batched, for embedded
and remote xApps alike, and reach the RIC within about 1 s. `get_latency()` and stopping the xApp flush them at once.

**Configuration.** Instrumentation is on by default. When it is off, each hook costs one boolean check.

```json
"latency": {"enabled": true, "log_file": null, "trace_ttl_s": 10}
```

On the command line: `--latency` / `--no-latency` and `--latency-log FILE`.

**Interfaces.**

* `GET /api/latency` returns `stages`, `by_xapp`, `by_node`, `series` (one entry per key), `counters`
  (`controls_triggered`, `controls_untriggered`, `controls_trace_expired`, also per xApp and per node) and `traces`. Values are milliseconds
  (floats) next to the exact `count`. Filter with `?xapp=NAME&node=ID&stage=S`, and add `buckets=1` to get the
  histogram buckets. `POST /api/latency/reset` clears everything.
* `xAppBase.get_latency()` returns the same snapshot restricted to the calling xApp. Remote xApps use the northbound
  `latency` op.
* The dashboard shows a *Control-loop latency* card on the Overview page (loop p50/p95/p99 against the 10 ms near-RT
  bound, plus a table of every stage), a latency table for each xApp on the xApps page, and reset buttons.
* Northbound protocol additions, all optional: the `hello` answer lists `features`; `indication` events carry `trace`;
  `control` accepts `trace`; a new `timing` op carries batched xApp samples (sent every 256 samples or every 1 s);
  a new `latency` op returns the snapshot. xApps built with an older SDK keep working (their controls count as
  untriggered). A new xApp connected to an older RIC sends none of these and `get_latency()` returns `{}`.

**Benchmark.** `tools/bench_latency.py` starts the RIC, the simulator and the reflex xApp
(`uric.xapps.reflex_xapp`: one RC slice PRB quota control for every KPM indication), each as its own process, and
measures over a fixed window:

```bash
python tools/bench_latency.py --nodes 3 --ues 4 --period-ms 100 --duration 60 --mode both --compare --repeat 2
```

* `--mode embedded|remote|both` runs the xApp inside the RIC or over the northbound interface.
* `--compare` runs every configuration with and without instrumentation and reports the difference in throughput
  (indications handled per second) and in RIC CPU time per indication.
* `--latency-log PREFIX` also writes the raw samples.
* `--min-period-ms` lowers the simulator's 50 ms floor, for saturation runs.

The tool prints the stage table and writes `bench-results/results.json` with the full configuration (seed,
interpreter, platform, git commit), per-run counters, CPU time and complete histograms.

**Measured results.** Reference runs on an Apple M4 Max (macOS, CPython 3.14.4, TCP E2 transport, reflex xApp with
ack). These are indicative only: rerun the benchmark on your own target.

| Setup | Mode | loop p50 / p95 / p99 / max (ms) |
|---|---|---|
| 3 nodes × 4 UEs, 100 ms period (~30 ind/s), 60 s | embedded | 3.8 / 5.6 / 6.1 / 7.5 |
| same | remote | 5.1 / 7.4 / 8.2 / 9.6 |
| 10 nodes, 10 ms period (~965 ind/s), 30 s | embedded | 3.5 / 49.9 / 142 / 159 |
| same | remote | 3.1 / 5.6 / 8.2 / 21.8 |

Cost of the instrumentation, on vs off:

| Load | Mode | Throughput | RIC CPU / indication |
|---|---|---|---|
| ~30 ind/s | embedded / remote | −0.03 % / +0.17 % | +0.2 % / +1.7 % |
| ~965 ind/s (RIC not saturated) | embedded / remote | −0.02 % / +0.06 % | +4.3 % / +4.5 % |
| ~3,600 ind/s offered (saturated) | remote | −0.21 % | +1.2 % |
| ~3,600 ind/s offered (saturated) | embedded | **−4.7 %** (3 runs each) | −0.5 % |

The microbenchmarked cost of one hook is 0.3–0.8 µs, and 0.03 µs when instrumentation is disabled. That adds up to
roughly 5–10 µs per indication.

Caveats when reading these numbers:

* **Overloaded embedded xApps.** This is the only case that misses the 3 % throughput target. The RIC event loop runs at
  about 98 % CPU, and the xApp is saturated, with its queue growing to about 24 s. The embedded xApp's throughput is
  then set by how often it gets the GIL from the event loop, and about 1 % of extra work on the loop costs it about 5 %
  of throughput. xApp-side samples are already batched, which reduced the loss from 7.2 % to 4.7 %. Remote xApps are
  unaffected. For heavy loads, run xApps remotely, or measure with `--no-latency` when saturated embedded throughput
  is what you are measuring.
* **The GIL also limits latency.** At about 1,000 ind/s, the queue of an embedded xApp reaches a p99 of 142 ms
  (`xapp_queue`), while the same xApp run remotely keeps the loop p99 at 8 ms.
* **Expired traces.** Under overload, controls whose indication is older than `trace_ttl_s` are counted as
  `controls_trace_expired` and give no `loop` sample, so the `loop` percentiles leave out the slowest loops. Always
  report this counter next to the percentiles.
* **CPU frequency scaling.** Individual stages ran about 3× faster at ~1,000 ind/s than at ~30 ind/s (for example,
  `e2ap_decode` p50 fell from 0.24 to 0.08 ms), most likely because of CPU frequency scaling. Pin the CPU governor
  or power mode, and state it, when you publish numbers.
* **Synchronised reporting.** With identical periods the simulated nodes report in the same phase, so `xapp_queue`
  (about 2 ms at 30 ind/s) is mostly indications waiting behind each other's callbacks in a burst.
* **Unbounded xApp inbox.** The inbox has no size limit, so a saturated xApp's backlog, and its memory use, grows
  without bound.

## Development

```bash
python -m unittest discover -s tests -v     # codec tests and end-to-end tests (RIC + simulator + xApps + API)
python tools/gen_e2sm.py                    # regenerate uric/codec/_gen from uric/asn1/*.asn1
```

E2AP comes from pycrate's bundled `pycrate_asn1dir.E2AP`. The E2SM-KPM v03.00 and E2SM-RC v01.03 ASN.1 modules are in
`uric/asn1/`, compiled with pycrate's own compiler. pycrate shares one runtime object between all uses of a type, so the
recursive E2SM-RC types (RANParameter-ValueType, -Definition and -Testing) would overwrite each other during nested
encoding. `tools/gen_e2sm.py` avoids this by unrolling those types to 12 levels before compiling. The APER bytes are
unchanged.

## Limitations

* E2SM-RC control header and message **format 2** (multiple actions) can be decoded but have no builder.
  Use `send_control_request()` with your own payload.
* The SCTP transport uses one-to-one sockets with a single association per E2 node, and no multi-homing.
* REAL values are encoded in base 2, because pycrate's base-10 encoder is unreliable. Decoding accepts both bases.

## License

uric is released under the [Apache License, Version 2.0](LICENSE). See the `LICENSE` file for the full text.
