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
 (packages  |        \-------------> XAppContext <-------/              |
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
`kpm-monitor` xApp that ships in the uric package (`uric.xapps.kpm_monitor`). It is not registered automatically, so
name it explicitly. It subscribes to KPM node- and UE-level reports and to RC UE information on every node:

```bash
uric --xapp uric.xapps.kpm_monitor   # inside the RIC
python -m uric.xapps.kpm_monitor --ric 127.0.0.1:4560   # or as a separate process
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
     [--xapp SPEC ...] [--list-xapps] [--log-level INFO]
```

`--xapp` accepts any of these:

* the entry point name of an installed xApp package
* `package.module[:Class]`
* a path to a `.py` file or a package directory

Only the xApps given with `--xapp`, or listed in the configuration file, are loaded. A configuration entry with
`"autostart": false` registers the xApp without starting it, so it can be started later from the dashboard. See
[examples/uric.json](examples/uric.json) for the configuration file format.

## Dashboard

The dashboard is plain HTML, CSS and JS under `uric/dashboard/`. It loads nothing from the internet, so it works offline. It polls the REST API and listens to the SSE event stream. It shows:

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
  stop it. The RIC builds the class with the xApp configuration
  as `config` and calls `start(**config["start_args"])` in a dedicated thread. The xApp code is the same in both modes.

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
| `send_control_request(node, ran_function_id, header, message, ack_request=1)` | raw RIC control |
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

### Reference xApp

[uric/xapps/kpm_monitor.py](uric/xapps/kpm_monitor.py), the `kpm-monitor` xApp, is a complete example of the
API. It subscribes to KPM report style 1 and to style 4 for each slice, and to RC report style 4. Its callbacks decode
with `extract_hdr_info` / `extract_meas_data`, and its `__main__` block lets it run standalone.
[uric/xapps/prb_quotas.py](uric/xapps/prb_quotas.py), the `prb-quotas` xApp, discovers UEs by itself, through KPM report
style 4 for each slice and RC report style 4. It forgets a node's UEs when the node disconnects, and discovers them again
when the node reconnects. Every 5 s it moves each UE's slice-level max PRB quota to the next value in 30, 50, 70, 100 %,
using RC style 2 / action 6. Start it with `uric --xapp uric.xapps.prb_quotas` or `python -m uric.xapps.prb_quotas`.

To build E2SM payloads of your own, use the low-level codecs in `uric.codec.e2ap`, `uric.codec.kpm`, `uric.codec.rc`
and `uric.codec.e2sm`, and send them with `subscribe()` and `send_control_request()`.

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
| `GET /api/events/history`, `GET /api/events` | recent events, and the live stream (SSE) |

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
