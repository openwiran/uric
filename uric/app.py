"""uric launcher: ``python -m uric [options]``."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
from typing import Optional

from . import __version__
from .api.server import APIServer
from .core.northbound import DEFAULT_XAPP_PORT, NorthboundServer
from .core.ric import NearRTRIC, RICConfig
from .core.transport import E2_SCTP_PORT
from .xapp.manager import XAppManager

log = logging.getLogger("uric")

#: keys of an entry of the "xapps" list of the configuration file
XAPP_ENTRY_KEYS = {"spec", "name", "config", "start_args", "autostart"}


class RICApp:
    """Glue object: RIC core + xApp manager + API server."""

    def __init__(self, config: RICConfig, http_host: str = "0.0.0.0", http_port: int = 8080,
                 xapps: Optional[list[dict]] = None, xapp_host: str = "0.0.0.0",
                 xapp_port: int = DEFAULT_XAPP_PORT):
        self.ric = NearRTRIC(config)
        self.manager = XAppManager(self.ric)
        self.api = APIServer(self.ric, self.manager, http_host, http_port) if http_port >= 0 else None
        self.northbound = NorthboundServer(self.ric, self.manager, xapp_host, xapp_port) if xapp_port >= 0 else None
        self.xapp_specs = xapps or []

    async def start(self) -> None:
        await self.ric.start()
        if self.api:
            await self.api.start()
        if self.northbound:
            await self.northbound.start()
        for x in self.xapp_specs:
            unknown = set(x) - XAPP_ENTRY_KEYS
            if unknown:
                log.warning("xApp entry %s: unknown key(s) %s ignored (allowed: %s)", x.get("spec"),
                            ", ".join(sorted(unknown)), ", ".join(sorted(XAPP_ENTRY_KEYS)))
            try:
                rec = self.manager.load(x["spec"], x.get("name"), x.get("config"), x.get("start_args"))
                if x.get("autostart", True):
                    await self.manager.start(rec.name)
            except Exception as exc:
                log.error("cannot load xApp %s: %s", x.get("spec"), exc)

    async def stop(self) -> None:
        await self.manager.stop_all()
        if self.northbound:
            await self.northbound.stop()
        if self.api:
            await self.api.stop()
        await self.ric.stop()


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog="uric", description="uric - a pure python O-RAN near-RT RIC")
    p.add_argument("--config", help="JSON configuration file")
    p.add_argument("--e2-host", default=None, help="E2 bind address (default 0.0.0.0)")
    p.add_argument("--e2-port", type=int, default=None, help=f"E2 port (default {E2_SCTP_PORT})")
    p.add_argument("--transport", choices=("auto", "sctp", "tcp", "both"), default=None,
                   help="E2 transport: sctp (standard), tcp (dev, length-prefixed), both, or auto")
    p.add_argument("--plmn", default=None, help="RIC PLMN (default 00101)")
    p.add_argument("--ric-id", type=int, default=None, help="20-bit RIC ID (default 1)")
    p.add_argument("--http-host", default=None)
    p.add_argument("--http-port", type=int, default=None, help="dashboard/API port (default 8080, -1 disables)")
    p.add_argument("--xapp-port", type=int, default=None,
                   help=f"xApp interface port for standalone xApps (default {DEFAULT_XAPP_PORT}, -1 disables)")
    p.add_argument("--xapp", action="append", default=[], metavar="SPEC",
                   help="start an xApp: installed entry-point name, module[:Class] or path (repeatable)")
    p.add_argument("--list-xapps", action="store_true", help="list discoverable xApps and exit")
    p.add_argument("--log-level", default=None)
    p.add_argument("--version", action="version", version=f"uric {__version__}")
    return p.parse_args(argv)


def build(args) -> tuple[RICApp, str]:
    cfg: dict = {}
    if args.config:
        with open(args.config) as f:
            cfg = json.load(f)
    rc = cfg.get("ric", {})
    transport = args.transport or rc.get("transport", "auto")
    transports = ("sctp", "tcp") if transport == "both" else (transport,)
    config = RICConfig(
        plmn=str(args.plmn or rc.get("plmn", "00101")),
        ric_id=args.ric_id if args.ric_id is not None else rc.get("ric_id", 1),
        e2_host=args.e2_host or rc.get("e2_host", "0.0.0.0"),
        e2_port=args.e2_port if args.e2_port is not None else rc.get("e2_port", E2_SCTP_PORT),
        e2_transports=transports,
        e2_tcp_port=rc.get("e2_tcp_port"),
        procedure_timeout=rc.get("procedure_timeout", 5.0),
        ue_timeout=rc.get("ue_timeout", 30.0),
        node_retention=rc.get("node_retention", 600.0),
    )
    http = cfg.get("http", {})
    xapps = list(cfg.get("xapps", []))
    for spec in args.xapp:
        xapps.append({"spec": spec})
    app = RICApp(config, args.http_host or http.get("host", "0.0.0.0"),
                 args.http_port if args.http_port is not None else http.get("port", 8080), xapps,
                 cfg.get("xapp_interface", {}).get("host", "0.0.0.0"),
                 args.xapp_port if args.xapp_port is not None else cfg.get("xapp_interface", {}).get(
                     "port", DEFAULT_XAPP_PORT))
    return app, args.log_level or cfg.get("log_level", "INFO")


def main(argv=None) -> None:
    args = parse_args(argv)
    app, level = build(args)
    logging.basicConfig(level=level.upper(), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    if args.list_xapps:
        for x in app.manager.list():
            print(f"{x['name']:24} {x['source']:12} {x['spec']}")
        return

    async def run():
        await app.start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        await stop.wait()
        log.info("shutting down")
        await app.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
