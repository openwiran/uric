"""Discovery, loading and lifecycle management of xApps.

xApps are regular python packages. The RIC knows about them through:

* the ``uric.xapps`` entry point group (``pip install`` an xApp package that
  declares ``[project.entry-points."uric.xapps"] my-xapp = "pkg.module:MyXApp"``);
* an explicit spec given on the command line / in the configuration file:
  ``"package.module:ClassName"``, ``"package.module"`` (the module's ``XAPP``
  attribute or its only xAppBase subclass) or a path to a ``.py`` file /
  package directory.

Every embedded xApp is an :class:`~uric.xapp.xAppBase` subclass; it runs in its
own thread and talks to the RIC through a :class:`~uric.xapp.links.LocalLink`.
Standalone xApps connect through :mod:`uric.core.northbound` and are listed
here with ``source == "remote"``.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import inspect
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from importlib import metadata
from typing import Any, Optional

from ..core import events as ev
from ..core.ric import NearRTRIC
from ..core.session import XAppSession
from .links import LocalLink
from .xappbase import _embed, xAppBase

log = logging.getLogger("uric.xapps")

ENTRY_POINT_GROUP = "uric.xapps"

@dataclass
class XAppRecord:
    name: str
    spec: str
    source: str
    cls: Optional[type] = None
    config: dict = field(default_factory=dict)
    instance: Optional[xAppBase] = None
    session: Optional[XAppSession] = None
    state: str = "available"  # available | loaded | running | stopped | failed
    error: Optional[str] = None
    started_at: Optional[float] = None

    def to_dict(self) -> dict:
        cls = self.cls
        ses = self.session
        subs = ses.subscriptions() if ses and self.state == "running" else []
        return {"name": self.name, "spec": self.spec, "source": self.source, "state": self.state,
                "error": self.error, "started_at": self.started_at, "config": self.config,
                "version": getattr(cls, "version", None) if cls else None,
                "description": getattr(cls, "description", "") if cls else "",
                "requestor_id": ses.requestor_id if ses else None,
                "subscriptions": [s.id for s in subs],
                "status": ses.status if ses else {},
                "status_ts": ses.status_ts if ses else None}


def _find_xapp_class(module) -> type:
    obj = getattr(module, "XAPP", None)
    if obj is not None:
        return obj
    found = [o for o in vars(module).values()
             if inspect.isclass(o) and issubclass(o, xAppBase) and o is not xAppBase
             and o.__module__ == module.__name__]
    if len(found) != 1:
        raise ImportError(f"{module.__name__}: expected exactly one xAppBase subclass or an XAPP attribute, "
                          f"found {len(found)}")
    return found[0]


def load_spec(spec: str) -> type:
    """Import an xApp class from a spec string."""
    if os.path.exists(spec):
        path = os.path.abspath(spec)
        if os.path.isdir(path):
            parent, pkg = os.path.split(path.rstrip(os.sep))
            if parent not in sys.path:
                sys.path.insert(0, parent)
            module = importlib.import_module(pkg)
        else:
            name = "uric_xapp_" + os.path.splitext(os.path.basename(path))[0]
            s = importlib.util.spec_from_file_location(name, path)
            if s is None or s.loader is None:
                raise ImportError(f"cannot load {path}")
            module = importlib.util.module_from_spec(s)
            sys.modules[name] = module
            s.loader.exec_module(module)
        return _find_xapp_class(module)
    mod_name, _, attr = spec.partition(":")
    module = importlib.import_module(mod_name)
    if not attr:
        return _find_xapp_class(module)
    obj = module
    for part in attr.split("."):
        obj = getattr(obj, part)
    if not (inspect.isclass(obj) and issubclass(obj, xAppBase)):
        raise ImportError(f"{spec} is not an xAppBase subclass")
    return obj  # type: ignore[return-value]


class XAppManager:
    def __init__(self, ric: NearRTRIC):
        self.ric = ric
        self.xapps: dict[str, XAppRecord] = {}
        self.discover()

    # discovery ---------------------------------------------------------------

    def discover(self) -> None:
        try:
            eps = metadata.entry_points()
            group = eps.select(group=ENTRY_POINT_GROUP) if hasattr(eps, "select") else eps.get(ENTRY_POINT_GROUP, [])
        except Exception as exc:  # pragma: no cover - broken environments
            log.warning("entry point discovery failed: %s", exc)
            group = []
        for ep in group:
            if ep.name not in self.xapps:
                self.xapps[ep.name] = XAppRecord(name=ep.name, spec=ep.value, source="entry-point")

    # loading -----------------------------------------------------------------

    def load(self, spec: str, name: Optional[str] = None, config: Optional[dict] = None) -> XAppRecord:
        """Register an xApp from an entry point name or a spec."""
        rec = self.xapps.get(spec)
        if rec is not None and name in (None, spec):
            if config:
                rec.config = {**rec.config, **config}
            self._import(rec)
            return rec
        cls = load_spec(spec)
        xname = name or getattr(cls, "name", None) or cls.__name__
        rec = self.xapps.get(xname)
        if rec is None or rec.state not in ("running",):
            rec = XAppRecord(name=xname, spec=spec, source="spec", cls=cls, config=dict(config or {}),
                             state="loaded")
            self.xapps[xname] = rec
        return rec

    def _import(self, rec: XAppRecord) -> None:
        if rec.cls is None:
            try:
                rec.cls = load_spec(rec.spec)
                rec.state = "loaded" if rec.state == "available" else rec.state
            except Exception as exc:
                rec.state, rec.error = "failed", f"import error: {exc}"
                raise

    # lifecycle -----------------------------------------------------------------

    async def start(self, name: str, config: Optional[dict] = None) -> XAppRecord:
        rec = self.xapps.get(name)
        if rec is None:
            rec = self.load(name, config=config)
        elif config:
            rec.config = {**rec.config, **config}
        if rec.state == "running":
            return rec
        if rec.source == "remote":
            raise RuntimeError(f"{name} is a remote xApp: start it from its own process")
        self._import(rec)
        assert rec.cls is not None
        if not issubclass(rec.cls, xAppBase):
            rec.state, rec.error = "failed", f"{rec.spec} is not an xAppBase subclass"
            self.notify(rec)
            return rec
        rec.session = XAppSession(self.ric, rec.name)
        return await self._start_xappbase(rec)

    def notify(self, rec: XAppRecord) -> None:
        self.ric.events.emit(ev.XAPP, {"name": rec.name, "state": rec.state, "error": rec.error})

    async def _start_xappbase(self, rec: XAppRecord) -> XAppRecord:
        """Run an OSC style xApp inside the RIC: constructor and ``start()`` run in
        a dedicated thread, RIC calls are bridged to the event loop."""
        loop = asyncio.get_running_loop()
        link = LocalLink(rec.session, loop)
        ready = threading.Event()
        holder: dict = {}
        cls = rec.cls
        config = dict(rec.config)
        start_args = config.get("start_args") or {}

        def construct():
            names = {"config": config, "http_server_port": 8090, "rmr_port": 4560}
            args = []
            for p in list(inspect.signature(cls.__init__).parameters.values())[1:]:
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.default is p.empty:
                    args.append(names.get(p.name))
            inst = cls(*args)
            if not inst.config:
                inst.config = config
            return inst

        def runner():
            _embed.link = link
            try:
                inst = construct()
            except Exception as exc:
                holder["error"] = exc
                ready.set()
                return
            finally:
                _embed.link = None
            holder["inst"] = inst
            ready.set()
            try:
                start = getattr(inst, "start", None)
                if start is None:
                    raise TypeError("xAppBase subclass without a start() method")
                if getattr(start, "_uric_start_function", False):
                    start(**start_args)
                else:
                    inst.running = True
                    start(**start_args)
                    inst._run()
            except SystemExit:
                pass
            except Exception as exc:
                log.exception("xApp %s crashed", rec.name)
                rec.error = str(exc)
                loop.call_soon_threadsafe(self._crashed, rec)

        t = threading.Thread(target=runner, name=f"xapp-{rec.name}", daemon=True)
        t.start()
        await loop.run_in_executor(None, ready.wait, 15)
        if "inst" not in holder:
            rec.state = "failed"
            rec.error = str(holder.get("error", "constructor timed out"))
            log.error("xApp %s failed to start: %s", rec.name, rec.error)
            await rec.session.close()
            self.notify(rec)
            return rec
        rec.instance = holder["inst"]
        rec.state, rec.error, rec.started_at = "running", None, time.time()
        log.info("xApp %s started (requestor id %d)", rec.name, rec.session.requestor_id)
        self.notify(rec)
        return rec

    def _crashed(self, rec: XAppRecord) -> None:
        if rec.state == "running":
            rec.state = "failed"
            asyncio.ensure_future(rec.session.close())
            self.notify(rec)

    async def stop(self, name: str) -> XAppRecord:
        rec = self.xapps[name]
        if rec.state != "running":
            return rec
        if rec.source == "remote":
            close = getattr(rec, "remote_close", None)
            if close:
                close()
            return rec
        rec.state = "stopping"
        try:
            if rec.instance is not None:
                await asyncio.get_running_loop().run_in_executor(None, rec.instance.stop)
        except Exception:
            log.exception("xApp %s: stop failed", name)
        if rec.session:
            await rec.session.close()
        rec.state = "stopped"
        rec.instance = None
        log.info("xApp %s stopped", name)
        self.notify(rec)
        return rec

    async def stop_all(self) -> None:
        await asyncio.gather(*(self.stop(n) for n, r in self.xapps.items() if r.state == "running"),
                             return_exceptions=True)

    def list(self) -> list[dict]:
        return [r.to_dict() for r in self.xapps.values()]
