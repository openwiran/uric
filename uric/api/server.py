"""Northbound REST + Server-Sent-Events API and static dashboard server.

A deliberately small HTTP/1.1 implementation on top of asyncio streams so that
uric has no dependency besides pycrate.

Endpoints (all JSON)::

    GET    /api/ric                         platform information and counters
    GET    /api/nodes                       connected / recently seen E2 nodes
    GET    /api/nodes/{id}                  one node, incl. decoded RAN functions
    POST   /api/nodes/{id}/reset            RIC initiated E2 Reset
    POST   /api/nodes/{id}/query            RIC Service Query
    POST   /api/nodes/{id}/remove           RIC initiated E2 Removal
    GET    /api/ues[?node=id]               UE registry
    GET    /api/ues/{key}                   one UE
    GET    /api/subscriptions               active subscriptions
    DELETE /api/subscriptions/{node}/{req}  delete a subscription ({req} = "requestor-instance")
    GET    /api/xapps                       xApps (installed, configured, remote)
    POST   /api/xapps/{name}/start          start an embedded xApp (body: optional config, start_args)
    POST   /api/xapps/{name}/stop           stop an embedded xApp / disconnect a remote one
    GET    /api/events/history              recent platform events
    GET    /api/events                      live event stream (text/event-stream)
"""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import time
import urllib.parse
from typing import Any, Optional

from ..codec.asn import to_jsonable
from ..core.ric import NearRTRIC, RICError
from ..xapp.manager import XAppManager

log = logging.getLogger("uric.api")

DASHBOARD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")

_REASONS = {200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request", 404: "Not Found",
            405: "Method Not Allowed", 409: "Conflict", 500: "Internal Server Error",
            502: "Bad Gateway", 504: "Gateway Timeout"}


class HTTPError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class Request:
    def __init__(self, method: str, target: str, headers: dict, body: bytes):
        self.method = method
        url = urllib.parse.urlsplit(target)
        self.path = urllib.parse.unquote(url.path)
        self.query = {k: v[-1] for k, v in urllib.parse.parse_qs(url.query).items()}
        self.headers = headers
        self.body = body

    def json(self) -> Any:
        if not self.body:
            return {}
        try:
            return json.loads(self.body)
        except ValueError:
            raise HTTPError(400, "invalid JSON body") from None


def _dumps(obj: Any) -> bytes:
    return json.dumps(to_jsonable(obj), default=str).encode()


class APIServer:
    def __init__(self, ric: NearRTRIC, xapps: XAppManager, host: str = "0.0.0.0", port: int = 8080,
                 static_dir: str = DASHBOARD_DIR):
        self.ric, self.xapps = ric, xapps
        self.host, self.port = host, port
        self.static_dir = os.path.abspath(static_dir)
        self.server: Optional[asyncio.base_events.Server] = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._client, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        log.info("dashboard and API on http://%s:%d/", "localhost" if self.host in ("0.0.0.0", "::") else self.host,
                 self.port)

    async def stop(self) -> None:
        if self.server:
            self.server.close()

    # ------------------------------------------------------------------ HTTP

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                try:
                    line = await reader.readline()
                except (ConnectionError, asyncio.LimitOverrunError, ValueError):
                    break
                if not line:
                    break
                try:
                    method, target, version = line.decode("latin-1").strip().split(" ", 2)
                except ValueError:
                    break
                headers = {}
                while True:
                    h = await reader.readline()
                    if h in (b"\r\n", b"\n", b""):
                        break
                    k, _, v = h.decode("latin-1").partition(":")
                    headers[k.strip().lower()] = v.strip()
                n = int(headers.get("content-length", "0") or 0)
                body = await reader.readexactly(n) if n else b""
                req = Request(method.upper(), target, headers, body)
                keep = version == "HTTP/1.1" and headers.get("connection", "").lower() != "close"
                if req.path == "/api/events" and req.method == "GET":
                    await self._sse(writer)
                    break
                status, ctype, payload, extra = await self._handle(req)
                await self._write(writer, status, ctype, payload, keep, extra)
                if not keep:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:
            log.exception("HTTP client error")
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _write(self, writer, status: int, ctype: str, payload: bytes, keep: bool, extra=None) -> None:
        hdrs = [f"HTTP/1.1 {status} {_REASONS.get(status, 'OK')}",
                f"Content-Type: {ctype}", f"Content-Length: {len(payload)}",
                "Cache-Control: no-cache", f"Connection: {'keep-alive' if keep else 'close'}"]
        for k, v in (extra or {}).items():
            hdrs.append(f"{k}: {v}")
        writer.write(("\r\n".join(hdrs) + "\r\n\r\n").encode() + payload)
        await writer.drain()

    async def _sse(self, writer) -> None:
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n"
                     b"Connection: keep-alive\r\n\r\n")
        await writer.drain()
        q = self.ric.events.queue()
        try:
            writer.write(b"retry: 2000\n\n")
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), 15)
                    if item["event"] in ("indication", "ue.updated"):
                        continue  # high rate, dashboard polls state instead
                    writer.write(b"data: " + _dumps(item) + b"\n\n")
                except asyncio.TimeoutError:
                    writer.write(b": keepalive\n\n")
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self.ric.events.unqueue(q)

    async def _handle(self, req: Request):
        try:
            if req.path.startswith("/api/"):
                res = await self._api(req)
                status = 200
                if isinstance(res, tuple):
                    status, res = res
                return status, "application/json", _dumps(res), None
            return self._static(req)
        except HTTPError as exc:
            return exc.status, "application/json", _dumps({"error": exc.message}), None
        except RICError as exc:
            return 502, "application/json", _dumps({"error": str(exc)}), None
        except Exception as exc:
            log.exception("API error on %s %s", req.method, req.path)
            return 500, "application/json", _dumps({"error": str(exc)}), None

    def _static(self, req: Request):
        if req.method not in ("GET", "HEAD"):
            raise HTTPError(405, "method not allowed")
        rel = req.path.lstrip("/") or "index.html"
        path = os.path.abspath(os.path.join(self.static_dir, rel))
        if not path.startswith(self.static_dir + os.sep) or not os.path.isfile(path):
            raise HTTPError(404, "not found")
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript", "image/svg+xml"):
            ctype += "; charset=utf-8"
        with open(path, "rb") as f:
            data = f.read()
        return 200, ctype, data, None

    # ------------------------------------------------------------------ API

    async def _api(self, req: Request):
        parts = [p for p in req.path[len("/api/"):].split("/") if p]
        m = req.method
        ric = self.ric
        if not parts:
            return {"endpoints": __doc__}
        head = parts[0]

        if head == "ric" and m == "GET":
            info = ric.info()
            info["xapps"] = len([x for x in self.xapps.xapps.values() if x.state == "running"])
            info["time"] = time.time()
            return info

        if head == "nodes":
            if len(parts) == 1 and m == "GET":
                return [n.to_dict(len(ric.ues_of(n.id))) for n in ric.nodes.values()]
            node = ric.nodes.get(parts[1])
            if node is None:
                raise HTTPError(404, f"unknown E2 node {parts[1]}")
            if len(parts) == 2 and m == "GET":
                d = node.to_dict(len(ric.ues_of(node.id)))
                d["ues"] = [u.to_dict() for u in ric.ues_of(node.id)]
                d["subscriptions"] = [s.to_dict() for s in ric.subscriptions_of(node_id=node.id)]
                return d
            if len(parts) == 3 and m == "POST":
                if parts[2] == "reset":
                    await ric.reset(node.id)
                    return {"ok": True}
                if parts[2] == "query":
                    r = await ric.service_query(node.id)
                    return {"ok": True, "response": r.name}
                if parts[2] == "remove":
                    await ric.remove_node(node.id)
                    return {"ok": True}
            raise HTTPError(404, "not found")

        if head == "ues":
            if len(parts) == 1 and m == "GET":
                node = req.query.get("node")
                ues = ric.ues_of(node) if node else ric.ues.values()
                return [u.to_dict() for u in ues]
            ue = ric.ues.get(parts[1])
            if ue is None:
                raise HTTPError(404, f"unknown UE {parts[1]}")
            return ue.to_dict()

        if head == "subscriptions":
            if len(parts) == 1 and m == "GET":
                return [s.to_dict() for s in ric.subscriptions.values()]
            if len(parts) == 3 and m == "DELETE":
                try:
                    r, i = (int(x) for x in parts[2].split("-", 1))
                except ValueError:
                    raise HTTPError(400, "subscription id must be node/requestor-instance") from None
                sub = ric.subscriptions.get((parts[1], r, i))
                if sub is None:
                    raise HTTPError(404, "unknown subscription")
                await ric.unsubscribe(sub)
                return {"ok": True}
            raise HTTPError(404, "not found")

        if head == "xapps":
            if len(parts) == 1:
                if m == "GET":
                    return self.xapps.list()
                raise HTTPError(405, "method not allowed")
            name = parts[1]
            if name not in self.xapps.xapps:
                raise HTTPError(404, f"unknown xApp {name}")
            if len(parts) == 2 and m == "GET":
                return self.xapps.xapps[name].to_dict()
            if len(parts) == 3 and m == "POST":
                if parts[2] == "start":
                    body = req.json()
                    try:
                        body = body if isinstance(body, dict) else {}
                        rec = await self.xapps.start(name, body.get("config"), body.get("start_args"))
                    except Exception as exc:
                        raise HTTPError(400, str(exc)) from None
                    return rec.to_dict()
                if parts[2] == "stop":
                    return (await self.xapps.stop(name)).to_dict()
            raise HTTPError(404, "not found")

        if head == "events" and len(parts) == 2 and parts[1] == "history":
            return list(ric.events.history)[-int(req.query.get("limit", 200)):]

        raise HTTPError(404, "not found")
