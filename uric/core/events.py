"""In-process publish/subscribe event bus used by the RIC, xApps and the API."""

from __future__ import annotations

import asyncio
import collections
import inspect
import logging
import time
from typing import Any, Callable

log = logging.getLogger("uric.events")

# Event names published by the RIC
NODE_CONNECTED = "node.connected"
NODE_DISCONNECTED = "node.disconnected"
NODE_UPDATED = "node.updated"
UE_ADDED = "ue.added"
UE_UPDATED = "ue.updated"
UE_REMOVED = "ue.removed"
SUBSCRIPTION_CREATED = "subscription.created"
SUBSCRIPTION_DELETED = "subscription.deleted"
SUBSCRIPTION_FAILED = "subscription.failed"
INDICATION = "indication"
CONTROL = "control"
ERROR = "error"
XAPP = "xapp"


class EventBus:
    """Fan-out events to sync/async handlers and to async queues (SSE)."""

    def __init__(self, history: int = 500):
        self._handlers: dict[str, list[Callable]] = collections.defaultdict(list)
        self._queues: set[asyncio.Queue] = set()
        self.history: collections.deque = collections.deque(maxlen=history)

    def on(self, event: str, handler: Callable) -> Callable[[], None]:
        """Register ``handler(event_name, data)``. ``event`` may be ``"*"`` or a
        prefix ending in ``.*`` (e.g. ``"node.*"``). Returns an unsubscribe function."""
        self._handlers[event].append(handler)

        def off():
            try:
                self._handlers[event].remove(handler)
            except ValueError:
                pass
        return off

    def queue(self, maxsize: int = 1000) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize)
        self._queues.add(q)
        return q

    def unqueue(self, q: asyncio.Queue) -> None:
        self._queues.discard(q)

    def _match(self, event: str):
        for pattern, hs in list(self._handlers.items()):
            if pattern == event or pattern == "*" or (
                    pattern.endswith(".*") and event.startswith(pattern[:-1])):
                yield from list(hs)

    def emit(self, event: str, data: Any = None, record: bool = True) -> None:
        item = {"ts": time.time(), "event": event, "data": data}
        if record:
            self.history.append(item)
        for q in list(self._queues):
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass
        for h in self._match(event):
            try:
                res = h(event, data)
                if inspect.isawaitable(res):
                    t = asyncio.ensure_future(res)
                    t.add_done_callback(_log_failure)
            except Exception:
                log.exception("event handler for %s failed", event)


def _log_failure(t: asyncio.Future) -> None:
    if not t.cancelled() and t.exception() is not None:
        log.error("async event handler failed", exc_info=t.exception())
