"""Per-xApp session inside the RIC (internal).

A session owns the subscriptions created on behalf of one xApp (embedded or
remote) under its own RIC Requestor ID, keeps its published status and
releases everything when the xApp goes away.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from .model import Action, Subscription
from .ric import ControlResult, NearRTRIC


class XAppSession:
    def __init__(self, ric: NearRTRIC, name: str):
        self.ric = ric
        self.name = name
        self.log = logging.getLogger(f"xapp.{name}")
        self.status: dict[str, Any] = {}
        self.status_ts: float = 0.0
        self._closed = False

    @property
    def requestor_id(self) -> int:
        return self.ric.requestor_id(self.name)

    async def subscribe(self, node_id: str, ran_function_id: int, event_trigger: bytes,
                        actions: list[Action], callback: Optional[Callable] = None,
                        description: Optional[dict] = None) -> Subscription:
        return await self.ric.subscribe(node_id, ran_function_id, event_trigger, actions, callback,
                                        owner=self.name, description=description)

    async def unsubscribe(self, sub: Subscription) -> None:
        await self.ric.unsubscribe(sub)

    def subscriptions(self) -> list[Subscription]:
        return self.ric.subscriptions_of(owner=self.name)

    async def control(self, node_id: str, ran_function_id: int, header: bytes, message: bytes,
                      ack: bool = True, call_process_id: Optional[bytes] = None) -> ControlResult:
        return await self.ric.control(node_id, ran_function_id, header, message, ack=ack,
                                      call_process_id=call_process_id, owner=self.name)

    def set_status(self, **kw) -> None:
        self.status.update(kw)
        self.status_ts = time.time()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for sub in self.subscriptions():
            try:
                await self.ric.unsubscribe(sub)
            except Exception as exc:
                self.log.warning("could not delete subscription %s: %s", sub.id, exc)
