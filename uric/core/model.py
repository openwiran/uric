"""Data model of the RIC: E2 nodes, RAN functions, UEs and subscriptions."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional

from ..codec import kpm, rc
from ..codec.asn import CodecError, to_jsonable
from ..codec.e2ap import describe_global_node_id
from ..codec.e2sm import describe_cgi, describe_ueid

if TYPE_CHECKING:
    from .transport import Connection


@dataclass
class RANFunction:
    id: int
    oid: str
    revision: int
    definition: bytes
    #: "KPM", "RC" or "UNKNOWN"
    service_model: str = "UNKNOWN"
    #: decoded definition (JSON friendly) or None when unknown/undecodable
    capabilities: Optional[dict] = None
    error: Optional[str] = None

    @classmethod
    def from_item(cls, item: dict) -> "RANFunction":
        f = cls(id=item["ranFunctionID"], oid=item.get("ranFunctionOID", ""),
                revision=item.get("ranFunctionRevision", 0),
                definition=item.get("ranFunctionDefinition", b""))
        f.decode()
        return f

    def decode(self) -> None:
        attempts = []
        if kpm.is_kpm(self.oid):
            attempts.append(("KPM", kpm.decode_ran_function))
        if rc.is_rc(self.oid):
            attempts.append(("RC", rc.decode_ran_function))
        if not attempts:  # unknown OID: try both
            attempts = [("KPM", kpm.decode_ran_function), ("RC", rc.decode_ran_function)]
        for sm, fn in attempts:
            try:
                caps = fn(self.definition)
            except CodecError as exc:
                self.error = str(exc)
                continue
            short = (caps.get("name") or {}).get("short_name") or ""
            if len(attempts) > 1 and sm not in short.upper():
                continue
            self.service_model, self.capabilities, self.error = sm, caps, None
            return

    def to_dict(self) -> dict:
        return {"id": self.id, "oid": self.oid, "revision": self.revision,
                "service_model": self.service_model, "capabilities": self.capabilities,
                "error": self.error, "definition_len": len(self.definition)}


@dataclass
class E2Node:
    id: str
    global_id: Any
    conn: Optional["Connection"] = None
    ran_functions: dict[int, RANFunction] = field(default_factory=dict)
    components: list = field(default_factory=list)
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    connected: bool = True
    stats: dict = field(default_factory=lambda: {"rx": 0, "tx": 0, "indications": 0, "errors": 0})
    #: latest node/cell level KPM values: metric name -> value
    metrics: dict = field(default_factory=dict)
    metrics_ts: float = 0.0

    @property
    def info(self) -> dict:
        return describe_global_node_id(self.global_id)

    def function(self, service_model: str) -> Optional[RANFunction]:
        """First RAN function implementing ``service_model`` ("KPM" / "RC")."""
        for f in self.ran_functions.values():
            if f.service_model == service_model:
                return f
        return None

    def to_dict(self, ues: int = 0) -> dict:
        comps = []
        for c in self.components:
            comps.append({"interface": c.get("e2nodeComponentInterfaceType"),
                          "id": to_jsonable(c.get("e2nodeComponentID"))})
        return {"id": self.id, "info": self.info, "connected": self.connected,
                "peer": self.conn.peer if self.conn else None,
                "transport": self.conn.transport if self.conn else None,
                "connected_at": self.connected_at, "last_seen": self.last_seen,
                "ran_functions": [f.to_dict() for f in self.ran_functions.values()],
                "components": comps, "stats": dict(self.stats), "ue_count": ues,
                "metrics": to_jsonable(self.metrics), "metrics_ts": self.metrics_ts}


@dataclass
class UE:
    key: str
    ue_id: Any
    node_id: str
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    cell: Any = None
    #: latest KPM values: metric name -> value
    metrics: dict = field(default_factory=dict)
    metrics_ts: float = 0.0
    #: latest RC reported RAN parameters
    rc_params: dict = field(default_factory=dict)
    attrs: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"key": self.key, "ue_id": describe_ueid(self.ue_id), "node_id": self.node_id,
                "first_seen": self.first_seen, "last_seen": self.last_seen,
                "cell": describe_cgi(self.cell) if self.cell else None,
                "metrics": to_jsonable(self.metrics), "metrics_ts": self.metrics_ts,
                "rc_params": to_jsonable(self.rc_params), "attrs": to_jsonable(self.attrs)}


@dataclass
class Action:
    id: int
    type: str = "report"  # report | insert | policy
    definition: Optional[bytes] = None
    subsequent: Optional[dict] = None

    def to_item(self) -> dict:
        it: dict[str, Any] = {"ricActionID": self.id, "ricActionType": self.type}
        if self.definition is not None:
            it["ricActionDefinition"] = self.definition
        if self.subsequent:
            it["ricSubsequentAction"] = self.subsequent
        return it


@dataclass
class Subscription:
    node_id: str
    ran_function_id: int
    requestor_id: int
    instance_id: int
    event_trigger: bytes
    actions: list[Action]
    callback: Optional[Callable] = None
    owner: str = "ric"
    service_model: str = "UNKNOWN"
    state: str = "pending"  # pending | active | failed | deleting | deleted
    created_at: float = field(default_factory=time.time)
    admitted: list = field(default_factory=list)
    not_admitted: list = field(default_factory=list)
    indications: int = 0
    last_indication: float = 0.0
    error: Optional[str] = None
    description: dict = field(default_factory=dict)

    @property
    def request_id(self) -> tuple[int, int]:
        return (self.requestor_id, self.instance_id)

    @property
    def id(self) -> str:
        return f"{self.node_id}/{self.requestor_id}-{self.instance_id}"

    def to_dict(self) -> dict:
        return {"id": self.id, "node_id": self.node_id, "ran_function_id": self.ran_function_id,
                "request_id": {"requestor": self.requestor_id, "instance": self.instance_id},
                "owner": self.owner, "service_model": self.service_model, "state": self.state,
                "created_at": self.created_at, "admitted": self.admitted,
                "not_admitted": to_jsonable(self.not_admitted), "indications": self.indications,
                "last_indication": self.last_indication, "error": self.error,
                "description": to_jsonable(self.description)}


@dataclass
class Indication:
    """A RIC indication delivered to a subscription callback."""
    subscription: Subscription
    node_id: str
    ran_function_id: int
    action_id: int
    sn: Optional[int]
    type: str
    header: bytes
    message: bytes
    call_process_id: Optional[bytes] = None
    #: decoded content (KPMReport for KPM, dict for RC) when the SM is known
    decoded: Any = None
    received_at: float = field(default_factory=time.time)
