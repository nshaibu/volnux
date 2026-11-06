from datetime import datetime, timezone
from typing import Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import ForeignKeyField, FKConfig, OnDelete

from .enums import NodeType, NodeStatus
from .base import GovernanceModel
from .users import Organization, User


class MeshNode(GovernanceModel):
    """Execution node in the P2P mesh.

    Reverse Relations:
        execution_traces — Execution traces on this node
        heartbeats — Heartbeat history
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="mesh_nodes", on_delete=OnDelete.CASCADE),
    ]
    capacity_max_concurrent_tasks: MiniAnnotated[int, Attrib(default=10, ge=1)]
    registered_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="registered_nodes", on_delete=OnDelete.PROTECT),
    ]

    decommissioned_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="decommissioned_nodes",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    decommissioned_at: Optional[float] = None
    last_heartbeat: Optional[float] = None

    capacity_cpu_cores: MiniAnnotated[Optional[int], Attrib(default=None, ge=1)]
    capacity_memory_gb: MiniAnnotated[Optional[float], Attrib(default=None, gt=0)]
    capacity_disk_gb: MiniAnnotated[Optional[float], Attrib(default=None, gt=0)]
    current_load: MiniAnnotated[int, Attrib(default=0, ge=0)]

    node_type: NodeType = NodeType.GENERAL
    status: NodeStatus = NodeStatus.OFFLINE
    region: Optional[str] = None
    endpoint: Optional[str] = None
    mtls_cert_fingerprint: Optional[str] = None


class NodeHeartbeat(GovernanceModel):
    """Heartbeat record for node health monitoring."""

    node: ForeignKeyField[
        MeshNode,
        FKConfig(reverse_name="heartbeats", on_delete=OnDelete.CASCADE),
    ]
    status: NodeStatus
    current_load: MiniAnnotated[int, Attrib(ge=0)]
    recorded_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]

    cpu_utilization: MiniAnnotated[
        Optional[float], Attrib(default=None, ge=0.0, le=100.0)
    ]
    memory_utilization: MiniAnnotated[
        Optional[float], Attrib(default=None, ge=0.0, le=100.0)
    ]
    disk_utilization: MiniAnnotated[
        Optional[float], Attrib(default=None, ge=0.0, le=100.0)
    ]
