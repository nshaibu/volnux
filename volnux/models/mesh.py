from datetime import datetime, timezone
from typing import Optional

from formax import MiniAnnotated, Attrib, InitStrategy

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    # ForeignKey,
    OnDelete,
    FKConstraint,
)

from .enums import NodeType, NodeStatus
from .base import GovernanceModel
from .users import Organization, User
from .utils import pre_format_timestamps, post_format_timestamps


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
            reverse_name="decommissioned_nodes",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    decommissioned_at: Optional[float] = None
    last_heartbeat: Optional[float] = None

    capacity_cpu_cores: Optional[int] = None
    capacity_memory_gb: Optional[float] = None
    capacity_disk_gb: Optional[float] = None
    current_load: int = 0

    node_type: NodeType = NodeType.GENERAL
    status: NodeStatus = NodeStatus.OFFLINE
    region: Optional[str] = None
    endpoint: Optional[str] = None
    mtls_cert_fingerprint: Optional[str] = None

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True


class NodeHeartbeat(GovernanceModel):
    """Heartbeat record for node health monitoring."""

    node: ForeignKeyField[
        MeshNode,
        FKConfig(reverse_name="heartbeats", on_delete=OnDelete.CASCADE),
    ]
    status: NodeStatus
    current_load: int
    recorded_at: MiniAnnotated[
        float,
        Attrib(
            default_factory=lambda: datetime.now(timezone.utc).timestamp(),
            pre_formatter=pre_format_timestamps,
            post_formatter=post_format_timestamps,
        ),
    ]

    cpu_utilization: Optional[float] = None
    memory_utilization: Optional[float] = None
    disk_utilization: Optional[float] = None

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
