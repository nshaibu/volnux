"""What a Kubernetes Job and the executor that created it say to each other.

Three records, each in a queue of its own per execution, carried by the
``messaging`` entry of ``DATABASES`` (``MessagingBackendIntegrationMixin``), so the
executor never opens a connection of its own and the backend can be changed in the
settings:

* :class:`K8sJobPayload` — executor -> pod: the workflow parameters and the
  ``volnux.context`` carrier.
* :class:`K8sJobResult` — pod -> executor: the workflow's result.
* :class:`K8sJobError` — pod -> executor: why the workflow failed.

The routes have no ``{node_id}``: the pod is a different node from the executor,
and both sides must reach the same queue. The ``{execution_id}`` placeholder is
filled with ``route_override=execution_route(execution_id)``.
"""

from typing import Any, Dict, Optional

from formax import Attrib, BaseModel, InitStrategy, MiniAnnotated

from volnux.backends.storage_route import StorageRoute
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

__all__ = [
    "K8sJobPayload",
    "K8sJobResult",
    "K8sJobError",
    "execution_route",
]


def execution_route(execution_id: str) -> Dict[str, str]:
    """The ``route_override`` that addresses one execution's queues."""
    return {"execution_id": execution_id}


def _route(kind: str) -> StorageRoute:
    return StorageRoute(
        components=["volnux", "k8s-job", kind, "{execution_id}"],
        routing_keys=lambda: {"execution_id": "default"},
    )


class K8sJobPayload(MessagingBackendIntegrationMixin, BaseModel):
    """Executor -> pod: the workflow's parameters and the context carrier."""

    class Config:
        init_strategy = InitStrategy.DATACLASS

    params: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    carrier: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return _route("payload")


class K8sJobResult(MessagingBackendIntegrationMixin, BaseModel):
    """Pod -> executor: the workflow's result (as JSON values)."""

    class Config:
        init_strategy = InitStrategy.DATACLASS

    result: MiniAnnotated[Any, Attrib(default=None)]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return _route("result")


class K8sJobError(MessagingBackendIntegrationMixin, BaseModel):
    """Pod -> executor: the workflow failed."""

    class Config:
        init_strategy = InitStrategy.DATACLASS

    error: str
    traceback: MiniAnnotated[str, Attrib(default="")]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return _route("error")
