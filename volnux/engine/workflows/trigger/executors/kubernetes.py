"""
KubernetesWorkflowExecutor — dispatches workflow execution as Kubernetes Jobs.

Architecture
------------
                API / TriggerEngine process
                ┌────────────────────────────────────────┐
                │ KubernetesWorkflowExecutor             │
                │   execute()                            │
                │   ├─ push K8sJobPayload                │──► messaging backend
                │   ├─ create_namespaced_job()           │──► K8s API (thread)
                │   └─ _poll_job_status()                │
                │        └─ read_namespaced_job() loop   │──► K8s API (thread)
                │   └─ _read_outcome()                   │◄── messaging backend
                └────────────────────────────────────────┘

                Kubernetes Job pod (separate node, separate process)
                ┌───────────────────────────────────────┐
                │ volnux-job-runner                      │
                │   └─ run_workflow() under the carried  │
                │      volnux context                    │
                │   └─ push K8sJobResult / K8sJobError   │──► messaging backend
                │   └─ exit 0 / exit 1                  │
                └───────────────────────────────────────┘

Key design decisions
--------------------
D1. All K8s API calls run in a thread via run_in_executor.
    The kubernetes Python client uses urllib3 (blocking HTTP). Every API
    call must run in a thread pool thread to avoid blocking the event loop.

D2. Results travel through the messaging backend, not pod stdout.
    Pod logs are ephemeral, subject to rotation, and not available immediately
    after pod exit. The pod pushes a K8sJobResult (or K8sJobError) onto a queue
    of its own before exiting; the executor pops it after job completion. The
    queues are models of ``MessagingBackendIntegrationMixin`` (see
    ``k8s_messages``), so the backend is the ``messaging`` entry of ``DATABASES``
    and can be changed in the settings; this module opens no connection itself.
    Every queue has a TTL as a safety net and is deleted in ``finally``.

D3. Params go through the messaging backend, not environment variables.
    Large workflow params (embeddings, document content, batch configs) can
    exceed environment variable size limits. The pod is told the execution id
    and pops its K8sJobPayload at startup. To reach the backend it needs
    ``DATABASES``: the executor puts it in the pod's ``VOLNUX_DATABASES``
    (passwords removed; see ``_database_env``).

D4. Job names are K8s-compliant: ≤ 63 chars, unique, URL-safe.
    Pod labels are limited to 63 characters. Job names are used as label
    values. Names are truncated to fit, and a UUID suffix provides uniqueness.

D5. Jobs are cleaned up on all exit paths.
    execute() has a finally block that deletes the Job regardless of whether
    it succeeded, failed, or raised an exception. TTL is a fallback, not
    the primary cleanup mechanism.

D6. Status is polled via run_in_executor, not a blocking watch.
    kubernetes.watch.Watch().stream() is a synchronous blocking generator.
    Using it in async def blocks the event loop. Status polling via
    non-blocking read_namespaced_job() + asyncio.sleep() is correct.

D7. Pod uses a console script, not an embedded script string.
    `volnux-job-runner` (``job_runner_main``, registered in pyproject.toml) is
    the container's command. The embedded script approach is fragile
    (indentation, implicit convention, no syntax checking).

D9. The pod runs under the dispatcher's volnux context.
    The K8sJobPayload holds ``params`` and ``carrier`` (``__volnux_context__``). The
    carrier is the non-secret configuration and the ids of the process that
    created the Job (``volnux.context``); the runner activates it around the
    workflow, so the workflow sees the same settings as it would in-process.
    Secrets are never carried: they come from ``env_secret_name``.

D8. Pending jobs are tracked for cancellation.
    execute() registers the job name before the Job is created and
    deregisters it in finally. delete_job() is the cancellation mechanism.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from volnux.config import setting
from volnux.context import (
    CARRIER_KEY,
    extract_carrier,
    get_current_correlation_id,
    inject_carrier,
    volnux_context,
)
from volnux.engine.workflows.workflow import WorkflowExecutionError

from .base import WorkflowExecutor
from .k8s_messages import (
    K8sJobError,
    K8sJobPayload,
    K8sJobResult,
    execution_route,
)

if TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)

__all__ = ["KubernetesWorkflowExecutor"]

# How long the executor waits for a result that is not there yet when the Job
# has finished (replication lag on a remote backend; the pod pushed it first).
_RESULT_GRACE_SECONDS = 2.0

# How long the pod waits for its payload (the executor pushed it before creating
# the Job, so normally it is there at once).
_PAYLOAD_WAIT_SECONDS = 30.0


def _key_ttl_seconds() -> int:
    """Seconds the queues of a job live (the K8S_KEY_TTL_SECONDS setting): long
    enough for any reasonable job."""
    return int(setting("K8S_KEY_TTL_SECONDS", 3600))


def _strip_passwords(databases: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """A copy of ``DATABASES`` without its ``PASSWORD`` values."""
    return {
        alias: {k: v for k, v in entry.items() if k != "PASSWORD"}
        for alias, entry in databases.items()
    }


class KubernetesWorkflowExecutor(WorkflowExecutor):
    """
    Dispatches workflow execution as Kubernetes Jobs.

    Each workflow execution creates a Kubernetes Job. The Job pod runs
    ``volnux-job-runner`` with the workflow name and params. Results
    are communicated through the messaging backend rather than pod logs.

    Prerequisites
    -------------
    The container image must have Volnux installed (it provides the
    ``volnux-job-runner`` command). The pod must have network access to the same
    messaging backend as the executor process (``DATABASES['messaging']``).

    Setup
    -----
    In-cluster (from within a K8s pod):
        executor = KubernetesWorkflowExecutor(
            workflow_registry = ...,
            namespace = "volnux",
            image = "your-registry/volnux-engine:2.0.0",
        )

    Out-of-cluster (local dev, kube_config_file provided):
        executor = KubernetesWorkflowExecutor(
            workflow_registry = ...,
            kube_config_file  = "~/.kube/config",
        )

    Cancellation
    ------------
    Use ``delete_job(job_name)`` to stop a running Job.
    Job names are available from ``get_active_jobs()``.
    """

    def __init__(
        self,
        workflow_registry: "WorkflowRegistry",
        namespace: Optional[str] = None,
        image: Optional[str] = None,
        image_pull_policy: Optional[str] = None,
        service_account: Optional[str] = None,
        resource_requests: Optional[Dict[str, str]] = None,
        resource_limits: Optional[Dict[str, str]] = None,
        env_secret_name: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        node_selector: Optional[Dict[str, str]] = None,
        tolerations: Optional[List[dict]] = None,
        active_deadline_seconds: Optional[int] = None,
        ttl_seconds_after_finished: Optional[int] = None,
        backoff_limit: Optional[int] = None,
        poll_interval: Optional[float] = None,
        kube_config_file: Optional[str] = None,
    ):
        """
        Initialise the Kubernetes workflow executor.

        Args:
            workflow_registry:
                Registry for resolving workflow names to WorkflowConfig classes.

            namespace:
                Kubernetes namespace where Jobs are created.

            image:
                Container image with the Volnux engine and ``volnux`` CLI.
                Must be accessible from the cluster's container registry.

            image_pull_policy:
                ``"Always"``, ``"IfNotPresent"``, or ``"Never"``.
                Use ``"Always"`` in production.

            service_account:
                Kubernetes ServiceAccount for the Job pod. Must have
                permissions required by the workflows (e.g. RBAC for
                creating sub-jobs, accessing secrets).

            resource_requests:
                CPU and memory requests (e.g. ``{"cpu": "1", "memory": "2Gi"}``).

            resource_limits:
                CPU and memory limits. Should be set in production to prevent
                runaway workflows from consuming unbounded node resources.

            env_secret_name:
                Name of a Kubernetes Secret whose keys are mounted as
                environment variables in the Job pod. Used to pass database
                passwords, API keys, and other secrets without hardcoding.
                The pod gets ``DATABASES`` from ``VOLNUX_DATABASES``; the
                executor sets it without passwords. If ``DATABASES`` has a
                ``PASSWORD`` and this Secret is given, the Secret must supply
                ``VOLNUX_DATABASES`` (with the passwords) and the executor sets
                none.

            env_vars:
                Additional plain-text environment variables. Do not put
                secrets here — use ``env_secret_name`` for sensitive values.

            node_selector:
                Node label selector for pod scheduling. Use to target
                GPU nodes, high-memory nodes, or specific availability zones.

            tolerations:
                Pod tolerations. Each dict maps to a ``V1Toleration`` field.

            active_deadline_seconds:
                Maximum Job duration in seconds. The Job is failed and the
                pod is terminated after this many seconds. None = no limit.

            ttl_seconds_after_finished:
                Seconds after which Kubernetes deletes completed Jobs.
                Requires TTL Controller to be enabled on the cluster.
                ``execute()`` also deletes Jobs explicitly in its finally block,
                making TTL a safety net rather than the primary mechanism.

            backoff_limit:
                Number of pod restarts before the Job is marked as failed.
                Default 2 handles transient infrastructure failures (node
                eviction, OOMKill) without conflicting with RetryMixin,
                which handles application-level retries within a pod's lifetime.

            poll_interval:
                Seconds between K8s API status checks. Lower = more responsive
                at the cost of higher API server load.

            kube_config_file:
                Path to a kubeconfig file. If None, tries in-cluster config
                first, then the default kubeconfig at ~/.kube/config.

        """
        super().__init__(workflow_registry)

        # (None: the K8S_NAMESPACE / K8S_IMAGE / K8S_IMAGE_PULL_POLICY settings)
        self._namespace = namespace or setting("K8S_NAMESPACE", "default")
        self._image = image or setting("K8S_IMAGE", "volnux-engine:latest")
        self._image_pull_policy = image_pull_policy or setting(
            "K8S_IMAGE_PULL_POLICY", "IfNotPresent"
        )
        self._service_account = service_account
        self._resource_requests = resource_requests or {}
        self._resource_limits = resource_limits or {}
        self._env_secret_name = env_secret_name
        self._env_vars = env_vars or {}
        self._node_selector = node_selector
        self._tolerations = tolerations
        self._active_deadline_seconds = active_deadline_seconds
        self._ttl_seconds_after_finished = int(
            ttl_seconds_after_finished
            if ttl_seconds_after_finished is not None
            else setting("K8S_JOB_TTL_SECONDS", 3600)
        )
        self._backoff_limit = int(
            backoff_limit
            if backoff_limit is not None
            else setting("K8S_BACKOFF_LIMIT", 2)
        )
        self._poll_interval = float(
            poll_interval
            if poll_interval is not None
            else setting("K8S_POLL_INTERVAL", 2.0)
        )
        self._database_env = self._pod_databases()

        # Track submitted jobs for observability and cancellation
        # Maps execution_id → job_name
        self._pending: Dict[str, str] = {}

        # Initialise Kubernetes clients (synchronous — done once at construction)
        self._batch_client = self._init_k8s_clients(kube_config_file)

    async def execute(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        *,
        execution_id: Optional[str] = None,
    ) -> Any:
        """
        Create a Kubernetes Job to execute a workflow and await its result.

        Execution flow:
            1. Push the params and the context carrier (avoids env var size limits)
            2. Create K8s Job (non-blocking via run_in_executor)
            3. Poll Job status until succeeded/failed (non-blocking)
            4. Read the pod's result (or error) from the messaging backend
            5. Delete Job (always — even on failure)

        Args:
            workflow_name: Name of the workflow to execute.
            params:        Workflow parameters. Not size-limited (sent as a message).

        Returns:
            The workflow result.

        Raises:
            WorkflowExecutionError: If the Job fails, is deleted, or times out.
        """

        execution_id = _new_execution_id()
        job_name = _make_job_name(workflow_name, execution_id)

        self._pending[execution_id] = job_name
        logger.info(
            "Creating K8s Job %r for workflow %r — execution_id=%s namespace=%s",
            job_name,
            workflow_name,
            execution_id,
            self._namespace,
        )

        loop = asyncio.get_running_loop()

        # The payload is a new dict (``params`` is not touched). Its carrier is the
        # context of this process, which the Job runs under.
        with volnux_context(
            workflow_id=workflow_name,
            execution_id=execution_id,
            correlation_id=get_current_correlation_id() or execution_id,
        ):
            carrier = inject_carrier({})[CARRIER_KEY]

        try:

            # Avoids environment variable size limits for large param payloads.
            # The queue expires after the K8S_KEY_TTL_SECONDS setting whether the
            # job reads it or not.
            await self._write_payload(execution_id, params, carrier)

            job = self._build_job_spec(
                workflow_name=workflow_name,
                job_name=job_name,
                execution_id=execution_id,
            )
            await loop.run_in_executor(
                None,
                lambda: self._batch_client.create_namespaced_job(
                    namespace=self._namespace,
                    body=job,
                ),
            )
            logger.debug("K8s Job %r created — execution_id=%s", job_name, execution_id)

            await self._poll_job_status(
                job_name=job_name,
                execution_id=execution_id,
            )

            result = await self._read_outcome(
                workflow_name=workflow_name, execution_id=execution_id
            )

            logger.info(
                "Workflow %r completed on K8s Job %r — execution_id=%s",
                workflow_name,
                job_name,
                execution_id,
            )
            return result

        except WorkflowExecutionError:
            raise
        except Exception as exc:
            logger.error(
                "Workflow %r failed — job=%r execution_id=%s: %s",
                workflow_name,
                job_name,
                execution_id,
                exc,
                exc_info=True,
            )
            raise WorkflowExecutionError(
                f"K8s workflow execution failed for {workflow_name!r} "
                f"(job={job_name}, execution_id={execution_id}): {exc}"
            ) from exc

        finally:
            # Clean up — always runs, regardless of outcome
            self._pending.pop(execution_id, None)
            await self._delete_job(job_name)
            await self._delete_messages(execution_id)

    async def delete_job(self, job_name: str) -> bool:
        """
        Delete a Kubernetes Job and its pod(s).

        Equivalent of revoke() for K8s Jobs. Uses ``propagation_policy="Foreground"``
        to ensure the pod is terminated before the Job resource is deleted.

        Args:
            job_name: The K8s Job name (from ``get_active_jobs()``).

        Returns:
            True if the Job was found and deleted, False otherwise.
        """
        return await self._delete_job(job_name, propagation="Foreground")

    async def get_active_jobs(self) -> Dict[str, Any]:
        """
        List active Volnux workflow Jobs in the configured namespace.

        Non-blocking — runs the K8s API list call in a thread.

        Returns:
            Dict with namespace, job count, and per-job details.
        """
        loop = asyncio.get_running_loop()
        try:
            jobs = await loop.run_in_executor(
                None,
                lambda: self._batch_client.list_namespaced_job(
                    namespace=self._namespace,
                    label_selector="app=volnux",
                ),
            )
        except Exception as exc:
            logger.warning("get_active_jobs failed: %s", exc)
            return {"namespace": self._namespace, "error": str(exc)}

        active = []
        for job in jobs.items:
            s = job.status
            active.append(
                {
                    "name": job.metadata.name,
                    "workflow": job.metadata.labels.get("workflow", "unknown"),
                    "execution_id": job.metadata.labels.get("execution-id", "unknown"),
                    "active": s.active or 0,
                    "succeeded": s.succeeded or 0,
                    "failed": s.failed or 0,
                    "start_time": s.start_time.isoformat() if s.start_time else None,
                }
            )

        return {
            "namespace": self._namespace,
            "active_jobs": len([j for j in active if j["active"] > 0]),
            "total_jobs": len(active),
            "jobs": active,
        }

    def get_pending_executions(self) -> Dict[str, str]:
        """
        Return currently tracked pending executions.

        Returns:
            Dict mapping execution_id → job_name.
        """
        return dict(self._pending)

    async def _poll_job_status(
        self,
        job_name: str,
        execution_id: str,
    ) -> None:
        """
        Poll the Kubernetes Job status until it reaches a terminal state.

        Uses non-blocking read_namespaced_job() calls wrapped in
        run_in_executor — the event loop is never blocked between polls.

        Raises:
            WorkflowExecutionError: If the Job failed, was deleted, or timed out.
        """

        from kubernetes.client import ApiException

        loop = asyncio.get_running_loop()
        started_at = time.monotonic()

        while True:
            await asyncio.sleep(self._poll_interval)

            # Check active_deadline_seconds — defensive wall clock check.
            # Kubernetes enforces this too, but we surface it early.
            if (
                self._active_deadline_seconds is not None
                and time.monotonic() - started_at > self._active_deadline_seconds
            ):
                raise WorkflowExecutionError(
                    f"K8s Job {job_name!r} exceeded deadline of "
                    f"{self._active_deadline_seconds}s (execution_id={execution_id})"
                )

            try:
                job = await loop.run_in_executor(
                    None,
                    lambda: self._batch_client.read_namespaced_job(
                        name=job_name,
                        namespace=self._namespace,
                    ),
                )
            except ApiException as exc:
                if exc.status == 404:
                    raise WorkflowExecutionError(
                        f"K8s Job {job_name!r} was deleted before completing "
                        f"(execution_id={execution_id})"
                    )
                logger.warning(
                    "K8s API error while polling job %r: %s — retrying",
                    job_name,
                    exc,
                )
                continue

            status = job.status

            if status.succeeded and status.succeeded > 0:
                logger.debug(
                    "Job %r succeeded — execution_id=%s", job_name, execution_id
                )
                return  # ← poll loop exits, caller reads the result message

            # A pod that failed is retried up to ``backoff_limit`` times. The Job
            # has failed only when Kubernetes says so (a "Failed" condition), not
            # at the first failed pod, or the retries could never run.
            if self._job_has_failed(status):
                message = self._extract_failure_message(status)
                raise WorkflowExecutionError(
                    f"K8s Job {job_name!r} failed after "
                    f"{status.failed or 0} attempt(s): {message} "
                    f"(execution_id={execution_id})"
                )

            # Still running — log active count for observability
            if status.active and status.active > 0:
                logger.debug(
                    "Job %r active (%d pod(s)) — elapsed=%.0fs execution_id=%s",
                    job_name,
                    status.active,
                    time.monotonic() - started_at,
                    execution_id,
                )

    @staticmethod
    def _job_has_failed(status) -> bool:
        """Whether the Job carries a ``Failed`` condition that is true (the backoff
        limit or the deadline was reached)."""
        for condition in status.conditions or ():
            if condition.type == "Failed" and str(condition.status) == "True":
                return True
        return False

    @staticmethod
    def _extract_failure_message(status) -> str:
        """The message of the Job's ``Failed`` condition."""
        conditions = status.conditions or ()
        if not conditions:
            return "unknown — no conditions reported"
        for condition in conditions:
            if condition.type == "Failed" and condition.message:
                return condition.message[:500]
        return conditions[-1].message or "no message"

    async def _write_payload(
        self, execution_id: str, params: Dict[str, Any], carrier: Dict[str, Any]
    ) -> None:
        """Send the pod its parameters and context carrier (a queue of its own,
        dropped after the TTL whether the pod reads it or not)."""
        try:
            await K8sJobPayload.push(
                K8sJobPayload(params=params, carrier=carrier),
                ttl=_key_ttl_seconds(),
                route_override=execution_route(execution_id),
            )
            logger.debug("Payload sent for execution %s", execution_id)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to send the workflow payload ({execution_id}): {exc}"
            ) from exc

    async def _read_outcome(self, workflow_name: str, execution_id: str) -> Any:
        """
        Read the workflow result or error the pod sent after job completion.

        The pod sends exactly one of the two before exiting. Reading is
        destructive (a message is read once).
        """
        route = execution_route(execution_id)
        try:
            error = await K8sJobError.pop(route_override=route)
            result = (
                None
                if error is not None
                else await K8sJobResult.pop(
                    timeout=_RESULT_GRACE_SECONDS, route_override=route
                )
            )
        except Exception as exc:
            raise WorkflowExecutionError(
                f"Failed to read the result for workflow {workflow_name!r} "
                f"(execution_id={execution_id}): {exc}"
            ) from exc

        if error is not None:
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} raised an exception in the K8s pod "
                f"(execution_id={execution_id}): {error.error}\n{error.traceback}"
            )

        if result is None:
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} completed but sent no result. "
                f"The pod may have been OOMKilled or terminated before sending. "
                f"Check pod logs: kubectl logs -l execution-id={execution_id} "
                f"-n {self._namespace} "
                f"(execution_id={execution_id})"
            )
        return result.result

    async def _delete_messages(self, execution_id: str) -> None:
        """Delete the execution's three queues. Non-fatal on failure."""
        route = execution_route(execution_id)
        for model in (K8sJobPayload, K8sJobResult, K8sJobError):
            try:
                await model.queue_delete(route_override=route)
            except Exception as exc:
                logger.warning(
                    "Failed to delete %s queue for execution %s: %s",
                    model.__name__,
                    execution_id,
                    exc,
                )

    def _pod_databases(self) -> Optional[str]:
        """The value of ``VOLNUX_DATABASES`` for the pod: ``DATABASES`` as JSON.

        The pod has to reach the messaging backend before it can read anything, so
        the carrier (which is stored there) cannot tell it where that is. Passwords
        are never put in the Job spec: if ``DATABASES`` has any and a Secret is
        given, the Secret must supply ``VOLNUX_DATABASES`` and None is returned;
        without a Secret the entries go without their passwords and a warning
        says so.
        """
        from volnux.backends.databases import get_database_config, get_databases

        databases = get_databases()
        engine = get_database_config("messaging")["ENGINE"]
        if "inmemory" in engine.lower():
            logger.warning(
                "The 'messaging' database is the in-memory backend (%s): a Job pod "
                "is another process and can never see it. Configure a shared "
                "backend in DATABASES['messaging'].",
                engine,
            )

        has_password = any("PASSWORD" in entry for entry in databases.values())
        if has_password and self._env_secret_name:
            return None
        if has_password:
            logger.warning(
                "DATABASES has a PASSWORD: it is not put in the Job spec, so the "
                "pod cannot authenticate. Pass env_secret_name with a Secret that "
                "sets VOLNUX_DATABASES."
            )
        return json.dumps(_strip_passwords(databases), default=str)

    def _build_job_spec(
        self,
        workflow_name: str,
        job_name: str,
        execution_id: str,
    ):
        """
        Build the Kubernetes V1Job specification.

        The pod command uses the Volnux CLI rather than an embedded Python
        script string. The CLI is tested, versioned, and handles errors cleanly.

        Params are sent as a message (not env vars) to avoid size limits; the pod
        learns the execution id from its environment.
        Results are sent back as messages by the pod before exit.
        Secrets are mounted from a K8s Secret via envFrom (not plain env vars).
        """
        try:
            from kubernetes import client as k8s
        except ImportError:
            raise RuntimeError(
                "kubernetes package not installed. "
                "Install with: pip install 'volnux[kubernetes]'"
            )

        # Pass only non-sensitive, non-large values as env vars.
        # Secrets come from env_secret_name (envFrom).
        # Params come as a K8sJobPayload message (VOLNUX_JOB_EXECUTION_ID names it).
        env = [
            k8s.V1EnvVar(name="VOLNUX_WORKFLOW_NAME", value=workflow_name),
            k8s.V1EnvVar(name="VOLNUX_JOB_EXECUTION_ID", value=execution_id),
            # Pod name as node identity (the name VolnuxConfig reads)
            k8s.V1EnvVar(
                name="VOLNUX_MESH_NODE_ID",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="metadata.name")
                ),
            ),
        ]
        if self._database_env is not None:
            env.append(k8s.V1EnvVar(name="VOLNUX_DATABASES", value=self._database_env))
        project_dir = setting("PROJECT_DIR", None)
        if project_dir:
            env.append(k8s.V1EnvVar(name="VOLNUX_PROJECT_DIR", value=str(project_dir)))
        for key, value in self._env_vars.items():
            env.append(k8s.V1EnvVar(name=key, value=value))

        # envFrom mounts all keys from a Secret as environment variables.
        # Use for database passwords, API keys, JWT secrets — anything secret.
        env_from = []
        if self._env_secret_name:
            env_from.append(
                k8s.V1EnvFromSource(
                    secret_ref=k8s.V1SecretEnvSource(name=self._env_secret_name)
                )
            )

        resources = k8s.V1ResourceRequirements(
            requests=self._resource_requests or None,
            limits=self._resource_limits or None,
        )

        # volnux-job-runner (registered as a console script) initialises the
        # engine from the image's baked-in project and:
        #   1. Pops its K8sJobPayload (params + context carrier)
        #   2. Runs the workflow under the carried context
        #   3. Pushes a K8sJobResult, or a K8sJobError on failure
        container = k8s.V1Container(
            name="volnux-worker",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            env=env,
            env_from=env_from or None,
            resources=resources,
            command=["volnux-job-runner"],
        )

        tolerations = None
        if self._tolerations:
            tolerations = [k8s.V1Toleration(**t) for t in self._tolerations]

        pod_spec = k8s.V1PodSpec(
            containers=[container],
            restart_policy="Never",  # Job handles retry via backoff_limit
            service_account_name=self._service_account,
            node_selector=self._node_selector or None,
            tolerations=tolerations,
        )

        # All label values must be ≤ 63 characters.
        pod_labels = {
            "app": "volnux",
            "workflow": workflow_name[:63],
            "execution-id": execution_id[:63],
        }

        return k8s.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=k8s.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels=dict(pod_labels),
            ),
            spec=k8s.V1JobSpec(
                template=k8s.V1PodTemplateSpec(
                    metadata=k8s.V1ObjectMeta(labels=pod_labels),
                    spec=pod_spec,
                ),
                backoff_limit=self._backoff_limit,
                active_deadline_seconds=self._active_deadline_seconds,
                ttl_seconds_after_finished=self._ttl_seconds_after_finished,
                completions=1,
                parallelism=1,
            ),
        )

    async def _delete_job(
        self,
        job_name: str,
        propagation: str = "Background",
    ) -> bool:
        """
        Delete a Kubernetes Job. Non-blocking. Non-fatal.

        Args:
            job_name:    Name of the K8s Job to delete.
            propagation: "Background" (fast) or "Foreground" (wait for pod termination).

        Returns:
            True if deleted, False if not found or error.
        """
        from kubernetes.client import ApiException

        loop = asyncio.get_running_loop()
        try:
            from kubernetes import client as k8s

            await loop.run_in_executor(
                None,
                lambda: self._batch_client.delete_namespaced_job(
                    name=job_name,
                    namespace=self._namespace,
                    body=k8s.V1DeleteOptions(propagation_policy=propagation),
                ),
            )
            logger.debug("Deleted K8s Job %r (propagation=%s)", job_name, propagation)
            return True

        except ApiException as exc:
            if exc.status == 404:
                return False  # Already gone — not an error
            logger.warning("Failed to delete K8s Job %r: %s", job_name, exc)
            return False

        except Exception as exc:
            logger.warning("Unexpected error deleting K8s Job %r: %s", job_name, exc)
            return False

    @staticmethod
    def _init_k8s_clients(kube_config_file: Optional[str]):
        """
        Load Kubernetes configuration and return the BatchV1Api client.

        Tries in-cluster config first (works when running inside K8s),
        then falls back to the local kubeconfig file.
        """
        try:
            from kubernetes import client as k8s, config as k8s_config
            from kubernetes.config import ConfigException
        except ImportError:
            raise RuntimeError(
                "kubernetes package not installed. "
                "Install with: pip install 'volnux[kubernetes]'"
            )

        try:
            if kube_config_file:
                k8s_config.load_kube_config(config_file=kube_config_file)
                logger.debug("K8s: loaded config from file %s", kube_config_file)
            else:
                try:
                    k8s_config.load_incluster_config()
                    logger.debug("K8s: loaded in-cluster config")
                except ConfigException:
                    k8s_config.load_kube_config()
                    logger.debug("K8s: loaded kubeconfig from default location")

        except Exception as exc:
            raise RuntimeError(
                f"Failed to load Kubernetes configuration: {exc}. "
                "Ensure the process is running inside a K8s cluster "
                "or provide kube_config_file."
            ) from exc

        return k8s.BatchV1Api()


async def run_job(
    workflow_name: str, payload: K8sJobPayload, project_dir: Any
) -> Dict[str, Any]:
    """Run one workflow the way a Job pod does: initialise the engine, activate
    the carried context, run the workflow.

    Returns ``{"result": ...}`` (the result as JSON; what JSON cannot hold is its
    text).

    Raises:
        WorkflowExecutionError: the workflow is not registered or it failed.
    """
    from pathlib import Path

    from volnux.config import VolnuxConfig
    from volnux.setup import initialise_workflows

    params = payload.params or {}
    snapshot = extract_carrier({CARRIER_KEY: payload.carrier or {}})

    engine = await initialise_workflows(Path(project_dir))
    config = engine.get_workflow_registry().get_workflow_config(workflow_name)
    if config is None:
        raise WorkflowExecutionError(
            f"Workflow {workflow_name!r} not found in registry"
        )

    run_type = params.get("run_type", "single")
    workflow_params = {k: v for k, v in params.items() if k != "run_type"}

    # The pod is its own node (its name, from VOLNUX_MESH_NODE_ID); everything
    # else is the dispatcher's.
    with volnux_context(
        snapshot=snapshot, node_id=VolnuxConfig.get_instance().get_node_id()
    ):
        result = await config.run_workflow(params=workflow_params, run_type=run_type)
    return {"result": json.loads(json.dumps(result, default=str))}


def job_runner_main() -> None:
    """
    Entrypoint for the volnux-job-runner command inside K8s Job pods.

    Registered as a console script in pyproject.toml:
        [project.scripts]
        volnux-job-runner = "volnux.engine.workflows.trigger.executors.kubernetes:job_runner_main"

    Flow:
        1. Pop the K8sJobPayload (params and context carrier) of this execution
           (VOLNUX_JOB_EXECUTION_ID) from the messaging backend
        2. Initialise the Volnux engine
        3. Run the workflow under the carried context (``run_job``)
        4. Push a K8sJobResult on success
        5. Push a K8sJobError on failure
        6. Exit 0 on success, exit 1 on failure

    The messaging backend is the ``messaging`` entry of ``DATABASES``, which the
    executor passed in ``VOLNUX_DATABASES`` (or the pod's Secret did).

    This function is the ONLY place that calls initialise_workflows() in
    the K8s Job context — it is not called by the executor process.
    """
    import asyncio
    import os
    import sys
    import traceback

    def _die(msg: str) -> None:
        print(f"[volnux-job-runner] FATAL: {msg}", file=sys.stderr)
        sys.exit(1)

    # The per-job wiring is the one thing read from the environment directly: it
    # names this job, it is not a setting.
    workflow_name = os.environ.get("VOLNUX_WORKFLOW_NAME")
    execution_id = os.environ.get("VOLNUX_JOB_EXECUTION_ID")
    for name, value in (
        ("VOLNUX_WORKFLOW_NAME", workflow_name),
        ("VOLNUX_JOB_EXECUTION_ID", execution_id),
    ):
        if not value:
            _die(f"{name} is not set")
    route = execution_route(execution_id)
    project_dir = setting("PROJECT_DIR", None) or "."
    ttl = _key_ttl_seconds()

    async def _run() -> int:
        payload = await K8sJobPayload.pop(
            timeout=_PAYLOAD_WAIT_SECONDS, route_override=route
        )
        if payload is None:
            _die(
                f"No payload for execution {execution_id!r} — did the executor send "
                "it, and do both sides use the same DATABASES['messaging']?"
            )
        print(f"[volnux-job-runner] Starting workflow {workflow_name!r}", flush=True)
        try:
            outcome = await run_job(workflow_name, payload, project_dir)
        except Exception as exc:
            # (anything: also a failure to load the project or the context)
            await K8sJobError.push(
                K8sJobError(error=str(exc), traceback=traceback.format_exc()),
                ttl=ttl,
                route_override=route,
            )
            print(
                f"[volnux-job-runner] Workflow {workflow_name!r} FAILED: {exc}",
                file=sys.stderr,
            )
            return 1
        await K8sJobResult.push(
            K8sJobResult(result=outcome["result"]), ttl=ttl, route_override=route
        )
        print(f"[volnux-job-runner] Workflow {workflow_name!r} completed", flush=True)
        return 0

    sys.exit(asyncio.run(_run()))


def _new_execution_id() -> str:
    """Generate a time-sortable unique ID."""
    return f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"


def _make_job_name(workflow_name: str, execution_id: str) -> str:
    """
    Generate a Kubernetes-compliant Job name.

    Kubernetes resource name rules:
        - Lowercase alphanumeric and hyphens only
        - Max 253 characters for resource names
        - Max 63 characters for label values (job name is used as a label)

    Strategy: prefix "vx-" + truncated workflow name + "-" + short ID suffix.
    Total: 3 + 40 + 1 + 16 = 60 chars (comfortably under 63).
    """
    safe_workflow = workflow_name.lower().replace("_", "-").replace(" ", "-")
    # Keep only valid K8s name characters
    safe_workflow = "".join(c for c in safe_workflow if c.isalnum() or c == "-")
    # 60 chars total: "vx-" (3) + workflow (40) + "-" (1) + id suffix (16)
    short_workflow = safe_workflow[:40]
    # Short ID: last 16 chars of execution_id (millisecond timestamp + 8 hex chars)
    short_id = execution_id[-16:].replace("-", "")[:16]
    return f"vx-{short_workflow}-{short_id}"
