"""
KubernetesWorkflowExecutor — dispatches workflow execution as Kubernetes Jobs.

Architecture
------------
                API / TriggerEngine process
                ┌────────────────────────────────────────┐
                │ KubernetesWorkflowExecutor             │
                │   execute()                            │
                │   ├─ store params in Redis             │──► Redis
                │   ├─ create_namespaced_job()           │──► K8s API (thread)
                │   └─ _poll_job_status()                │
                │        └─ read_namespaced_job() loop   │──► K8s API (thread)
                │   └─ _read_result_from_redis()         │◄── Redis
                └────────────────────────────────────────┘

                Kubernetes Job pod (separate node, separate process)
                ┌───────────────────────────────────────┐
                │ volnux workflow run {name}             │
                │   └─ run_workflow_async()              │
                │   └─ write result → Redis              │──► Redis
                │   └─ exit 0 / exit 1                  │
                └───────────────────────────────────────┘

Key design decisions
--------------------
D1. All K8s API calls run in a thread via run_in_executor.
    The kubernetes Python client uses urllib3 (blocking HTTP). Every API
    call must run in a thread pool thread to avoid blocking the event loop.

D2. Results are stored in Redis, not pod stdout.
    Pod logs are ephemeral, subject to rotation, and not available immediately
    after pod exit. Redis provides durable, immediately readable storage with
    TTL-based cleanup. The pod writes to a key before exiting; the executor
    reads and deletes it after job completion.

D3. Params stored in Redis, not as environment variables.
    Large workflow params (embeddings, document content, batch configs) can
    exceed environment variable size limits. A Redis key is passed instead;
    the pod reads params from Redis at startup.

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

D7. Pod uses the Volnux CLI, not an embedded script string.
    `volnux workflow run` is the container's command. The embedded script
    approach is fragile (indentation, implicit convention, no syntax checking).
    The CLI command is tested, versioned, and has proper error handling.

D8. Pending jobs are tracked for cancellation.
    execute() registers the job name before the Job is created and
    deregisters it in finally. delete_job() is the cancellation mechanism.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from .base import BaseWorkflowConfigExecutor

logger = logging.getLogger(__name__)

__all__ = ["KubernetesWorkflowExecutor"]

# Redis key prefixes for job communication
_RESULT_KEY_PREFIX = "volnux:k8s-job:result"
_ERROR_KEY_PREFIX = "volnux:k8s-job:error"
_PARAMS_KEY_PREFIX = "volnux:k8s-job:params"
_KEY_TTL_SECONDS = 3600  # 1 hour — long enough for any reasonable job


class KubernetesWorkflowExecutor(BaseWorkflowConfigExecutor):
    """
    Dispatches workflow execution as Kubernetes Jobs.

    Each workflow execution creates a Kubernetes Job. The Job pod runs
    ``volnux workflow run`` with the workflow name and params. Results
    are communicated through Redis rather than pod logs.

    Prerequisites
    -------------
    The container image must have Volnux installed and the ``volnux`` CLI
    available. The pod must have network access to the same Redis instance
    used by the executor process.

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
        namespace: str = "default",
        image: str = "volnux-engine:latest",
        image_pull_policy: str = "IfNotPresent",
        service_account: Optional[str] = None,
        resource_requests: Optional[Dict[str, str]] = None,
        resource_limits: Optional[Dict[str, str]] = None,
        env_secret_name: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        node_selector: Optional[Dict[str, str]] = None,
        tolerations: Optional[List[dict]] = None,
        active_deadline_seconds: Optional[int] = None,
        ttl_seconds_after_finished: int = 3600,
        backoff_limit: int = 2,
        poll_interval: float = 2.0,
        kube_config_file: Optional[str] = None,
        redis_url: Optional[str] = None,
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

            redis_url:
                Redis URL for result and params transport. Defaults to
                VOLNUX_REDIS_CHECKPOINT_URL environment variable.
        """
        super().__init__(workflow_registry)

        self._namespace = namespace
        self._image = image
        self._image_pull_policy = image_pull_policy
        self._service_account = service_account
        self._resource_requests = resource_requests or {}
        self._resource_limits = resource_limits or {}
        self._env_secret_name = env_secret_name
        self._env_vars = env_vars or {}
        self._node_selector = node_selector
        self._tolerations = tolerations
        self._active_deadline_seconds = active_deadline_seconds
        self._ttl_seconds_after_finished = ttl_seconds_after_finished
        self._backoff_limit = backoff_limit
        self._poll_interval = poll_interval
        self._redis_url = redis_url or os.environ.get(
            "VOLNUX_REDIS_CHECKPOINT_URL", "redis://localhost:6379/0"
        )

        # Track submitted jobs for observability and cancellation
        # Maps execution_id → job_name
        self._pending: Dict[str, str] = {}

        # Initialise Kubernetes clients (synchronous — done once at construction)
        self._batch_client, self._core_client = self._init_k8s_clients(kube_config_file)

    async def execute(
        self,
        workflow_name: str,
        params: Dict[str, Any],
    ) -> Any:
        """
        Create a Kubernetes Job to execute a workflow and await its result.

        Execution flow:
            1. Store params in Redis (avoids env var size limits)
            2. Create K8s Job (non-blocking via run_in_executor)
            3. Poll Job status until succeeded/failed (non-blocking)
            4. Read result from Redis
            5. Delete Job (always — even on failure)

        Args:
            workflow_name: Name of the workflow to execute.
            params:        Workflow parameters. Not size-limited (stored in Redis).

        Returns:
            The workflow result.

        Raises:
            WorkflowExecutionError: If the Job fails, is deleted, or times out.
            ValueError:             If workflow_name is not registered.
        """

        execution_id = _new_execution_id()
        job_name = _make_job_name(workflow_name, execution_id)
        params_key = f"{_PARAMS_KEY_PREFIX}:{execution_id}"
        result_key = f"{_RESULT_KEY_PREFIX}:{execution_id}"
        error_key = f"{_ERROR_KEY_PREFIX}:{execution_id}"

        self._pending[execution_id] = job_name
        logger.info(
            "Creating K8s Job %r for workflow %r — execution_id=%s namespace=%s",
            job_name,
            workflow_name,
            execution_id,
            self._namespace,
        )

        loop = asyncio.get_event_loop()

        try:

            # Avoids environment variable size limits for large param payloads.
            # Key expires after _KEY_TTL_SECONDS whether the job reads it or not.
            await self._write_params_to_redis(params_key, params)

            job = self._build_job_spec(
                workflow_name=workflow_name,
                job_name=job_name,
                execution_id=execution_id,
                params_key=params_key,
                result_key=result_key,
                error_key=error_key,
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

            result = await self._read_result_from_redis(
                result_key=result_key,
                error_key=error_key,
                workflow_name=workflow_name,
                execution_id=execution_id,
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
            await self._delete_redis_keys(params_key, result_key, error_key)

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
        loop = asyncio.get_event_loop()
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

        loop = asyncio.get_event_loop()
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
                return  # ← poll loop exits, caller reads result from Redis

            if status.failed and status.failed > 0:
                # Extract last condition message for a clear error
                message = self._extract_failure_message(status)
                raise WorkflowExecutionError(
                    f"K8s Job {job_name!r} failed after "
                    f"{status.failed} attempt(s): {message} "
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
    def _extract_failure_message(status) -> str:
        """Extract the most recent failure condition message from job status."""
        if not status.conditions:
            return "unknown — no conditions reported"
        # Conditions are ordered newest-first for Failed type
        for condition in reversed(status.conditions):
            if condition.type == "Failed" and condition.message:
                return condition.message[:500]
        return status.conditions[-1].message or "no message"

    async def _write_params_to_redis(
        self, params_key: str, params: Dict[str, Any]
    ) -> None:
        """Serialise and write workflow params to Redis with TTL."""
        import json

        try:
            import redis.asyncio as aioredis

            client = aioredis.Redis.from_url(self._redis_url)
            await client.setex(params_key, _KEY_TTL_SECONDS, json.dumps(params))
            await client.aclose()
            logger.debug("Params stored at Redis key %s", params_key)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to write workflow params to Redis ({params_key}): {exc}"
            ) from exc

    async def _read_result_from_redis(
        self,
        result_key: str,
        error_key: str,
        workflow_name: str,
        execution_id: str,
    ) -> Any:
        """
        Read the workflow result or error from Redis after job completion.

        The pod writes to exactly one of result_key or error_key before exiting.
        Both are checked and deleted to prevent stale reads.
        """

        try:
            import redis.asyncio as aioredis

            r = aioredis.Redis.from_url(self._redis_url)

            result_raw, error_raw = await asyncio.gather(
                r.get(result_key),
                r.get(error_key),
                return_exceptions=False,
            )
            await r.aclose()

        except Exception as exc:
            raise WorkflowExecutionError(
                f"Failed to read result from Redis for workflow {workflow_name!r} "
                f"(execution_id={execution_id}): {exc}"
            ) from exc

        if error_raw is not None:
            try:
                error_info = json.loads(error_raw)
                error_msg = error_info.get("error", str(error_raw))
                tb = error_info.get("traceback", "")
            except Exception:
                error_msg = (
                    error_raw.decode()
                    if isinstance(error_raw, bytes)
                    else str(error_raw)
                )
                tb = ""
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} raised an exception in the K8s pod "
                f"(execution_id={execution_id}): {error_msg}\n{tb}"
            )

        if result_raw is None:
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} completed but wrote no result to Redis. "
                f"The pod may have been OOMKilled or terminated before writing. "
                f"Check pod logs: kubectl logs -l execution-id={execution_id} "
                f"-n {self._namespace} "
                f"(execution_id={execution_id})"
            )

        try:
            envelope = json.loads(result_raw)
            return envelope.get("result") if isinstance(envelope, dict) else envelope
        except Exception as exc:
            raise WorkflowExecutionError(
                f"Failed to deserialise result for workflow {workflow_name!r}: {exc}"
            ) from exc

    async def _delete_redis_keys(self, *keys: str) -> None:
        """Delete one or more Redis keys. Non-fatal on failure."""
        try:
            import redis.asyncio as aioredis

            r = aioredis.Redis.from_url(self._redis_url)
            await r.delete(*keys)
            await r.aclose()
        except Exception as exc:
            logger.warning("Failed to delete Redis keys %s: %s", keys, exc)

    def _build_job_spec(
        self,
        workflow_name: str,
        job_name: str,
        execution_id: str,
        params_key: str,
        result_key: str,
        error_key: str,
    ):
        """
        Build the Kubernetes V1Job specification.

        The pod command uses the Volnux CLI rather than an embedded Python
        script string. The CLI is tested, versioned, and handles errors cleanly.

        Params are passed via a Redis key (not env vars) to avoid size limits.
        Results are written to Redis by the pod before exit.
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
        # Params come from Redis via VOLNUX_JOB_PARAMS_KEY.
        env = [
            k8s.V1EnvVar(name="VOLNUX_WORKFLOW_NAME", value=workflow_name),
            k8s.V1EnvVar(name="VOLNUX_JOB_PARAMS_KEY", value=params_key),
            k8s.V1EnvVar(name="VOLNUX_JOB_RESULT_KEY", value=result_key),
            k8s.V1EnvVar(name="VOLNUX_JOB_ERROR_KEY", value=error_key),
            k8s.V1EnvVar(name="VOLNUX_REDIS_CHECKPOINT_URL", value=self._redis_url),
            # Pod name as node identity (consistent with other K8s deployments)
            k8s.V1EnvVar(
                name="VOLNUX_NODE_ID",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="metadata.name")
                ),
            ),
        ]
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

        # The entrypoint.sh handles the "worker" role — initialises the engine
        # from the image's baked-in project and runs the workflow via CLI.
        # volnux-job-runner is a purpose-built slim entrypoint that:
        #   1. Reads params from VOLNUX_JOB_PARAMS_KEY (Redis)
        #   2. Calls volnux workflow run {VOLNUX_WORKFLOW_NAME}
        #   3. Writes result to VOLNUX_JOB_RESULT_KEY or VOLNUX_JOB_ERROR_KEY
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
                labels={
                    "app": "volnux",
                    "workflow": workflow_name[:63],
                    "execution-id": execution_id[:63],
                },
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

        loop = asyncio.get_event_loop()
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
        Load Kubernetes configuration and return (BatchV1Api, CoreV1Api).

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

        return k8s.BatchV1Api(), k8s.CoreV1Api()


def job_runner_main() -> None:
    """
    Entrypoint for the volnux-job-runner command inside K8s Job pods.

    Registered as a console script in pyproject.toml:
        [project.scripts]
        volnux-job-runner = "volnux.executors.kubernetes_workflow:job_runner_main"

    Flow:
        1. Read params from Redis (VOLNUX_JOB_PARAMS_KEY)
        2. Initialise the Volnux engine
        3. Run the workflow via asyncio.run()
        4. Write result to VOLNUX_JOB_RESULT_KEY on success
        5. Write error to VOLNUX_JOB_ERROR_KEY on failure
        6. Exit 0 on success, exit 1 on failure

    This function is the ONLY place that calls initialise_workflows() in
    the K8s Job context — it is not called by the executor process.
    """
    import asyncio
    import json
    import os
    import sys
    import traceback
    from pathlib import Path

    def _die(msg: str) -> None:
        print(f"[volnux-job-runner] FATAL: {msg}", file=sys.stderr)
        sys.exit(1)

    workflow_name = os.environ.get("VOLNUX_WORKFLOW_NAME") or _die(
        "VOLNUX_WORKFLOW_NAME is not set"
    )
    params_key = os.environ.get("VOLNUX_JOB_PARAMS_KEY") or _die(
        "VOLNUX_JOB_PARAMS_KEY not set"
    )
    result_key = os.environ.get("VOLNUX_JOB_RESULT_KEY") or _die(
        "VOLNUX_JOB_RESULT_KEY not set"
    )
    error_key = os.environ.get("VOLNUX_JOB_ERROR_KEY") or _die(
        "VOLNUX_JOB_ERROR_KEY not set"
    )
    redis_url = os.environ.get(
        "VOLNUX_REDIS_CHECKPOINT_URL", "redis://localhost:6379/0"
    )
    project_dir = Path(os.environ.get("VOLNUX_PROJECT_DIR", "."))

    async def _run():
        import redis.asyncio as aioredis

        r = aioredis.Redis.from_url(redis_url)

        raw_params = await r.get(params_key)
        if raw_params is None:
            _die(
                f"Params key {params_key!r} not found in Redis — did the executor write it?"
            )
        params = json.loads(raw_params)
        print(f"[volnux-job-runner] Starting workflow {workflow_name!r}", flush=True)

        from volnux.setup import initialise_workflows

        engine = initialise_workflows(project_dir)
        registry = engine.get_workflow_registry()
        config = registry.get_workflow_config(workflow_name)

        if config is None:
            error_payload = json.dumps(
                {
                    "error": f"Workflow {workflow_name!r} not found in registry",
                    "traceback": "",
                }
            )
            await r.setex(error_key, _KEY_TTL_SECONDS, error_payload)
            await r.aclose()
            sys.exit(1)

        try:
            result = await config.run_workflow_async(
                params=params,
                run_type="single",
            )
            result_payload = json.dumps({"result": result}, default=str)
            await r.setex(result_key, _KEY_TTL_SECONDS, result_payload)
            print(
                f"[volnux-job-runner] Workflow {workflow_name!r} completed", flush=True
            )

        except Exception as exc:
            tb = traceback.format_exc()
            error_payload = json.dumps(
                {
                    "error": str(exc),
                    "traceback": tb,
                }
            )
            await r.setex(error_key, _KEY_TTL_SECONDS, error_payload)
            print(
                f"[volnux-job-runner] Workflow {workflow_name!r} FAILED: {exc}",
                file=sys.stderr,
            )
            await r.aclose()
            sys.exit(1)

        await r.aclose()

    asyncio.run(_run())


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
