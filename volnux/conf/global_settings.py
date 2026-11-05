from pathlib import Path

# =============================================================================
# Volnux global settings: the defaults of every setting.
#
# This is the equivalent of Django's `global_settings.py`. Do not edit it: a
# project sets only the values it changes, in its own settings module (`volnux
# init` writes one), and everything else comes from here.
#
# These defaults are chosen so that Volnux runs with NO external infrastructure:
# state is kept in memory and in a local SQLite file, nothing is exported, and
# every value can be left as it is while you try things out.
#
# Every setting has the same documentation block:
#
#     What        what the setting does
#     Used by     the part of Volnux that reads it
#     Env         the environment variable that overrides it
#     Production  an example value for a production deployment
#
# How a value is found (the first that is set wins):
#
#     1. the environment variable VOLNUX_<NAME>      (e.g. VOLNUX_API_PORT=9000)
#     2. a settings file named by VOLNUX_CONFIG
#     3. the project's settings module, named by VOLNUX_SETTINGS_MODULE (or a
#        `settings.py` in the project directory)
#     4. this file
#
# Only UPPER_CASE names are settings. An environment value is converted to the
# type of the setting it overrides: "9" is 9 for an integer, "false" is False for
# a boolean, and a JSON text is a list or a dict for a list or dict setting.
#
# Read a setting with:   from volnux.conf import settings;  settings.API_PORT
#
# SECRETS: never write a secret in a settings file. Set it in the environment
# (VOLNUX_SECRET_KEY ...) or in a secret manager. Settings whose name ends in
# _SECRET, _PASSWORD, _TOKEN or _KEY are never sent to workers.
# =============================================================================


# =============================================================================
# 1. Core
#
# Who this process is and what kind of deployment it belongs to.
# =============================================================================

# -----------------------------------------------------------------------------
# DEBUG
#   What:        Development mode. False (the default) is production: unsafe defaults
#                become errors, for example the API refuses to start without SECRET_KEY,
#                and telemetry is labelled 'production'. `volnux dev` turns it on. Never
#                leave it on in a deployment.
#   Used by:     volnux/app/utils.py (SECRET_KEY guard),
#                volnux/adapters/lifecycle/utils.py (telemetry label),
#                volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_DEBUG
#   Production:
#       DEBUG = False
DEBUG = False

# -----------------------------------------------------------------------------
# SECRET_KEY
#   What:        Key that signs login tokens. REQUIRED when DEBUG is False: the API
#                refuses to start without it. With DEBUG on, a random one is made per
#                process, so tokens stop working on restart. Use at least 32 random
#                bytes. Never write it in a settings file.
#   Used by:     volnux/app/utils.py
#   Env:         VOLNUX_SECRET_KEY
#   Production:
#       # SECRET_KEY is not set in a file. Export it instead:
#       #     export VOLNUX_SECRET_KEY="$(openssl rand -hex 32)"
#       SECRET_KEY = None
SECRET_KEY = None

# -----------------------------------------------------------------------------
# BASE_DIR
#   What:        Root directory of the project. The default is the directory that
#                contains the installed `volnux` package; a project's own settings file
#                sets it to the project directory (`BASE_DIR =
#                Path(__file__).resolve().parent`, as `volnux init` writes it). Relative
#                paths of the remote manager are resolved against it.
#   Used by:     volnux/manager/base.py, volnux/manager/remote_manager.py
#   Env:         VOLNUX_BASE_DIR
#   Production:
#       BASE_DIR = Path("/app")
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# -----------------------------------------------------------------------------
# PROJECT_ID
#   What:        Identifier of the project. Stamped on governance events, stream/queue
#                keys and saga records so several projects can share one storage backend.
#   Used by:     volnux/governance/*, volnux/mixins/stream/queue.py,
#                volnux/backends/saga/*
#   Env:         VOLNUX_PROJECT_ID
#   Production:
#       PROJECT_ID = "acme-trading"
PROJECT_ID = None

# -----------------------------------------------------------------------------
# NODE_ID
#   What:        Name of this node in telemetry, worker registration and saga records.
#                Must be unique per node in a cluster.
#   Used by:     volnux/adapters/*, volnux/backends/saga/*, volnux/mixins/stream/queue.py
#   Env:         VOLNUX_NODE_ID
#   Production:
#       NODE_ID = "worker-eu-1"        # or VOLNUX_NODE_ID=$HOSTNAME in a container
NODE_ID = "local"

# -----------------------------------------------------------------------------
# VERSION
#   What:        Application version reported by workers and telemetry.
#   Used by:     volnux/adapters/lifecycle/*, volnux/adapters/worker_server.py
#   Env:         VOLNUX_VERSION
#   Production:
#       VERSION = "2.1.0"
VERSION = "unknown"

# -----------------------------------------------------------------------------
# PROJECT_DIR
#   What:        Directory of the Volnux project (settings, workflows) for executors that
#                start a new process, i.e. a Celery worker or a Kubernetes job. None: the
#                current directory.
#   Used by:     volnux/engine/workflows/trigger/executors/celery.py, kubernetes.py
#   Env:         VOLNUX_PROJECT_DIR
#   Production:
#       PROJECT_DIR = "/app"
PROJECT_DIR = None


# =============================================================================
# 2. Databases
#
# Where Volnux keeps its data. The default needs no infrastructure (memory and a local SQLite file).
# =============================================================================

# -----------------------------------------------------------------------------
# DATABASES
#   What:        Where Volnux keeps its data, as in Django: a dict of named databases.
#                Each has ENGINE (the dotted path of a backend class) and, as the backend
#                needs them, NAME (database name or file), HOST, PORT, USER, PASSWORD and
#                OPTIONS (a dict of further connection arguments such as timeout,
#                pool_size, ssl_enabled). A model chooses its database with its
#                `get_database_alias()` classmethod; an alias that is not configured here
#                uses 'default'. The aliases Volnux's own models use: 'default'
#                (everything not listed below; in memory by default, so nothing survives
#                the process), 'triggers' (trigger state, shared by the engine and the
#                CLI; a SQLite file by default), 'messaging' (events published between
#                processes, which needs a store with publish/subscribe, such as Redis or
#                Kafka), 'streams' (stream chunks), 'saga' (saga recovery state and dead
#                letters) and 'checkpoints' (checkpoint snapshots).
#   Used by:     volnux/backends/databases.py, volnux/mixins/connection.py,
#                volnux/mixins/key_value_store_integration.py,
#                volnux/cli/command/mixins/check.py
#   Env:         VOLNUX_DATABASES
#   Production:
#       DATABASES = {
#           "default": {
#               "ENGINE": "volnux.backends.stores.postgres.PostgresStoreBackend",
#               "NAME": "volnux",
#               "HOST": "db.internal",
#               "PORT": 5432,
#               "USER": "volnux",
#               # PASSWORD: set VOLNUX_DATABASES, or build this dict from the environment in your settings module
#               "OPTIONS": {"pool_size": 20, "ssl_enabled": True},
#           },
#           "messaging": {
#               "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
#               "HOST": "redis.internal",
#               "PORT": 6379,
#               "NAME": 0,
#           },
#           "streams": {"ENGINE": "volnux.backends.stores.redis.RedisStoreBackend", "HOST": "redis.internal", "PORT": 6379, "NAME": 1},
#           "triggers": {"ENGINE": "volnux.backends.stores.sqlite.SqliteStoreBackend", "NAME": "/var/lib/volnux/volnux.db"},
#       }
#       # Kafka (pip install 'volnux[kafka]'): "volnux.backends.stores.kafka.KafkaStoreBackend",
#       # with "OPTIONS": {"bootstrap_servers": "broker1:9092,broker2:9092", "ssl_enabled": True}
DATABASES = {
    "default": {
        "ENGINE": "volnux.backends.stores.inmemory.InMemoryKeyValueStoreBackend",
    },
    "triggers": {
        "ENGINE": "volnux.backends.stores.sqlite.SqliteStoreBackend",
        "NAME": "volnux.db",
    },
}


# =============================================================================
# 3. Paths
#
# Where Volnux keeps files. The defaults live under the user's home directory so
# nothing needs to exist beforehand.
# =============================================================================

# -----------------------------------------------------------------------------
# MIGRATION_DIR
#   What:        Directory where model migration records are written (one sub-directory
#                per model).
#   Used by:     volnux/mixins/key_value_store_integration.py
#   Env:         VOLNUX_MIGRATION_DIR
#   Production:
#       MIGRATION_DIR = Path("/var/lib/volnux/migrations")   # a persistent volume
MIGRATION_DIR = Path.home() / ".volnux" / "migrations"

# -----------------------------------------------------------------------------
# WORKFLOWS_CACHE_DIR
#   What:        Cache of workflow packages fetched from a hub, PyPI or git source.
#   Used by:     volnux/engine/workflows/registry.py
#   Env:         VOLNUX_WORKFLOWS_CACHE_DIR
#   Production:
#       WORKFLOWS_CACHE_DIR = Path("/var/cache/volnux/workflows")
WORKFLOWS_CACHE_DIR = Path.home() / ".volnux" / "workflow_cache"

# -----------------------------------------------------------------------------
# CREDENTIALS_FILE
#   What:        File where `volnux login` stores the API token of the command-line
#                client.
#   Used by:     volnux/cli/command/mixins/runtime.py
#   Env:         VOLNUX_CREDENTIALS_FILE
#   Production:
#       CREDENTIALS_FILE = Path("/etc/volnux/credentials")   # rarely changed
CREDENTIALS_FILE = Path.home() / ".volnux" / "credentials"

# -----------------------------------------------------------------------------
# TRIGGERS_PID_FILE
#   What:        PID file of the trigger engine when it runs as a daemon (`volnux
#                triggers engine start --daemon`).
#   Used by:     volnux/cli/command/builtins/triggers/subcommands/engine/*
#   Env:         VOLNUX_TRIGGERS_PID_FILE
#   Production:
#       TRIGGERS_PID_FILE = "/var/run/volnux/triggers.pid"
TRIGGERS_PID_FILE = ".volnux_triggers.pid"


# =============================================================================
# 4. Engine and execution
#
# Limits of the workflow engine and of the pools it runs tasks on.
# =============================================================================

# -----------------------------------------------------------------------------
# MAX_ENGINE_ITERATIONS
#   What:        Most task executions one engine run may make before it is stopped as a
#                runaway (a cyclic workflow). 0 disables the guard.
#   Used by:     volnux/engine/default_engine.py
#   Env:         VOLNUX_MAX_ENGINE_ITERATIONS
#   Production:
#       MAX_ENGINE_ITERATIONS = 100_000    # long loops are intended; keep a bound
MAX_ENGINE_ITERATIONS = 10000

# -----------------------------------------------------------------------------
# COOPERATIVE_MAX_CONCURRENCY
#   What:        How many chains of one `{}` group run at the same time (0: no limit). A
#                group can override it with `[max_concurrency=N]`.
#   Used by:     volnux/engine/default_engine.py
#   Env:         VOLNUX_COOPERATIVE_MAX_CONCURRENCY
#   Production:
#       COOPERATIVE_MAX_CONCURRENCY = 256
COOPERATIVE_MAX_CONCURRENCY = 64

# -----------------------------------------------------------------------------
# SHARED_PROCESS_POOL_SIZE
#   What:        Size of the process pool shared by all flows. None: the CPU count.
#   Used by:     volnux/executors/shared.py
#   Env:         VOLNUX_SHARED_PROCESS_POOL_SIZE
#   Production:
#       SHARED_PROCESS_POOL_SIZE = 16
SHARED_PROCESS_POOL_SIZE = None

# -----------------------------------------------------------------------------
# SHARED_THREAD_POOL_SIZE
#   What:        Size of the thread pool shared by all flows. None: min(32, CPU count +
#                4).
#   Used by:     volnux/executors/shared.py
#   Env:         VOLNUX_SHARED_THREAD_POOL_SIZE
#   Production:
#       SHARED_THREAD_POOL_SIZE = 64
SHARED_THREAD_POOL_SIZE = None

# -----------------------------------------------------------------------------
# SHARED_EXECUTOR_POOLS
#   What:        True: one process and one thread pool for the whole process. False:
#                every flow creates its own pools.
#   Used by:     volnux/executors/shared.py
#   Env:         VOLNUX_SHARED_EXECUTOR_POOLS
#   Production:
#       SHARED_EXECUTOR_POOLS = True
SHARED_EXECUTOR_POOLS = True

# -----------------------------------------------------------------------------
# KV_EXECUTOR_WORKERS
#   What:        Threads reserved for blocking key-value backend calls (a separate pool,
#                so persistence never waits for task threads).
#   Used by:     volnux/concurrency/kv_executor.py
#   Env:         VOLNUX_KV_EXECUTOR_WORKERS
#   Production:
#       KV_EXECUTOR_WORKERS = 256   # many concurrent flows against a networked store
KV_EXECUTOR_WORKERS = 128

# -----------------------------------------------------------------------------
# SUBPHASE_EXECUTOR_WORKERS
#   What:        Threads for the sub-phases of an event (setup, teardown, checkpoint
#                hooks).
#   Used by:     volnux/mixins/event/checkpointer.py
#   Env:         VOLNUX_SUBPHASE_EXECUTOR_WORKERS
#   Production:
#       SUBPHASE_EXECUTOR_WORKERS = 128
SUBPHASE_EXECUTOR_WORKERS = 64

# -----------------------------------------------------------------------------
# MAX_BATCH_PROCESSING_WORKERS
#   What:        Workers used to process the batches of a BatchPipeline when the pipeline
#                does not set `max_workers`.
#   Used by:     volnux/execution/pipeline/batch.py
#   Env:         VOLNUX_MAX_BATCH_PROCESSING_WORKERS
#   Production:
#       MAX_BATCH_PROCESSING_WORKERS = 16
MAX_BATCH_PROCESSING_WORKERS = 4

# -----------------------------------------------------------------------------
# DEFAULT_BATCH_SIZE
#   What:        Items per batch for the default collection batch processor.
#   Used by:     volnux/default_batch_processors.py
#   Env:         VOLNUX_DEFAULT_BATCH_SIZE
#   Production:
#       DEFAULT_BATCH_SIZE = 1000
DEFAULT_BATCH_SIZE = 100

# -----------------------------------------------------------------------------
# DEFAULT_CHUNK_SIZE
#   What:        Bytes per chunk for the default file/stream batch processor.
#   Used by:     volnux/default_batch_processors.py
#   Env:         VOLNUX_DEFAULT_CHUNK_SIZE
#   Production:
#       DEFAULT_CHUNK_SIZE = 1_048_576   # 1 MiB
DEFAULT_CHUNK_SIZE = 10240


# =============================================================================
# 5. Event retry
#
# Retry policy of events that do not set their own.
# =============================================================================

# -----------------------------------------------------------------------------
# MAX_EVENT_RETRIES
#   What:        Attempts an event may make before it fails for good.
#   Used by:     volnux/mixins/event/retry.py
#   Env:         VOLNUX_MAX_EVENT_RETRIES
#   Production:
#       MAX_EVENT_RETRIES = 3
MAX_EVENT_RETRIES = 5

# -----------------------------------------------------------------------------
# MAX_EVENT_BACKOFF_FACTOR
#   What:        Base of the exponential back-off between attempts, in seconds (delay =
#                factor * 2 ** attempt).
#   Used by:     volnux/mixins/event/retry.py
#   Env:         VOLNUX_MAX_EVENT_BACKOFF_FACTOR
#   Production:
#       MAX_EVENT_BACKOFF_FACTOR = 0.5
MAX_EVENT_BACKOFF_FACTOR = 0.05

# -----------------------------------------------------------------------------
# MAX_EVENT_BACKOFF
#   What:        Longest wait between two attempts, in seconds.
#   Used by:     volnux/mixins/event/retry.py
#   Env:         VOLNUX_MAX_EVENT_BACKOFF
#   Production:
#       MAX_EVENT_BACKOFF = 300
MAX_EVENT_BACKOFF = 100


# =============================================================================
# 6. Checkpointing
#
# Checkpoints let a stopped or crashed run resume. They are kept in the 'checkpoints'
# database, which is in memory unless DATABASES says otherwise: use a durable one in
# production.
# =============================================================================

# -----------------------------------------------------------------------------
# CHECKPOINT_REQUIRED
#   What:        True: an event fails if its checkpoint cannot be created or queued.
#                False: the failure is logged and execution goes on.
#   Used by:     volnux/mixins/event/checkpointer.py
#   Env:         VOLNUX_CHECKPOINT_REQUIRED
#   Production:
#       CHECKPOINT_REQUIRED = True
CHECKPOINT_REQUIRED = False

# -----------------------------------------------------------------------------
# CHECKPOINT_CONFIG
#   What:        How and when checkpoints are taken, for an engine that does not receive
#                its own CheckPointConfig. policy.frequency: per_task | periodic |
#                on_state_change. runtime.*: concurrency, retries and time-to-live of a
#                checkpoint. Where they are stored is the 'checkpoints' entry of
#                DATABASES (else 'default').
#   Used by:     volnux/engine/base.py, volnux/engine/default_engine.py
#   Env:         VOLNUX_CHECKPOINT_CONFIG
#   Production:
#       CHECKPOINT_CONFIG = {
#           "policy": {"frequency": "per_task", "checkpoint_interval_seconds": 5.0},
#           "runtime": {"max_concurrent_checkpoints": 20, "retry_attempts": 5,
#                       "retry_delay_seconds": 1.0, "checkpoint_ttl_seconds": 86400.0},
#       }
#       # and in DATABASES: "checkpoints": {"ENGINE": ".../RedisStoreBackend", "HOST": "redis.internal", ...}
CHECKPOINT_CONFIG = {
    "policy": {"frequency": "per_task", "checkpoint_interval_seconds": 5.0},
    "runtime": {
        "max_concurrent_checkpoints": 5,
        "retry_attempts": 3,
        "retry_delay_seconds": 1.0,
        "checkpoint_ttl_seconds": 300.0,
    },
}

# -----------------------------------------------------------------------------
# CHECKPOINT_DRAIN_TIMEOUT
#   What:        Seconds a stopping worker waits for queued checkpoints to be written
#                before it exits. (`volnux dev` uses 30.)
#   Used by:     volnux/cli/command/builtins/serve.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_CHECKPOINT_DRAIN_TIMEOUT
#   Production:
#       CHECKPOINT_DRAIN_TIMEOUT = 300.0
CHECKPOINT_DRAIN_TIMEOUT = 120.0


# =============================================================================
# 7. Trigger engine and workflow executors
#
# The engine that starts workflows from triggers, and the executors that run them.
# =============================================================================

# -----------------------------------------------------------------------------
# TRIGGER_ENGINE_CONSUMER_CONCURRENCY
#   What:        Workflows the trigger engine runs at the same time.
#   Used by:     volnux/engine/workflows/trigger/engine.py
#   Env:         VOLNUX_TRIGGER_ENGINE_CONSUMER_CONCURRENCY
#   Production:
#       TRIGGER_ENGINE_CONSUMER_CONCURRENCY = 20
TRIGGER_ENGINE_CONSUMER_CONCURRENCY = 5

# -----------------------------------------------------------------------------
# TRIGGER_ENGINE_MAX_QUEUE_SIZE
#   What:        Activations that may wait for a free consumer; above it new activations
#                are dropped with a warning.
#   Used by:     volnux/engine/workflows/trigger/engine.py
#   Env:         VOLNUX_TRIGGER_ENGINE_MAX_QUEUE_SIZE
#   Production:
#       TRIGGER_ENGINE_MAX_QUEUE_SIZE = 10_000
TRIGGER_ENGINE_MAX_QUEUE_SIZE = 1000

# -----------------------------------------------------------------------------
# TRIGGER_ENGINE_DRAIN_TIMEOUT
#   What:        Seconds the engine waits, when stopping, for queued activations to
#                finish.
#   Used by:     volnux/engine/workflows/trigger/engine.py
#   Env:         VOLNUX_TRIGGER_ENGINE_DRAIN_TIMEOUT
#   Production:
#       TRIGGER_ENGINE_DRAIN_TIMEOUT = 120
TRIGGER_ENGINE_DRAIN_TIMEOUT = 30

# -----------------------------------------------------------------------------
# TRIGGER_ENGINE_STATE_SYNC_INTERVAL
#   What:        Seconds between checks for changes made from the CLI (pause, resume,
#                disable) to trigger state.
#   Used by:     volnux/engine/workflows/trigger/engine.py
#   Env:         VOLNUX_TRIGGER_ENGINE_STATE_SYNC_INTERVAL
#   Production:
#       TRIGGER_ENGINE_STATE_SYNC_INTERVAL = 5
TRIGGER_ENGINE_STATE_SYNC_INTERVAL = 10

# -----------------------------------------------------------------------------
# DEFAULT_WORKFLOW_EXECUTOR
#   What:        Dotted path of the executor that runs a workflow when a trigger fires
#                and none was set explicitly.
#   Used by:     volnux/setup.py
#   Env:         VOLNUX_DEFAULT_WORKFLOW_EXECUTOR
#   Production:
#       DEFAULT_WORKFLOW_EXECUTOR = "myproject.executors.CeleryWorkflowExecutor"
DEFAULT_WORKFLOW_EXECUTOR = (
    "volnux.engine.workflows.trigger.engine.WorkflowConfigExecutor"
)

# -----------------------------------------------------------------------------
# CELERY_EXECUTOR_QUEUE
#   What:        Celery queue the Celery workflow executor sends workflows to.
#   Used by:     volnux/engine/workflows/trigger/executors/celery.py
#   Env:         VOLNUX_CELERY_EXECUTOR_QUEUE
#   Production:
#       CELERY_EXECUTOR_QUEUE = "volnux_prod_workflows"
CELERY_EXECUTOR_QUEUE = "volnux_workflows"

# -----------------------------------------------------------------------------
# CELERY_EXECUTOR_POLL_INTERVAL
#   What:        Seconds between checks of a Celery task's state.
#   Used by:     volnux/engine/workflows/trigger/executors/celery.py
#   Env:         VOLNUX_CELERY_EXECUTOR_POLL_INTERVAL
#   Production:
#       CELERY_EXECUTOR_POLL_INTERVAL = 1.0
CELERY_EXECUTOR_POLL_INTERVAL = 0.5

# -----------------------------------------------------------------------------
# REDIS_CHECKPOINT_URL
#   What:        Redis where Kubernetes jobs and `volnux serve` workers store job
#                parameters, results and checkpoints.
#   Used by:     volnux/cli/command/builtins/serve.py,
#                volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_REDIS_CHECKPOINT_URL
#   Production:
#       REDIS_CHECKPOINT_URL = "rediss://redis.internal:6380/1"    # prefer the environment variable
REDIS_CHECKPOINT_URL = "redis://localhost:6379/0"

# -----------------------------------------------------------------------------
# REDIS_BROKER_URL
#   What:        Message broker of the Celery executor started by `volnux serve`.
#   Used by:     volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_REDIS_BROKER_URL
#   Production:
#       REDIS_BROKER_URL = "rediss://redis.internal:6380/2"    # prefer the environment variable
REDIS_BROKER_URL = "redis://localhost:6380/0"

# -----------------------------------------------------------------------------
# K8S_NAMESPACE
#   What:        Namespace the Kubernetes workflow executor creates jobs in.
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_NAMESPACE
#   Production:
#       K8S_NAMESPACE = "volnux"
K8S_NAMESPACE = "default"

# -----------------------------------------------------------------------------
# K8S_IMAGE
#   What:        Container image of the jobs. Use a pinned tag, never `latest`, in
#                production.
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_IMAGE
#   Production:
#       K8S_IMAGE = "registry.example.com/volnux-engine:2.1.0"
K8S_IMAGE = "volnux-engine:latest"

# -----------------------------------------------------------------------------
# K8S_IMAGE_PULL_POLICY
#   What:        imagePullPolicy of the job's container (Always | IfNotPresent | Never).
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_IMAGE_PULL_POLICY
#   Production:
#       K8S_IMAGE_PULL_POLICY = "IfNotPresent"
K8S_IMAGE_PULL_POLICY = "IfNotPresent"

# -----------------------------------------------------------------------------
# K8S_JOB_TTL_SECONDS
#   What:        Seconds a finished job is kept before Kubernetes deletes it.
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_JOB_TTL_SECONDS
#   Production:
#       K8S_JOB_TTL_SECONDS = 86400
K8S_JOB_TTL_SECONDS = 3600

# -----------------------------------------------------------------------------
# K8S_BACKOFF_LIMIT
#   What:        Times Kubernetes retries a failed job.
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_BACKOFF_LIMIT
#   Production:
#       K8S_BACKOFF_LIMIT = 1
K8S_BACKOFF_LIMIT = 2

# -----------------------------------------------------------------------------
# K8S_POLL_INTERVAL
#   What:        Seconds between checks of a job's status.
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_POLL_INTERVAL
#   Production:
#       K8S_POLL_INTERVAL = 5.0
K8S_POLL_INTERVAL = 2.0

# -----------------------------------------------------------------------------
# K8S_KEY_TTL_SECONDS
#   What:        Time-to-live of the Redis keys that carry a job's parameters, result and
#                error; longer than any job should run.
#   Used by:     volnux/engine/workflows/trigger/executors/kubernetes.py
#   Env:         VOLNUX_K8S_KEY_TTL_SECONDS
#   Production:
#       K8S_KEY_TTL_SECONDS = 21_600
K8S_KEY_TTL_SECONDS = 3600


# =============================================================================
# 8. Remote execution
#
# Sockets and streams between a coordinator and remote workers.
# =============================================================================

# -----------------------------------------------------------------------------
# DEFAULT_CONNECTION_TIMEOUT
#   What:        Seconds to wait when connecting to, or reading from, a remote worker.
#   Used by:     volnux/executors/tcp.py, volnux/manager/remote_manager.py
#   Env:         VOLNUX_DEFAULT_CONNECTION_TIMEOUT
#   Production:
#       DEFAULT_CONNECTION_TIMEOUT = 10
DEFAULT_CONNECTION_TIMEOUT = 30

# -----------------------------------------------------------------------------
# DATA_CHUNK_SIZE
#   What:        Bytes read or written per socket operation.
#   Used by:     volnux/executors/tcp.py, volnux/manager/remote_manager.py
#   Env:         VOLNUX_DATA_CHUNK_SIZE
#   Production:
#       DATA_CHUNK_SIZE = 65536
DATA_CHUNK_SIZE = 4096

# -----------------------------------------------------------------------------
# CONNECTION_BACKLOG_SIZE
#   What:        Pending connections a listening socket accepts.
#   Used by:     volnux/executors/tcp.py, volnux/manager/remote_manager.py
#   Env:         VOLNUX_CONNECTION_BACKLOG_SIZE
#   Production:
#       CONNECTION_BACKLOG_SIZE = 128
CONNECTION_BACKLOG_SIZE = 5

# -----------------------------------------------------------------------------
# DATA_QUEUE_SIZE
#   What:        Messages buffered per connection.
#   Used by:     volnux/executors/tcp.py, volnux/manager/remote_manager.py
#   Env:         VOLNUX_DATA_QUEUE_SIZE
#   Production:
#       DATA_QUEUE_SIZE = 10_000
DATA_QUEUE_SIZE = 1000

# -----------------------------------------------------------------------------
# REMOTE_BATCH_TTL_SECONDS
#   What:        Time-to-live of the Redis list where a Celery task streams its result
#                batches. The requester must read them within it.
#   Used by:     volnux/execution/remote/stream.py
#   Env:         VOLNUX_REMOTE_BATCH_TTL_SECONDS
#   Production:
#       REMOTE_BATCH_TTL_SECONDS = 7200
REMOTE_BATCH_TTL_SECONDS = 3600


# =============================================================================
# 9. Mesh
#
# Peer-to-peer coordination between Volnux nodes (config gossip, capacity-based
# task placement). The node id and the signing keys are environment-only, see the end
# of this file.
# =============================================================================

# -----------------------------------------------------------------------------
# MESH_HOST
#   What:        Address the mesh listens on.
#   Used by:     volnux/manager/mesh/client.py
#   Env:         VOLNUX_MESH_HOST
#   Production:
#       MESH_HOST = "0.0.0.0"       # inside a container; the pod network controls reachability
MESH_HOST = "0.0.0.0"

# -----------------------------------------------------------------------------
# MESH_PORT
#   What:        Port the mesh listens on.
#   Used by:     volnux/manager/mesh/client.py
#   Env:         VOLNUX_MESH_PORT
#   Production:
#       MESH_PORT = 50051
MESH_PORT = 50051

# -----------------------------------------------------------------------------
# MESH_HEARTBEAT_INTERVAL
#   What:        Seconds between health checks of the peers.
#   Used by:     volnux/manager/mesh/client.py
#   Env:         VOLNUX_MESH_HEARTBEAT_INTERVAL
#   Production:
#       MESH_HEARTBEAT_INTERVAL = 10.0
MESH_HEARTBEAT_INTERVAL = 5.0

# -----------------------------------------------------------------------------
# MESH_GOSSIP_FANOUT
#   What:        Peers each node forwards a config update to. Larger spreads faster and
#                costs more traffic.
#   Used by:     volnux/manager/mesh/client.py
#   Env:         VOLNUX_MESH_GOSSIP_FANOUT
#   Production:
#       MESH_GOSSIP_FANOUT = 5
MESH_GOSSIP_FANOUT = 3

# -----------------------------------------------------------------------------
# MESH_CAPACITY_GATHER_TIMEOUT
#   What:        Seconds to wait for peers to report their capacity when choosing where
#                to run a task. Slower peers are left out of the choice.
#   Used by:     volnux/manager/mesh/client.py
#   Env:         VOLNUX_MESH_CAPACITY_GATHER_TIMEOUT
#   Production:
#       MESH_CAPACITY_GATHER_TIMEOUT = 1.0
MESH_CAPACITY_GATHER_TIMEOUT = 0.5

# -----------------------------------------------------------------------------
# MESH_SCORE_WEIGHTS
#   What:        Weights of the score that ranks peers for task placement (lower load and
#                latency is better). Should add up to 1.
#   Used by:     volnux/manager/mesh/client.py
#   Env:         VOLNUX_MESH_SCORE_WEIGHTS
#   Production:
#       MESH_SCORE_WEIGHTS = {"latency": 0.2, "queue": 0.3, "cpu": 0.3, "memory": 0.2}
MESH_SCORE_WEIGHTS = {"latency": 0.4, "queue": 0.3, "cpu": 0.2, "memory": 0.1}


# =============================================================================
# 10. API server and workers
#
# `volnux serve` (production) and the REST API. `volnux dev` keeps its own
# development defaults (127.0.0.1, debug logging, text logs).
# NOTE: `volnux serve` imports `volnux.server`, which is not in this source tree yet.
# =============================================================================

# -----------------------------------------------------------------------------
# API_HOST
#   What:        Address the REST API binds to.
#   Used by:     volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_API_HOST
#   Production:
#       API_HOST = "0.0.0.0"        # behind a reverse proxy / ingress
API_HOST = "0.0.0.0"

# -----------------------------------------------------------------------------
# API_PORT
#   What:        Port of the REST API.
#   Used by:     volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_API_PORT
#   Production:
#       API_PORT = 8080
API_PORT = 8080

# -----------------------------------------------------------------------------
# API_DOCS
#   What:        Serve the interactive API documentation (/docs). Leave off in
#                production.
#   Used by:     volnux/app/app.py
#   Env:         VOLNUX_API_DOCS
#   Production:
#       API_DOCS = False
API_DOCS = False

# -----------------------------------------------------------------------------
# ACCESS_LOG
#   What:        Log every HTTP request.
#   Used by:     volnux/adapters/lifecycle/api_server.py,
#                volnux/adapters/worker_server.py
#   Env:         VOLNUX_ACCESS_LOG
#   Production:
#       ACCESS_LOG = False             # usually the ingress logs requests already
ACCESS_LOG = False

# -----------------------------------------------------------------------------
# CORS_ALLOWED_ORIGINS
#   What:        Origins a browser may call the API from. ['*'] allows any origin: fine
#                for development, too open for production.
#   Used by:     volnux/app/app.py
#   Env:         VOLNUX_CORS_ALLOWED_ORIGINS
#   Production:
#       CORS_ALLOWED_ORIGINS = ["https://console.example.com"]
CORS_ALLOWED_ORIGINS = ["*"]

# -----------------------------------------------------------------------------
# WORKER_PORT
#   What:        Port of a worker's health and readiness endpoint.
#   Used by:     volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_WORKER_PORT
#   Production:
#       WORKER_PORT = 9090
WORKER_PORT = 9090

# -----------------------------------------------------------------------------
# GRPC_PORT
#   What:        Port of a worker's gRPC remote-manager endpoint.
#   Used by:     volnux/adapters/worker_server.py, volnux/adapters/lifecycle/*
#   Env:         VOLNUX_GRPC_PORT
#   Production:
#       GRPC_PORT = 45545
GRPC_PORT = 45545

# -----------------------------------------------------------------------------
# PROMETHEUS_PORT
#   What:        Port where Prometheus metrics are served (/metrics).
#   Used by:     volnux/cli/command/builtins/serve.py, volnux/adapters/*
#   Env:         VOLNUX_PROMETHEUS_PORT
#   Production:
#       PROMETHEUS_PORT = 9464
PROMETHEUS_PORT = 9464

# -----------------------------------------------------------------------------
# LOG_LEVEL
#   What:        Log level of the server processes: DEBUG, INFO, WARNING, ERROR.
#   Used by:     volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_LOG_LEVEL
#   Production:
#       LOG_LEVEL = "INFO"
LOG_LEVEL = "INFO"

# -----------------------------------------------------------------------------
# LOG_FORMAT
#   What:        'json' for log collectors, 'text' for people.
#   Used by:     volnux/cli/command/builtins/serve.py
#   Env:         VOLNUX_LOG_FORMAT
#   Production:
#       LOG_FORMAT = "json"
LOG_FORMAT = "json"

# -----------------------------------------------------------------------------
# HOST
#   What:        Server the command-line client (`volnux login`, `volnux triggers ...`)
#                talks to.
#   Used by:     volnux/cli/command/mixins/runtime.py,
#                volnux/cli/command/runtime/login.py
#   Env:         VOLNUX_HOST
#   Production:
#       HOST = "volnux.example.com"
HOST = "localhost:8080"


# =============================================================================
# 11. Authentication
#
# Tokens are signed with SECRET_KEY (see Core).
# =============================================================================

# -----------------------------------------------------------------------------
# JWT_ALGORITHM
#   What:        Signing algorithm of the tokens (a symmetric HMAC one, as JWT_SECRET is
#                a shared secret).
#   Used by:     volnux/app/utils.py
#   Env:         VOLNUX_JWT_ALGORITHM
#   Production:
#       JWT_ALGORITHM = "HS256"
JWT_ALGORITHM = "HS256"

# -----------------------------------------------------------------------------
# JWT_EXPIRATION_HOURS
#   What:        Hours a login token stays valid.
#   Used by:     volnux/app/utils.py
#   Env:         VOLNUX_JWT_EXPIRATION_HOURS
#   Production:
#       JWT_EXPIRATION_HOURS = 8
JWT_EXPIRATION_HOURS = 24


# =============================================================================
# 12. Observability
#
# Logging, tracing and metrics.
# =============================================================================

# -----------------------------------------------------------------------------
# LOGGING
#   What:        Python logging.config.dictConfig schema applied when Volnux starts.
#   Used by:     volnux/setup.py
#   Env:         VOLNUX_LOGGING
#   Production:
#       LOGGING = {... "loggers": {"volnux": {"handlers": ["default"], "level": "INFO", "propagate": False}}}
#       # add a handler for your log shipper, e.g. "class": "logging.handlers.WatchedFileHandler"
LOGGING = {
    "version": 1,
    # False: loggers created before this is applied (other libraries') keep working
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "WARNING",
            "propagate": False,
        },
        "volnux": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

# -----------------------------------------------------------------------------
# OTEL_ENABLED
#   What:        Export OpenTelemetry traces. Off by default because it needs a collector
#                to send to.
#   Used by:     volnux/adapters/lifecycle/utils.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_OTEL_ENABLED
#   Production:
#       OTEL_ENABLED = True
OTEL_ENABLED = False

# -----------------------------------------------------------------------------
# OTEL_BACKEND
#   What:        Where traces go: 'grafana' (Tempo), 'datadog' (Datadog Agent) or
#                anything else for a generic OTLP collector.
#   Used by:     volnux/adapters/lifecycle/utils.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_OTEL_BACKEND
#   Production:
#       OTEL_BACKEND = "grafana"
OTEL_BACKEND = "grafana"

# -----------------------------------------------------------------------------
# OTEL_ENDPOINT
#   What:        OTLP endpoint, used by every backend unless it has its own URL below.
#   Used by:     volnux/adapters/lifecycle/utils.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_OTEL_ENDPOINT
#   Production:
#       OTEL_ENDPOINT = "http://otel-collector.observability:4317"
OTEL_ENDPOINT = "http://localhost:4317"

# -----------------------------------------------------------------------------
# OTEL_SERVICE_NAME
#   What:        Service name on the traces. None: the project name, else 'volnux'.
#   Used by:     volnux/adapters/lifecycle/utils.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_OTEL_SERVICE_NAME
#   Production:
#       OTEL_SERVICE_NAME = "volnux-trading"
OTEL_SERVICE_NAME = None

# -----------------------------------------------------------------------------
# OTEL_DATADOG_URL
#   What:        Datadog Agent OTLP URL (backend 'datadog'). None: OTEL_ENDPOINT.
#   Used by:     volnux/adapters/lifecycle/utils.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_OTEL_DATADOG_URL
#   Production:
#       OTEL_DATADOG_URL = "http://datadog-agent:4317"
OTEL_DATADOG_URL = None

# -----------------------------------------------------------------------------
# OTEL_TEMPO_URL
#   What:        Tempo / Grafana Agent URL (backend 'grafana'). None: OTEL_ENDPOINT.
#   Used by:     volnux/adapters/lifecycle/utils.py, volnux/adapters/worker_server.py
#   Env:         VOLNUX_OTEL_TEMPO_URL
#   Production:
#       OTEL_TEMPO_URL = "http://tempo:4317"
OTEL_TEMPO_URL = None

# -----------------------------------------------------------------------------
# OTEL_SAMPLE_RATE
#   What:        Fraction of traces recorded, 0.0 to 1.0. Lower it under heavy load.
#   Used by:     volnux/otel/tracer_setup.py
#   Env:         VOLNUX_OTEL_SAMPLE_RATE
#   Production:
#       OTEL_SAMPLE_RATE = 0.1
OTEL_SAMPLE_RATE = 1.0


# =============================================================================
# 13. Governance and events
#
# Pipeline reporting, the defaults of the governance CLI, and event versioning.
# =============================================================================

# -----------------------------------------------------------------------------
# PIPELINE_REPORTERS
#   What:        Pipeline-lifecycle reporters installed once per process at bootstrap
#                (each is a dotted path, imported and called with no arguments). The
#                governance reporter produces the events of the governance feed.
#   Used by:     volnux/reporting.py, volnux/setup.py
#   Env:         VOLNUX_PIPELINE_REPORTERS
#   Production:
#       PIPELINE_REPORTERS = [
#           "volnux.governance.reporter.install_governance_reporter",
#           "myproject.reporting.install_audit_reporter",
#       ]
PIPELINE_REPORTERS = ["volnux.governance.reporter.install_governance_reporter"]

# -----------------------------------------------------------------------------
# DEFAULT_ORG_SLUG
#   What:        Organisation the CLI registers workflows under when none is given.
#   Used by:     volnux/backends/migrations.py
#   Env:         VOLNUX_DEFAULT_ORG_SLUG
#   Production:
#       DEFAULT_ORG_SLUG = "acme"
DEFAULT_ORG_SLUG = "default-org"

# -----------------------------------------------------------------------------
# DEFAULT_TEAM_SLUG
#   What:        Team the CLI registers workflows under when none is given.
#   Used by:     volnux/backends/migrations.py
#   Env:         VOLNUX_DEFAULT_TEAM_SLUG
#   Production:
#       DEFAULT_TEAM_SLUG = "trading"
DEFAULT_TEAM_SLUG = "default-team"

# -----------------------------------------------------------------------------
# DEFAULT_USER_EMAIL
#   What:        User the CLI acts as when it creates records.
#   Used by:     volnux/backends/migrations.py
#   Env:         VOLNUX_DEFAULT_USER_EMAIL
#   Production:
#       DEFAULT_USER_EMAIL = "ops@example.com"
DEFAULT_USER_EMAIL = "cli@volnux.local"

# -----------------------------------------------------------------------------
# GOVERNANCE_SAMPLING_CAPACITY
#   What:        Size of the reservoir of non-critical governance events kept per run;
#                critical events are always kept.
#   Used by:     volnux/governance/sampling.py
#   Env:         VOLNUX_GOVERNANCE_SAMPLING_CAPACITY
#   Production:
#       GOVERNANCE_SAMPLING_CAPACITY = 128
GOVERNANCE_SAMPLING_CAPACITY = 32

# -----------------------------------------------------------------------------
# DEFAULT_EVENT_NAMESPACE
#   What:        Namespace of events that do not declare one (`namespace = "trading"` on
#                an event). Events are versioned with semantic versioning
#                (MAJOR.MINOR.PATCH): set `version` on the event class, `"1.0.0"` by
#                default.
#   Used by:     volnux/versioning/semantic.py, volnux/event/base.py
#   Env:         VOLNUX_DEFAULT_EVENT_NAMESPACE
#   Production:
#       DEFAULT_EVENT_NAMESPACE = "trading"
DEFAULT_EVENT_NAMESPACE = "local"


# =============================================================================
# 14. Agents
#
# Defaults of AgentEventBase subclasses; a subclass overrides any of them with a class
# attribute of the same name in lower case (`max_reasoning_steps = 5`).
# =============================================================================

# -----------------------------------------------------------------------------
# AGENT_TEMPERATURE
#   What:        Sampling temperature (0 to 2). 0 is the most deterministic.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_TEMPERATURE
#   Production:
#       AGENT_TEMPERATURE = 0.0
AGENT_TEMPERATURE = 0.0

# -----------------------------------------------------------------------------
# AGENT_MAX_TOKENS
#   What:        Most tokens the model may produce per turn.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_MAX_TOKENS
#   Production:
#       AGENT_MAX_TOKENS = 2048
AGENT_MAX_TOKENS = 4096

# -----------------------------------------------------------------------------
# AGENT_MAX_REASONING_STEPS
#   What:        Turns an agent may take before it is stopped.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_MAX_REASONING_STEPS
#   Production:
#       AGENT_MAX_REASONING_STEPS = 10
AGENT_MAX_REASONING_STEPS = 20

# -----------------------------------------------------------------------------
# AGENT_TOOL_TIMEOUT_SECONDS
#   What:        Seconds a tool call may take.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_TOOL_TIMEOUT_SECONDS
#   Production:
#       AGENT_TOOL_TIMEOUT_SECONDS = 60.0
AGENT_TOOL_TIMEOUT_SECONDS = 30.0

# -----------------------------------------------------------------------------
# AGENT_LLM_MAX_RETRIES
#   What:        Times a failed model call is retried.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_LLM_MAX_RETRIES
#   Production:
#       AGENT_LLM_MAX_RETRIES = 5
AGENT_LLM_MAX_RETRIES = 3

# -----------------------------------------------------------------------------
# AGENT_LLM_RETRY_DELAY_SECONDS
#   What:        Seconds before the first retry of a model call (it grows with each
#                attempt).
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_LLM_RETRY_DELAY_SECONDS
#   Production:
#       AGENT_LLM_RETRY_DELAY_SECONDS = 2.0
AGENT_LLM_RETRY_DELAY_SECONDS = 1.0

# -----------------------------------------------------------------------------
# AGENT_MAX_TOTAL_TOKENS
#   What:        Token budget of one agent run (0: no limit). Set one in production to
#                bound cost.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_MAX_TOTAL_TOKENS
#   Production:
#       AGENT_MAX_TOTAL_TOKENS = 200_000
AGENT_MAX_TOTAL_TOKENS = 0

# -----------------------------------------------------------------------------
# AGENT_MAX_CONTEXT_MESSAGES
#   What:        Messages kept in the conversation; older ones are trimmed.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_MAX_CONTEXT_MESSAGES
#   Production:
#       AGENT_MAX_CONTEXT_MESSAGES = 30
AGENT_MAX_CONTEXT_MESSAGES = 50

# -----------------------------------------------------------------------------
# AGENT_MAX_PROTOCOL_VIOLATIONS
#   What:        Malformed model responses tolerated before the run fails.
#   Used by:     volnux/event/agent/base.py
#   Env:         VOLNUX_AGENT_MAX_PROTOCOL_VIOLATIONS
#   Production:
#       AGENT_MAX_PROTOCOL_VIOLATIONS = 2
AGENT_MAX_PROTOCOL_VIOLATIONS = 3

# -----------------------------------------------------------------------------
# AGENT_HTTP_TIMEOUT_SECONDS
#   What:        Seconds an HTTP model provider may take to answer (the adapter option
#                `timeout` overrides it).
#   Used by:     volnux/event/agent/providers/http.py
#   Env:         VOLNUX_AGENT_HTTP_TIMEOUT_SECONDS
#   Production:
#       AGENT_HTTP_TIMEOUT_SECONDS = 60.0
AGENT_HTTP_TIMEOUT_SECONDS = 120.0


# =============================================================================
# Environment-only variables
#
# These are read before this file can be, or hold secrets, so they cannot be
# settings. Set them in the environment.
# =============================================================================

# VOLNUX_SETTINGS_MODULE
#   Dotted path of the project's settings module, like DJANGO_SETTINGS_MODULE
#   (`myproject.settings`, or `settings` for a module in the project directory). It is
#   imported on top of these defaults. `volnux` commands run inside a project set it for
#   you.
#   Example:  export VOLNUX_SETTINGS_MODULE=myproject.settings
#
# VOLNUX_ROLE
#   Which server `volnux serve` runs when --role is not given: 'api' (default) or
#   'worker'.
#   Example:  export VOLNUX_ROLE=worker
#
# VOLNUX_CONFIG
#   Path of an extra settings file, loaded after settings.py and after any settings.py
#   found in VOLNUX_CONFIG_DIR.
#   Example:  export VOLNUX_CONFIG=/etc/volnux/production.py
#
# VOLNUX_MESH_NODE_ID
#   Identity of this node in the mesh. Generated (and exported) when unset; set it to
#   keep the same id across restarts.
#   Example:  export VOLNUX_MESH_NODE_ID=node-eu-1
#
# VOLNUX_MESH_PRIVATE_KEY
#   Ed25519 private key (PEM) this node signs its config updates with. A secret:
#   environment only. It is never loaded as a setting or sent to workers.
#   Example:  export VOLNUX_MESH_PRIVATE_KEY="$(cat /run/secrets/mesh_private.pem)"
#
# VOLNUX_MESH_PUBLIC_KEY
#   Ed25519 public key (PEM) used to verify the config updates of other nodes. Once set
#   (or a signer is configured), unsigned updates are rejected.
#   Example:  export VOLNUX_MESH_PUBLIC_KEY="$(cat /run/secrets/mesh_public.pem)"
#
# VOLNUX_CONFIG_DIR
#   Directory searched for a `settings.py` (the directory itself, then its sub-
#   directories) when no VOLNUX_SETTINGS_MODULE is set. Default: the current directory.
#   Example:  export VOLNUX_CONFIG_DIR=/etc/volnux
