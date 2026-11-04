from pathlib import Path

PROJECT_ROOT_DIR = Path(__file__).resolve().parent.parent

MAX_EVENT_RETRIES = 5
MAX_EVENT_BACKOFF_FACTOR = 0.05
MAX_EVENT_BACKOFF = 100

MAX_BATCH_PROCESSING_WORKERS = 4

# Pipeline-lifecycle reporters installed at bootstrap, once per process, by
# "volnux.setup.initialise_workflows" → "ensure_pipeline_reporters_installed"
# (each entry is a dotted path, imported and called with no arguments). The
# governance reporter is what produces events for the governance feed (the
# resolved, project/node-scoped storage route of GovernanceEvent)
PIPELINE_REPORTERS = ["volnux.governance.reporter.install_governance_reporter"]

KEY_VALUE_STORE_CONFIG = {
    "ENGINE": "volnux.backends.stores.inmemory.InMemoryKeyValueStoreBackend",
    # "CONNECTOR_CONFIG": {
    #     "host": "localhost",
    #     "port": 6379,
    # },
}
# Bounded resources. Process/thread pools are shared by all flows (never one pool per
# flow), and the chains of a ``{}`` group run at most this many at a time (0: no limit):
# SHARED_PROCESS_POOL_SIZE = None  # default: the CPU count
# SHARED_THREAD_POOL_SIZE = None   # default: min(32, CPU count + 4)
# SHARED_EXECUTOR_POOLS = True     # False: a pool per flow, as before
# COOPERATIVE_MAX_CONCURRENCY = 64

# Streams (``A |-> B`` between generator events) default to KEY_VALUE_STORE_CONFIG.
# Point them at another backend with a Redis or Kafka stream capability:
# STREAM_BACKEND_CONFIG = {
#     "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
#     "CONNECTOR_CONFIG": {"host": "localhost", "port": 6379},
# }
# Kafka (pip install 'volnux[kafka]') provides key-value records, pub/sub, queues
# and streams from one backend:
# KEY_VALUE_STORE_CONFIG = {
#     "ENGINE": "volnux.backends.stores.kafka.KafkaStoreBackend",
#     "CONNECTOR_CONFIG": {
#         "bootstrap_servers": "broker1:9092,broker2:9092",
#         # "username": "...", "password": "...", "ssl_enabled": True,
#         # "num_partitions": 1, "replication_factor": 3,
#     },
# }

DEFAULT_CONNECTION_TIMEOUT = 30  # seconds
DATA_CHUNK_SIZE = 4096
CONNECTION_BACKLOG_SIZE = 5
DATA_QUEUE_SIZE = 1000

DEFAULT_EVENT_VERSIONING_CLASS = {
    # Default versioning class for all events
    "DEFAULT_VERSIONING_CLASS": "volnux.versioning.NoVersioning",
    # Default version if isn't specified
    "DEFAULT_VERSION": "1.0.0",
    # Allowed versions (None = all allowed)
    "ALLOWED_VERSIONS": None,  # or ['1.0.0', '1.1.0', '2.0.0']
    # Default namespace
    "DEFAULT_NAMESPACE": "local",
    # Deprecation warnings
    "DEPRECATION_WARNINGS": True,
}


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
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
