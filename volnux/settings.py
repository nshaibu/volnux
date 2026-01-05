from pathlib import Path

PROJECT_ROOT_DIR = Path(__file__).resolve().parent.parent

MAX_EVENT_RETRIES = 5
MAX_EVENT_BACKOFF_FACTOR = 0.05
MAX_EVENT_BACKOFF = 100

MAX_BATCH_PROCESSING_WORKERS = 4

RESULT_BACKEND_CONFIG = {
    "ENGINE": "volnux.backends.stores.inmemory_store.InMemoryKeyValueStoreBackend",
    # "CONNECTOR_CONFIG": {
    #     "host": "localhost",
    #     "port": 6379,
    # },
    # "CONNECTION_MODE": "pooled",  # "single" or "pooled" or "auto"
    # "MAX_CONNECTIONS": 10,  # Only used in pooled mode
    # "CONNECTION_TIMEOUT": 30,  # Seconds to wait for connection acquisition
    # "IDLE_TIMEOUT": 300,  # Seconds before closing idle connections
}

DEFAULT_CONNECTION_TIMEOUT = 30  # seconds
DATA_CHUNK_SIZE = 4096
CONNECTION_BACKLOG_SIZE = 5
DATA_QUEUE_SIZE = 1000


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

REMOTE_EVENT_TIMEOUT = None