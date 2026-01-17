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

# Secret key for HMAC authentication (should be set in production)
SECRET_KEY = None

# SSL/TLS Configuration
# These settings control secure communication for remote executors and managers.
# Certificate paths can also be set via environment variables:
#   VOLNUX_SSL_CERT_PATH, VOLNUX_SSL_KEY_PATH, VOLNUX_SSL_CA_CERT_PATH, etc.
SSL_CONFIG = {
    # Certificate paths (None means not configured)
    "CERT_PATH": None,           # Path to client/server certificate (PEM format)
    "KEY_PATH": None,            # Path to private key (PEM format)
    "KEY_PASSWORD": None,        # Password for encrypted private keys
    "CA_CERT_PATH": None,        # Path to CA certificate bundle for verification

    # TLS version settings
    "MIN_TLS_VERSION": "TLSv1_2",  # Minimum TLS version (TLSv1_2 or TLSv1_3)
    "MAX_TLS_VERSION": "TLSv1_3",  # Maximum TLS version

    # Verification settings
    "VERIFY_HOSTNAME": True,       # Enable hostname verification (MITM protection)
    "VERIFY_CERTIFICATES": True,   # Enable certificate verification
    "ALLOW_SELF_SIGNED": False,    # Allow self-signed certs (INSECURE, dev only)

    # Mutual TLS (mTLS) settings
    "REQUIRE_CLIENT_CERT": False,  # Require client certificates (for servers)

    # Advanced security settings
    "CIPHER_SUITES": None,         # Custom cipher string (None = secure defaults)
    "PINNED_CERTIFICATES": [],     # List of SHA-256 fingerprints for pinning

    # Certificate monitoring
    "EXPIRATION_WARNING_DAYS": 30,  # Days before expiry to warn
}