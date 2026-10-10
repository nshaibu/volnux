from enum import Enum
from typing import Dict, Any, Optional, Union
from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy


class CheckPointFrequency(str, Enum):
    PER_TASK = "per_task"  # Before each task execution
    PERIODIC = "periodic"  # Only on timer (from checkpointer)
    ON_STATE_CHANGE = "on_state_change"  # On status change


class CheckPointPolicyConfig(BaseModel):
    """
    Configuration for when checkpointing should happen.

    Time-based values are expressed in seconds.
    """

    frequency: MiniAnnotated[
        CheckPointFrequency, Attrib(default=CheckPointFrequency.PER_TASK)
    ]
    checkpoint_interval_seconds: MiniAnnotated[float, Attrib(default=5.0, ge=0.0)]

    class Config:
        frozen = True
        init_strategy = InitStrategy.DATACLASS


class CheckPointRuntimeConfig(BaseModel):
    """
    Configuration for how checkpointing is executed.
    """

    max_concurrent_checkpoints: MiniAnnotated[int, Attrib(default=5, ge=1)]
    retry_attempts: MiniAnnotated[int, Attrib(default=3, ge=0)]
    retry_delay_seconds: MiniAnnotated[float, Attrib(default=1.0, ge=0.0)]
    checkpoint_ttl_seconds: MiniAnnotated[float, Attrib(default=300.0, ge=0.0)]

    class Config:
        # frozen = True
        init_strategy = InitStrategy.DATACLASS


class CheckPointStorageConfig(BaseModel):
    """
    Configuration for how checkpointing is stored.
    """

    extra_params: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    backend: str = "volnux.backends.stores.inmemory.InMemoryKeyValueStoreBackend"
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Union[str, int, None] = None

    class Config:
        # frozen = True
        init_strategy = InitStrategy.DATACLASS


class CheckPointConfig(BaseModel):
    """
    Configuration model for checkpoint configuration.

    This class defines the configuration structure for managing checkpoint
    policies, runtime settings, and storage settings. It leverages type
    annotations and default factories to ensure proper initialization
    and validation of its attributes. It is suited for usage where
    immutability is required.

    Schema for checkpoint configuration.
    {
        "policy": {
            "frequency": "per_task",
            "checkpoint_interval_seconds": 5.0
        },
        "runtime": {
            "max_concurrent_checkpoints": 5,
            "retry_attempts": 3,
            "retry_delay_seconds": 1.0,
            "checkpoint_ttl_seconds": 300.0
        },
        "storage": {
            "backend": "volnux.backends.stores.redis_store.RedisStoreBackend",
            "host": "localhost",
            "port": 6379,
            "username": "default",
            "password": "",
            "database": 0,
            "extra_params": {}
        },
    }

    :ivar policy: The configuration for checkpoint policies. It determines
        how checkpoints are managed during execution.
    :type policy: CheckPointPolicyConfig
    :ivar runtime: The configuration for checkpoint runtime settings. It
        handles execution-time parameters related to checkpoints.
    :type runtime: CheckPointRuntimeConfig
    :ivar storage: The configuration for checkpoint storage settings. It deals
        with parameters for managing checkpoint data persistence and storage.
    :type storage: CheckPointStorageConfig
    """

    policy: MiniAnnotated[
        CheckPointPolicyConfig, Attrib(default_factory=lambda: CheckPointPolicyConfig())
    ]
    runtime: MiniAnnotated[
        CheckPointRuntimeConfig, Attrib(default_factory=lambda: CheckPointRuntimeConfig())
    ]
    storage: MiniAnnotated[
        CheckPointStorageConfig, Attrib(default_factory=lambda: CheckPointStorageConfig())
    ]

    class Config:
        # frozen = True
        init_strategy = InitStrategy.DATACLASS
