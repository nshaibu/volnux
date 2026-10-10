from formax import Attrib, BaseModel, MiniAnnotated


class ScalingConfig(BaseModel):
    """Configuration for the adaptive scaling engine"""

    # Total CPU cores available (Q_max)
    max_cpu_quota: MiniAnnotated[float, Attrib(default=4.0)]

    # Total memory in GB
    max_memory_quota: MiniAnnotated[float, Attrib(default=8.0)]

    # Estimated CPU cores per worker (R_w)
    cpu_per_worker: MiniAnnotated[float, Attrib(default=1.0)]

    # Estimated memory GB per worker
    memory_per_worker: MiniAnnotated[float, Attrib(default=0.5)]

    # Multiplier for batch size calculation
    parallelism_multiplier: MiniAnnotated[int, Attrib(default=2)]

    # Queue utilization to trigger scale up
    scale_up_threshold: MiniAnnotated[float, Attrib(default=0.7)]

    # Idle time before scaling down (seconds)
    scale_down_timeout: MiniAnnotated[float, Attrib(default=10.0)]

    # Minimum number of workers
    min_workers: MiniAnnotated[int, Attrib(default=1)]

    # How often to check metrics (seconds)
    monitoring_interval: MiniAnnotated[float, Attrib(default=1.0)]

    # CPU usage below this triggers scale down (as % of quota)
    cpu_threshold_scale_down: MiniAnnotated[float, Attrib(default=0.3)]

    # CPU usage above this prevents scale up (as % of quota)
    cpu_threshold_scale_up: MiniAnnotated[float, Attrib(default=0.85)]

    # Memory usage threshold (90% of quota)
    memory_threshold: MiniAnnotated[float, Attrib(default=0.9)]

    # Enable aggressive real-time scaling
    aggressive_scaling: MiniAnnotated[
        bool,
        Attrib(default=True),
    ]
