# Attribute names that are either:
#   (a) already captured in dedicated EventCheckpointSnapshot fields, OR
#   (b) framework-internal references that should never be checkpointed as attribs
EVENT_EXCLUDED_ATTRS: frozenset[str] = frozenset(
    {
        # --- publicly accessible attributes ---
        "execution_context",
        "previous_result",
        "stop_condition",
        "run_bypass_event_checks",
        "options",
        "sequence_number",
        "kwargs",
        # --- Framework internals ---
        "_pipeline",
        "_trigger",
        "_step_runner",
        "_logger",
        "_options",
        "_task_id",
        "_parent_context",
        "_init_args",
        "_call_args",
        "_phase",
        "_external_resources",
        "_resource_instances",
        "_sequence_number",
        "_checkpoint_manager",
        "_main_worker_task",
        "_command_listener_task",
        "_command_channel",
        "_resource_monitor",
        "_pause_gate",
        "_preempted",
    }
)
