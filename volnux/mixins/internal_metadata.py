import typing

__all__ = ["InternalMetadataMixin"]


class InternalMetadataMixin:
    """
    Internal metadata store shared by Pipeline and BatchPipeline.

    Provides five methods for reading and writing framework metadata.
    All data lives in ``instance.__dict__["_internal_metadata"]`` — a
    plain dict that travels with the instance through pickling, copying,
    and checkpointing.

    Convention
    ----------
    Framework components own a named top-level key. Pipeline authors and
    event authors should not write to internal metadata — they may read
    any key freely via ``get_internal()``.

    Persistence hook
    ----------------
    ``_persist_internal_metadata()`` is called after every write. The default
    implementation is a no-op. Pipeline overrides it to write to PipelineState
    cache so the metadata survives checkpointing and crash recovery. BatchPipeline
    keeps the no-op — it has no PipelineState.
    """

    _INTERNAL_KEY = "_internal_metadata"

    def set_internal(
        self,
        key: str,
        value: typing.Any,
    ) -> None:
        """
        Write a framework metadata value.

        Replaces any existing value under ``key`` entirely. To merge into
        an existing dict value, use ``update_internal()``.

        Args:
            key:   Top-level namespace key (e.g. "trigger", "window").
            value: Any picklable value. Dicts are conventional.

        Example (TriggerEngine before firing a pipeline):
            pipeline.set_internal("trigger", {
                "name":       self.name,
                "type":       type(self).__name__,
                "fired_at":   time.time(),
                "exec_id":    execution_id,
            })
        """
        if self._INTERNAL_KEY not in self.__dict__:
            self.__dict__[self._INTERNAL_KEY] = {}
        self.__dict__[self._INTERNAL_KEY][key] = value
        self._persist_internal_metadata()

    def update_internal(
        self,
        key: str,
        updates: typing.Dict[str, typing.Any],
    ) -> None:
        """
        Merge ``updates`` into an existing internal metadata dict.

        If no value exists under ``key``, an empty dict is created first.

        Args:
            key: Top-level namespace key.
            updates: Key-value pairs to merge into the existing value.

        Raises:
            TypeError: If the existing value under ``key`` is not a dict.

        Example (WindowedTrigger adding close time after window fires):
            pipeline.update_internal("window", {
                "member_count": len(window.members),
                "close_time":   time.time(),
                "fire_reason":  "size_reached",
            })
        """
        existing = self.get_internal(key)
        if existing is None:
            existing = {}
        if not isinstance(existing, dict):
            raise TypeError(
                f"update_internal({key!r}): existing value is "
                f"{type(existing).__name__}, not a dict. "
                "Use set_internal() to replace non-dict values entirely."
            )
        existing.update(updates)
        self.set_internal(key, existing)

    def get_internal(
        self,
        key: str,
        default: typing.Any = None,
    ) -> typing.Any:
        """
        Read a framework metadata value.

        Reading is unrestricted — event authors and audit code may call
        this freely at any point during execution.

        Args:
            key: Top-level namespace key.
            default: Value returned when the key is absent. Default: None.

        Returns:
            The stored value, or ``default``.
        """
        return self.__dict__.get(self._INTERNAL_KEY, {}).get(key, default)

    def get_all_internal(self) -> typing.Dict[str, typing.Any]:
        """
        Return a shallow copy of all internal metadata.

        Used by the audit subsystem at execution boundaries and by
        checkpoint serialisation to capture the full trigger/window/chain
        context in the checkpoint record.

        Returns:
            Shallow copy of all metadata. Empty dict if nothing has been set.
        """
        return dict(self.__dict__.get(self._INTERNAL_KEY, {}))

    def clear_internal(self, key: typing.Optional[str] = None) -> None:
        """
        Clear internal metadata.

        Args:
            key: If given, clears only that namespace key. If None, clears
                 all internal metadata. Used primarily in tests and by
                 RehydrationManager when resetting pipeline state before
                 a fresh execution.

        Example (clearing only window context before re-use):
            pipeline.clear_internal("window")

        Example (full reset in tests):
            pipeline.clear_internal()
        """
        if self._INTERNAL_KEY not in self.__dict__:
            return
        if key is None:
            self.__dict__[self._INTERNAL_KEY] = {}
        else:
            self.__dict__[self._INTERNAL_KEY].pop(key, None)
        self._persist_internal_metadata()

    def _persist_internal_metadata(self) -> None:
        """
        Persist the internal metadata dict after a write.

        Default: no-op. Correct for BatchPipeline, which has no PipelineState.

        Pipeline overrides this to write to PipelineState cache so the
        metadata is included in checkpoints and survives crash recovery.
        The override wraps the call in try/except because PipelineState may
        not yet be initialised during early __init__ — the dict write to
        instance.__dict__ has already happened before this is called, so a
        failed cache write is non-fatal.
        """
