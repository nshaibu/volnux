import asyncio
import logging
import typing
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Lock as MPLock
from multiprocessing import Manager
from threading import Lock as ThreadLock

from volnux.concurrency.async_utils import to_thread
from volnux.exceptions import PipelineError, StopProcessingError, SwitchTask
from volnux.result import EventResult, ResultSet

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ABORTED = "aborted"
    FAILED = "failed"


@dataclass
class ExecutionState:
    """Holds the execution state for a pipeline/task."""

    status: ExecutionStatus = field(default=ExecutionStatus.PENDING)
    errors: typing.List[PipelineError] = field(default_factory=list)
    results: ResultSet[EventResult] = field(default_factory=lambda: ResultSet())
    aggregated_result: typing.Optional[EventResult] = None

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        """Serialize state for IPC"""
        return {
            "status": self.status.value,
            "errors": self.errors,
            "results": self.results,
            "aggregated_result": self.aggregated_result,
        }

    @classmethod
    def from_dict(cls, data: typing.Dict[str, typing.Any]) -> "ExecutionState":
        """Deserialize state from IPC"""
        return cls(
            status=ExecutionStatus(data["status"]),
            errors=data["errors"],
            results=data["results"],
            aggregated_result=data.get("aggregated_result"),
        )

    def get_stop_processing_request(self) -> typing.Optional[Exception]:
        """Check for StopProcessingError in errors"""
        for err in self.errors:
            if isinstance(err, Exception) and err.__class__ == StopProcessingError:
                return err
        return None

    def get_switch_request(self) -> typing.Optional[Exception]:
        """Check for SwitchTask in errors"""
        for err in self.errors:
            if isinstance(err, Exception) and err.__class__ == SwitchTask:
                return err
        return None


class StateManager:
    """Singleton manager for execution states with per-state locks."""

    _instance: typing.Optional["StateManager"] = None
    _instance_lock = ThreadLock()
    _manager = None

    def __new__(cls) -> "StateManager":
        # Thread-safe singleton with double-checked locking
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialize()
                    cls._instance = instance
        return cls._instance

    def _initialize(self) -> None:
        """Initialize the manager - called once during singleton creation"""
        if self._manager is None:
            self._manager = Manager()

            # Store states in shared dict - direct memory access
            self._states = self._manager.dict()

            # Each state_id gets its own lock
            self._locks = self._manager.dict()

            # Reference counting for safe cleanup
            self._ref_counts = self._manager.dict()

            # Lock to protect state/lock creation
            self._creation_lock = self._manager.Lock()

    @classmethod
    def get_instance(cls) -> "StateManager":
        """Returns the singleton instance."""
        return cls()

    def create_state(self, state_id: str, initial_state: ExecutionState) -> None:
        """
        Create a new state with its own dedicated lock.
        Thread-safe and idempotent - safe to call multiple times.
        """
        with self._creation_lock:
            if state_id not in self._states:
                self._states[state_id] = initial_state.to_dict()
                self._locks[state_id] = self._manager.Lock()
                self._ref_counts[state_id] = 0

            # Increment reference count
            self._ref_counts[state_id] = self._ref_counts[state_id] + 1

    async def create_state_async(
        self, state_id: str, initial_state: ExecutionState
    ) -> None:
        """
        Create a new state with its own dedicated lock.
        Args:
            state_id: State ID
            initial_state: Initial state
        """

        await to_thread(self.create_state, state_id, initial_state)

    def get_state(self, state_id: str) -> ExecutionState:
        """
        Retrieve state from shared memory - no pickling overhead.
        """
        try:
            return ExecutionState.from_dict(self._states[state_id])
        except KeyError as e:
            raise KeyError(f"State {state_id} not found") from e

    async def get_state_async(self, state_id: str) -> ExecutionState:
        state = await to_thread(self.get_state, state_id)
        return typing.cast(ExecutionState, state)

    def update_state(self, state_id: str, state: ExecutionState) -> None:
        """
        Update state in shared memory - minimal overhead.
        Must be called within a lock context for thread safety.
        """
        with self.acquire(state_id):
            self._states[state_id] = state.to_dict()

    async def update_state_async(self, state_id: str, state: ExecutionState) -> None:
        """
        Async version of update_state.
        """
        async with self.acquire_async(state_id):
            self._states[state_id] = state.to_dict()

    def update_status(self, state_id: str, new_status: ExecutionStatus) -> None:
        """
        Optimized status-only update - avoids full state serialization.
        Must be called within a lock context.
        """
        with self.acquire(state_id):
            state_dict = self._states[state_id]
            state_dict["status"] = new_status.value
            self._states[state_id] = state_dict

    async def update_status_async(
        self, state_id: str, new_status: ExecutionStatus
    ) -> None:
        """
        Async version of update_status.
        """
        async with self.acquire_async(state_id):
            state_dict = self._states[state_id]
            state_dict["status"] = new_status.value
            self._states[state_id] = state_dict

    async def update_aggregated_result_async(
        self, state_id: str, result: "EventResult"
    ) -> None:
        async with self.acquire_async(state_id):
            state_dict = self._states[state_id]
            state_dict["aggregated_result"] = result
            self._states[state_id] = state_dict

    def append_error(self, state_id: str, error: typing.Any) -> None:
        """
        Optimized error append - modifies shared memory directly.
        Must be called within a lock context.
        """
        with self.acquire(state_id):
            state_dict = self._states[state_id]
            state_dict["errors"].append(error)
            self._states[state_id] = state_dict

    async def append_error_async(self, state_id: str, error: typing.Any) -> None:
        """
        Async version of append_error.
        """
        async with self.acquire_async(state_id):
            state_dict = self._states[state_id]
            state_dict["errors"].append(error)
            self._states[state_id] = state_dict

    def append_result(self, state_id: str, result: typing.Any) -> None:
        """
        Optimized result append - modifies shared memory directly.
        Must be called within a lock context.
        """
        with self.acquire(state_id):
            state_dict = self._states[state_id]
            state_dict["results"].append(result)
            self._states[state_id] = state_dict

    async def append_result_async(self, state_id: str, result: typing.Any) -> None:
        """
        Async version of append_result.
        """
        async with self.acquire_async(state_id):
            state_dict = self._states[state_id]
            state_dict["results"].append(result)
            self._states[state_id] = state_dict

    @contextmanager
    def acquire(self, state_id: str) -> typing.Generator[None, typing.Any, None]:
        """
        Context manager for acquiring ONLY this state's lock.
        Other states are completely unaffected.
        """
        if state_id not in self._locks:
            raise KeyError(f"Lock for state {state_id} not found")
        lock = self._locks[state_id]
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    @asynccontextmanager
    async def acquire_async(
        self, state_id: str
    ) -> typing.AsyncGenerator[None, typing.Any]:
        """
        Async context manager for acquiring state's lock.
        """
        if state_id not in self._locks:
            raise KeyError(f"Lock for state {state_id} not found")
        lock = self._locks[state_id]

        await to_thread(lock.acquire)
        try:
            yield
        finally:
            await to_thread(lock.release)

    def release_state(self, state_id: str, force: bool = False) -> None:
        """
        Decrement reference count and optionally remove state.
        State is only removed when the reference count reaches 0 or force=True.
        """
        if state_id not in self._states:
            return

        with self._creation_lock:
            if force:
                self._remove_state_unsafe(state_id)
            else:
                current_count = self._ref_counts.get(state_id, 0)
                if current_count > 0:
                    self._ref_counts[state_id] = current_count - 1

                if self._ref_counts[state_id] == 0:
                    self._remove_state_unsafe(state_id)

    async def release_state_async(self, state_id: str, force: bool = False) -> None:
        """
        Decrement reference count and optionally remove state asynchronously.
        State is only removed when the reference count reaches 0 or force=True.
        """
        await to_thread(self.release_state, state_id, force)

    def _remove_state_unsafe(self, state_id: str) -> None:
        """Internal method to remove state - must be called within _creation_lock"""
        if state_id in self._states:
            del self._states[state_id]
        if state_id in self._locks:
            del self._locks[state_id]
        if state_id in self._ref_counts:
            del self._ref_counts[state_id]

    def remove_state(self, state_id: str) -> None:
        """Cleanup state and its dedicated lock"""
        self.release_state(state_id, force=False)

    async def remove_state_async(self, state_id: str) -> None:
        """Cleanup state and its dedicated lock asynchronously."""
        await to_thread(self.remove_state, state_id)

    def get_ref_count(self, state_id: str) -> int:
        """Get the number of active references to a state"""
        ref_count = self._ref_counts.get(state_id, 0)
        return typing.cast(int, ref_count)

    async def get_ref_count_async(self, state_id: str) -> int:
        """Get the number of active references to a state asynchronously."""
        ref_count = await to_thread(self.get_ref_count, state_id)
        return typing.cast(int, ref_count)

    def get_active_states(self) -> typing.List[str]:
        """Get list of all state_ids in shared memory"""
        return list(self._states.keys())

    async def get_active_states_async(self) -> typing.List[str]:
        """Get the list of all state_ids in shared memory asynchronously."""
        states = await to_thread(self.get_active_states)
        return typing.cast(typing.List[str], states)

    def clear_all_states(self) -> None:
        """
        Remove all states from shared memory.
        WARNING: Only use this when you're sure no processes are using the states.
        """
        with self._creation_lock:
            self._states.clear()
            self._locks.clear()
            self._ref_counts.clear()

    async def clear_all_states_async(self) -> None:
        """Remove all states from shared memory asynchronously."""
        await to_thread(self.clear_all_states)

    def shutdown(self) -> None:
        """Shuts down the manager's server process."""
        if self._manager:
            try:
                self._manager.shutdown()
                logger.info("StateManager shut down.")
            except Exception as e:
                logger.error(f"Error shutting down StateManager: {e}")
            finally:
                self._manager = None
                self.__class__._instance = None

    def __del__(self) -> None:
        """Cleanup when manager is destroyed"""
        try:
            if hasattr(self, "_manager"):
                if self._manager:
                    self._manager.shutdown()
        except:
            pass  # Ignore errors during cleanup
