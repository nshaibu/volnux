import asyncio
import random
import time
import uuid
import logging
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, Type

from volnux.config import VolnuxConfig, ConfigEntry
from volnux.executors.tcp import RemoteExecutor
from volnux.manager.base import BaseManager as ManagerService

logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()


# Number of peers each node forwards a gossip message to.
# Log-base-FANOUT(N) hops reaches the whole cluster.
_GOSSIP_FANOUT: int = 3

# Maximum time to wait for capacity responses before electing from whoever
# replied within the window.
_CAPACITY_GATHER_TIMEOUT: float = 0.5  # seconds

# Heuristic weights for node election (must sum to 1.0).
# Tune these to match your workload's sensitivity to each dimension.
_W_LATENCY: float = 0.4  # round-trip time to peer
_W_QUEUE: float = 0.3  # manager's internal queue depth
_W_CPU: float = 0.2  # remote CPU utilisation
_W_MEMORY: float = 0.1  # remote memory utilisation


@dataclass
class NodeCapacity:
    """
    Capacity metrics returned by a remote ManagerService in response to a
    CAPACITY_REQUEST system command.

    The ManagerService is responsible for populating these fields from its
    own internal queue and resource monitors before responding.
    """

    node_id: str
    queue_depth: int  # tasks waiting in the manager's internal queue
    cpu_percent: float  # 0-100
    memory_percent: float  # 0-100
    active_sessions: int  # tasks currently executing on that node


class _MembershipView:
    """
    Tracks which nodes the local node believes are alive.

    Every membership event carries a monotonically increasing *epoch*.
    The epoch guard prevents a delayed or reordered MEMBER_DOWN message
    (e.g. from a partitioned node) from evicting a peer that has since
    rejoined with a higher epoch.

    This is a local-only view that converges with the rest of the cluster
    through the epidemic gossip protocol, not through a central coordinator.
    """

    def __init__(self, local_node_id: str):
        self._members: Dict[str, int] = {}  # node_id -> epoch last seen alive
        self._epoch: int = 0
        self._lock = asyncio.Lock()
        self.local_node_id = local_node_id

    def next_epoch(self) -> int:
        """Increment and return the local epoch counter."""
        self._epoch += 1
        return self._epoch

    async def mark_up(self, node_id: str, epoch: int) -> bool:
        """Record or refresh a live node. Returns True if state changed."""
        async with self._lock:
            if self._members.get(node_id, -1) < epoch:
                self._members[node_id] = epoch
                return True
            return False

    async def mark_down(self, node_id: str, epoch: int) -> bool:
        """
        Evict a node. The epoch guard prevents a stale MEMBER_DOWN from
        removing a node that rejoined after the failure was detected.
        Returns True if the node was actually removed.
        """
        async with self._lock:
            if node_id in self._members and self._members[node_id] <= epoch:
                del self._members[node_id]
                return True
            return False

    async def known_members(self) -> List[str]:
        async with self._lock:
            return list(self._members.keys())

    async def is_alive(self, node_id: str) -> bool:
        async with self._lock:
            return node_id in self._members


class _Session:
    """
    Binds a session to the peer currently executing it and the correlation ID
    that identifies the task. Stored in the session registry so that
    _handle_peer_failure can re-route without any external look-up.

    Idempotency contract
    --------------------
    submit_deferred(correlation_id) MUST be idempotent on the remote node.
    If the same correlation_id arrives while the task is already running or
    after it has completed, the remote ManagerService must return the existing
    result rather than start a duplicate execution. This invariant is what
    makes failure-recovery re-routes safe.
    """

    __slots__ = ("peer_id", "correlation_id", "future")

    def __init__(self, peer_id: str, correlation_id: str, future: asyncio.Future):
        self.peer_id = peer_id
        self.correlation_id = correlation_id
        self.future = future


class MeshClient:
    """
    Orchestrates a cluster of RemoteExecutors (outbound) and a single
    ManagerService (inbound).

    Submission flow
    ---------------
    1. Broadcast CAPACITY_REQUEST to all eligible peers simultaneously.
    2. Collect NodeCapacity responses within _CAPACITY_GATHER_TIMEOUT.
    3. Score each respondent with a weighted heuristic (latency + queue +
       CPU + memory). Peers that don't respond in time are still eligible
       as a fallback, scored on latency alone.
    4. Submit the correlation_id to the winning executor.

    Gossip
    ------
    Both config updates and membership events use epidemic (rumour-mongering)
    gossip. Each node forwards to _GOSSIP_FANOUT random peers. A gossip_id
    per message prevents re-processing loops.

    NOTE: _seen_gossip grows without bound in the current implementation.
    In production, add a TTL-based eviction (e.g. keep only the last N IDs
    or expire entries after 2x the expected cluster-wide convergence time).

    Thread / task safety
    --------------------
    All mutable state is guarded by asyncio.Lock objects. The watcher loop
    and submission path operate on snapshots so they never hold a lock
    during I/O.
    """

    _instance: Optional["MeshClient"] = None

    def __init__(self, heartbeat_interval: float = 5.0):
        self.node_id = conf.get_node_id()
        self.config: VolnuxConfig = conf

        # Peer registry  -  NodeID -> RemoteExecutor
        self._peers: Dict[str, RemoteExecutor] = {}
        self._peers_lock = asyncio.Lock()

        # Session registry  -  session_id -> _Session
        self._sessions: Dict[str, _Session] = {}
        self._sessions_lock = asyncio.Lock()

        # Round-trip latency per peer, updated by the watcher loop
        self.peer_latencies: Dict[str, float] = {}

        # Epidemic gossip de-duplication  -  gossip_id -> seen
        self._seen_gossip: Dict[str, float] = {}
        self._gossip_ttl: float = 300.0  # 5 minutes is plenty for cluster convergence
        self._seen_gossip_lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

        # Authoritative (local) membership view
        self.membership = _MembershipView(self.node_id)

        self.manager: Optional[ManagerService] = None
        self._is_running = False

        # Cluster health check
        self.heartbeat_interval: float = heartbeat_interval
        self._watcher_task: Optional[asyncio.Task] = None

    @classmethod
    def get_instance(cls) -> "MeshClient":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    async def reset_instance(cls) -> None:
        """
        Stop the running instance (if any) then clear the singleton reference.
        Must be awaited — the old synchronous version would leave the watcher
        task and executor connections dangling between tests.
        """
        if cls._instance is not None:
            await cls._instance.stop()
            cls._instance = None

    async def start(self, host: str = "0.0.0.0", port: int = 50051) -> None:
        """Start the inbound Manager and the background health-watcher."""
        if self._is_running:
            logger.warning("MeshClient.start() called on an already-running instance.")
            return

        await self._start_manager(host, port)
        self._is_running = True
        self._watcher_task = asyncio.create_task(
            self._mesh_watcher_loop(), name=f"mesh-watcher-{self.node_id}"
        )
        self._cleanup_task = asyncio.create_task(
            self._cleanup_gossip_cache(), name=f"gossip-cleanup-{self.node_id}"
        )
        logger.info("MeshClient started - node_id=%s", self.node_id)

    async def stop(self) -> None:
        """Gracefully tear down: cancel the watcher, close all executors."""
        if not self._is_running:
            return

        self._is_running = False

        if self._watcher_task and not self._watcher_task.done():
            self._watcher_task.cancel()
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        async with self._peers_lock:
            shutdown_tasks = [e.shutdown(wait=True) for e in self._peers.values()]
            if shutdown_tasks:
                await asyncio.gather(*shutdown_tasks, return_exceptions=True)
            self._peers.clear()

        logger.info("MeshClient stopped - node_id=%s", self.node_id)

    async def _start_manager(self, host: str, port: int) -> None:
        self.manager = ManagerService(node_id=self.node_id, config=self.config)
        self.manager.on_config_received = self._handle_inbound_config_gossip
        self.manager.on_member_event = self._handle_inbound_member_event
        await self.manager.start(host, port)
        logger.info("ManagerService listening on %s:%s", host, port)

    async def register_peer(
        self,
        peer_id: str,
        executor_class: Type[RemoteExecutor],
        connection_info: Dict,
    ) -> bool:
        """
        Instantiate an executor for *peer_id*, run the handshake (mTLS,
        version check, clock sync), register it, and gossip MEMBER_UP.

        Returns True on success, False if the handshake fails.
        """
        async with self._peers_lock:
            if peer_id in self._peers:
                logger.debug("Peer %s is already registered.", peer_id)
                return True

        executor: RemoteExecutor = executor_class(
            node_id=self.node_id,
            peer_id=peer_id,
            **connection_info,
        )

        # Handshake outside any lock - it may block for a while.
        try:
            success = await executor.handshake()
        except Exception as exc:
            logger.error("Handshake with peer %s raised: %s", peer_id, exc)
            return False

        if not success:
            logger.warning("Handshake with peer %s failed.", peer_id)
            return False

        epoch = self.membership.next_epoch()
        await self.membership.mark_up(peer_id, epoch)

        async with self._peers_lock:
            self._peers[peer_id] = executor

        logger.info("Peer %s registered successfully.", peer_id)

        # Announce the new member to the rest of the cluster
        await self._gossip_member_event("MEMBER_UP", peer_id, epoch)
        return True

    async def deregister_peer(self, peer_id: str) -> None:
        """Remove a peer voluntarily (e.g. graceful leave) and gossip MEMBER_DOWN."""
        async with self._peers_lock:
            executor = self._peers.pop(peer_id, None)

        if executor:
            await executor.shutdown(wait=True)
            epoch = self.membership.next_epoch()
            await self.membership.mark_down(peer_id, epoch)
            await self._gossip_member_event("MEMBER_DOWN", peer_id, epoch)
            logger.info("Peer %s deregistered.", peer_id)

    async def _cleanup_gossip_cache(self) -> None:
        """
        Periodically prunes the gossip deduplication cache to prevent memory bloat.
        Runs every 60 seconds to remove entries older than self._gossip_ttl.
        """
        while self._is_running:
            try:
                await asyncio.sleep(60)  # Run cleanup once per minute

                now = time.time()
                expired_ids = []

                async with self._seen_gossip_lock:
                    # Identify keys that have outlived the TTL
                    for gossip_id, timestamp in self._seen_gossip.items():
                        if now - timestamp > self._gossip_ttl:
                            expired_ids.append(gossip_id)

                    # Batch remove to minimize lock contention
                    for gossip_id in expired_ids:
                        del self._seen_gossip[gossip_id]

                if expired_ids:
                    logger.debug(
                        "Gossip Cache Cleanup: Pruned %d expired IDs.", len(expired_ids)
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error during gossip cache cleanup: %s", e)

    async def submit_to_mesh(
        self,
        correlation_id: str,
        preferred_node: str = None,
        *,
        exclude_nodes: FrozenSet[str] = frozenset(),
    ) -> Any:
        """
        Route the task identified by *correlation_id* to the best available peer.

        Steps
        -----
        1. Snapshot eligible peers (excluding *exclude_nodes*).
        2. Broadcast CAPACITY_REQUEST to all of them; collect responses within
           _CAPACITY_GATHER_TIMEOUT.
        3. Score each respondent with the weighted heuristic; elect winner.
        4. Submit the correlation_id to the winning executor and await the result.

        See _Session for the idempotency contract that makes re-routes safe.
        """
        async with self._peers_lock:
            peers_snapshot: Dict[str, RemoteExecutor] = {
                pid: ex for pid, ex in self._peers.items() if pid not in exclude_nodes
            }

        if not peers_snapshot:
            raise RuntimeError("No available executor in the mesh.")

        capacities = await self._solicit_capacities(peers_snapshot)

        target_id = self._elect_best_node(
            capacities=capacities,
            peers=peers_snapshot,
            preferred_node=preferred_node,
            exclude_nodes=exclude_nodes,
        )
        if target_id is None:
            raise RuntimeError("Node election produced no winner.")

        executor = peers_snapshot[target_id]
        session_id = f"sess-{uuid.uuid4().hex[:8]}"
        task_future: asyncio.Future = executor.submit_deferred(correlation_id)

        session = _Session(
            peer_id=target_id,
            correlation_id=correlation_id,
            future=task_future,
        )
        async with self._sessions_lock:
            self._sessions[session_id] = session

        try:
            return await task_future
        except Exception as exc:
            logger.error(
                "Session %s on peer %s raised: %s - triggering recovery.",
                session_id,
                target_id,
                exc,
            )
            await asyncio.create_task(
                self._handle_peer_failure(target_id),
                name=f"recovery-{target_id}",
            )
            raise
        finally:
            async with self._sessions_lock:
                self._sessions.pop(session_id, None)

    async def _solicit_capacities(
        self, peers: Dict[str, RemoteExecutor]
    ) -> Dict[str, NodeCapacity]:
        """
        Fan out CAPACITY_REQUEST to all peers simultaneously.
        Peers that time out or error within _CAPACITY_GATHER_TIMEOUT are
        excluded from the scored election but may still be used as a latency-
        only fallback inside _elect_best_node.
        """

        async def _fetch(
            peer_id: str, executor: RemoteExecutor
        ) -> Tuple[str, Optional[NodeCapacity]]:
            try:
                capacity = await asyncio.wait_for(
                    executor.request_capacity(),
                    timeout=_CAPACITY_GATHER_TIMEOUT,
                )
                return peer_id, capacity
            except Exception as exc:
                logger.debug("Capacity request to %s failed: %s", peer_id, exc)
                return peer_id, None

        results = await asyncio.gather(
            *(_fetch(pid, ex) for pid, ex in peers.items()),
        )
        return {pid: cap for pid, cap in results if cap is not None}

    async def broadcast_config_change(self, key: str, value: Any) -> None:
        """
        Increment the Lamport clock, stamp a gossip_id, and begin the epidemic.
        Each recipient node forwards to _GOSSIP_FANOUT of its own peers until
        the gossip_id has been seen by every node.
        """
        self.config._lamport_clock += 1
        clock_value = self.config._lamport_clock
        gossip_id = uuid.uuid4().hex

        payload = {
            "gossip_id": gossip_id,
            "key": key,
            "value": value,
            "timestamp": clock_value,
            "origin_node": self.node_id,
        }

        async with self._seen_gossip_lock:
            self._seen_gossip[gossip_id] = time.time()

        await self._epidemic_send("CONFIG_UPDATE", payload)

    def _handle_inbound_config_gossip(self, payload: Dict) -> None:
        """
        Callback registered on ManagerService for CONFIG_UPDATE arrivals.
        Schedules deduplication + relay as a fire-and-forget task so the
        ManagerService callback stays non-blocking.
        """
        gossip_id = payload.get("gossip_id")
        if gossip_id:
            asyncio.create_task(
                self._relay_config_gossip(gossip_id, payload),
                name=f"relay-config-{gossip_id[:8]}",
            )

    async def _relay_config_gossip(self, gossip_id: str, payload: Dict) -> None:
        async with self._seen_gossip_lock:
            if gossip_id in self._seen_gossip:
                return  # already processed - stop propagation
            self._seen_gossip[gossip_id] = time.time()

        entry = ConfigEntry(
            value=payload["value"],
            timestamp=payload["timestamp"],
            origin_mesh_node=payload["origin_mesh_node"],
        )
        entry.key = payload["key"]
        self.config.update_from_mesh(entry)

        # Continue the epidemic to our own random subset
        await self._epidemic_send("CONFIG_UPDATE", payload)

    async def _gossip_member_event(self, event: str, node_id: str, epoch: int) -> None:
        """Stamp a new gossip_id and begin the epidemic for a membership change."""
        gossip_id = uuid.uuid4().hex
        payload = {
            "gossip_id": gossip_id,
            "event": event,  # "MEMBER_UP" | "MEMBER_DOWN"
            "node_id": node_id,
            "epoch": epoch,
            "origin_node": self.node_id,
        }
        async with self._seen_gossip_lock:
            self._seen_gossip[gossip_id] = time.time()

        await self._epidemic_send("MEMBER_EVENT", payload)

    def _handle_inbound_member_event(self, payload: Dict) -> None:
        """Callback registered on ManagerService for MEMBER_EVENT arrivals."""
        gossip_id = payload.get("gossip_id")
        if gossip_id:
            asyncio.create_task(
                self._relay_member_event(gossip_id, payload),
                name=f"relay-member-{gossip_id[:8]}",
            )

    async def _relay_member_event(self, gossip_id: str, payload: Dict) -> None:
        async with self._seen_gossip_lock:
            if gossip_id in self._seen_gossip:
                return
            self._seen_gossip[gossip_id] = time.time()

        event = payload["event"]
        node_id = payload["node_id"]
        epoch = payload["epoch"]

        if event == "MEMBER_UP":
            changed = await self.membership.mark_up(node_id, epoch)
            if changed:
                logger.info("Membership: %s rejoined (epoch=%d).", node_id, epoch)

        elif event == "MEMBER_DOWN":
            evicted = await self.membership.mark_down(node_id, epoch)
            if evicted:
                logger.warning(
                    "Membership: %s evicted via gossip (epoch=%d).", node_id, epoch
                )
                # Trigger local cleanup. already_gossiped=True prevents this
                # relay from emitting a second MEMBER_DOWN for the same event.
                await self._handle_peer_failure(node_id, already_gossiped=True)

        # Continue the epidemic regardless of whether we acted on the event
        await self._epidemic_send("MEMBER_EVENT", payload)

    async def _epidemic_send(self, command: str, payload: Dict) -> None:
        """
        Forward *command* to a random subset of up to _GOSSIP_FANOUT peers.
        Recipients are expected to do the same, giving O(log N) convergence.
        Failures are logged at DEBUG level - a failed relay is not fatal
        because other paths in the epidemic will cover the same nodes.
        """
        async with self._peers_lock:
            all_peers = list(self._peers.items())

        if not all_peers:
            return

        subset = random.sample(all_peers, min(_GOSSIP_FANOUT, len(all_peers)))

        results = await asyncio.gather(
            *(ex.send_system_command(command, payload) for _, ex in subset),
            return_exceptions=True,
        )
        failed = sum(1 for r in results if isinstance(r, Exception))
        if failed:
            logger.debug(
                "Epidemic %s: %d/%d targets failed (will converge via other paths).",
                command,
                failed,
                len(subset),
            )

    async def _mesh_watcher_loop(self) -> None:
        """
        Background loop: ping every peer on each heartbeat interval.
        Latency is recorded for the election heuristic; failures trigger recovery.
        """
        while self._is_running:
            async with self._peers_lock:
                peers_snapshot = list(self._peers.items())

            if peers_snapshot:
                await asyncio.gather(
                    *(self._ping_peer(pid, ex) for pid, ex in peers_snapshot),
                    return_exceptions=True,
                )

            await asyncio.sleep(self.heartbeat_interval)

    async def _ping_peer(self, peer_id: str, executor: RemoteExecutor) -> None:
        """Issue a single SYSTEM_PING; update latency or trigger failure recovery."""
        t0 = time.perf_counter()
        try:
            success = await asyncio.wait_for(executor.ping(), timeout=2.0)
            if success:
                self.peer_latencies[peer_id] = time.perf_counter() - t0
            else:
                logger.warning("Peer %s returned unhealthy ping.", peer_id)
                await self._handle_peer_failure(peer_id)
        except asyncio.TimeoutError:
            logger.warning("Peer %s ping timed out.", peer_id)
            await self._handle_peer_failure(peer_id)
        except Exception as exc:
            logger.error("Peer %s ping error: %s", peer_id, exc)
            await self._handle_peer_failure(peer_id)

    async def _handle_peer_failure(
        self, peer_id: str, *, already_gossiped: bool = False
    ) -> None:
        """
        1. Evict the dead executor from local state.
        2. Update membership and gossip MEMBER_DOWN — unless already_gossiped=True,
           which is set by _relay_member_event to prevent feedback loops.
        3. Re-route all sessions that were running on the failed peer.

        The re-route re-enters submit_to_mesh with exclude_nodes set, so the new
        capacity solicitation naturally avoids the dead peer.
        """
        logger.warning("Peer %s declared failed - starting recovery.", peer_id)

        async with self._peers_lock:
            executor = self._peers.pop(peer_id, None)
        self.peer_latencies.pop(peer_id, None)

        if executor:
            await executor.shutdown(wait=False)

        if not already_gossiped:
            epoch = self.membership.next_epoch()
            await self.membership.mark_down(peer_id, epoch)
            await self._gossip_member_event("MEMBER_DOWN", peer_id, epoch)

        async with self._sessions_lock:
            orphaned = [
                (sid, sess)
                for sid, sess in self._sessions.items()
                if sess.peer_id == peer_id
            ]
            for sid, _ in orphaned:
                del self._sessions[sid]

        if not orphaned:
            return

        logger.info(
            "Re-routing %d orphaned session(s) from %s.", len(orphaned), peer_id
        )

        async with self._peers_lock:
            has_peers = bool(self._peers)

        if not has_peers:
            logger.error(
                "No surviving peers - %d session(s) from %s cannot be recovered.",
                len(orphaned),
                peer_id,
            )
            for _, sess in orphaned:
                if not sess.future.done():
                    sess.future.set_exception(
                        RuntimeError(
                            f"Peer {peer_id} failed and no fallback is available."
                        )
                    )
            return

        for sid, sess in orphaned:
            logger.debug("Re-routing session %s (was on %s).", sid, peer_id)
            await asyncio.create_task(
                self.submit_to_mesh(
                    sess.correlation_id,
                    exclude_nodes=frozenset({peer_id}),
                ),
                name=f"reroute-{sid}",
            )

    def _elect_best_node(
        self,
        *,
        capacities: Dict[str, NodeCapacity],
        peers: Dict[str, RemoteExecutor],
        preferred_node: str = None,
        exclude_nodes: FrozenSet[str] = frozenset(),
    ) -> Optional[str]:
        """
        Score each candidate and return the lowest-scoring (best) node.

        Score formula (lower = better):
            score = W_LATENCY * latency_seconds
                  + W_QUEUE   * queue_depth
                  + W_CPU     * (cpu_percent  / 100)
                  + W_MEMORY  * (memory_percent / 100)

        Peers that did not respond to the capacity request are still eligible
        as a last resort, scored on latency alone with a +1.0 penalty offset
        so they rank below any peer that did respond.

        The preferred_node hint is honoured only when it returned capacity data.
        A non-responsive preferred node falls through to the heuristic winner.
        """
        candidates = {pid for pid in peers if pid not in exclude_nodes}
        if not candidates:
            return None

        # Fast-path: preferred node is healthy and responded with capacity data
        if (
            preferred_node
            and preferred_node in candidates
            and preferred_node in capacities
        ):
            return preferred_node

        def _score(pid: str) -> float:
            latency = self.peer_latencies.get(pid, float("inf"))
            cap = capacities.get(pid)
            if cap is None:
                # No capacity data - penalise but don't disqualify
                return _W_LATENCY * latency + 1.0
            return (
                _W_LATENCY * latency
                + _W_QUEUE * cap.queue_depth
                + _W_CPU * (cap.cpu_percent / 100.0)
                + _W_MEMORY * (cap.memory_percent / 100.0)
            )

        return min(candidates, key=_score)

    async def get_mesh_summary(self) -> Dict:
        async with self._peers_lock:
            peers = list(self._peers.keys())
        async with self._sessions_lock:
            session_count = len(self._sessions)
        known_members = await self.membership.known_members()

        return {
            "local_node": self.node_id,
            "active_peers": peers,
            "known_members": known_members,
            "peer_latencies": dict(self.peer_latencies),
            "active_sessions": session_count,
            "clock": self.config._lamport_clock,
            "is_running": self._is_running,
        }
