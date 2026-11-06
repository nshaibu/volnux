import asyncio
import logging
from typing import Dict, Optional

from volnux.executors.tcp import RemoteExecutor
from volnux.manager.base import ManagerService

logger = logging.getLogger(__name__)


class ConnectivityManager:
    """
    Manages the lifecycle of persistent P2P socket connections and the
    local inbound ManagerService.

    Detailed description of the class, its purpose, and usage.

    :ivar node_id: Identifier for the local node.
    :type node_id: str
    :ivar manager: Optional ManagerService instance for managing local operations.
    :type manager: Optional[ManagerService]
    """

    def __init__(self, node_id: str):
        self.node_id = node_id

        # peer_id -> active executor
        self._executors: Dict[str, RemoteExecutor] = {}

        # peer_id -> connection_info, kept so broken links can be re-established
        self._connection_registry: Dict[str, Dict] = {}

        # Per-peer locks to prevent concurrent handshakes for the same peer
        self._peer_locks: Dict[str, asyncio.Lock] = {}

        self.manager: Optional[ManagerService] = None
        self._is_active = False
        self._watchdog_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the connectivity manager and its watchdog loop."""
        if self._is_active:
            return
        self._is_active = True
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(), name=f"watchdog:{self.node_id}"
        )
        logger.info("ConnectivityManager started for node %s.", self.node_id)

    async def stop(self) -> None:
        """
        Gracefully shut down all links and the watchdog.
        Safe to call multiple times.
        """
        self._is_active = False

        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass

        for peer_id, executor in list(self._executors.items()):
            try:
                await executor.close()
            except Exception:
                logger.exception("Error closing link to %s during shutdown.", peer_id)

        self._executors.clear()
        self._connection_registry.clear()
        logger.info("ConnectivityManager stopped for node %s.", self.node_id)

    async def ensure_link(self, peer_id: str, connection_info: Dict) -> RemoteExecutor:
        """
        Return a live, handshake executor for the given peer.

        Stores connection_info so the link can be re-established automatically
        if it drops. Concurrent calls for the same peer_id are serialised via
        a per-peer lock to prevent duplicate handshakes.

        :param peer_id: Target node identifier.
        :param connection_info: kwargs forwarded to RemoteExecutor (host, port,
            certs, etc.). Must be provided on first call; subsequent calls may
            omit it and the cached info will be reused.
        :raises RuntimeError: If connection_info is absent and the peer is not
            in the registry.
        """
        # Merge supplied info with cached info; supplied takes precedence.
        if connection_info:
            self._connection_registry[peer_id] = connection_info
        elif peer_id not in self._connection_registry:
            raise RuntimeError(
                f"No connection_info for peer {peer_id!r} and none cached. "
                "Provide connection_info on the first ensure_link call."
            )

        lock = self._peer_locks.setdefault(peer_id, asyncio.Lock())
        async with lock:
            executor = self._executors.get(peer_id)
            if executor is None or not executor.is_connected:
                info = self._connection_registry[peer_id]
                executor = RemoteExecutor(peer_id=peer_id, **info)
                await executor.handshake()
                self._executors[peer_id] = executor
                logger.info("Link established to peer %s.", peer_id)

        return self._executors[peer_id]

    async def _reconnect(self, peer_id: str) -> None:
        """
        Re-establish a dropped link using cached connection_info.
        No-op if the peer was never registered.
        """
        if peer_id not in self._connection_registry:
            logger.error(
                "Cannot reconnect to %s: no connection_info in registry.", peer_id
            )
            return

        logger.info("Attempting reconnect to %s.", peer_id)
        try:
            await self.ensure_link(peer_id, connection_info={})
        except Exception:
            logger.exception("Reconnect to %s failed.", peer_id)
            # Remove stale executor so callers get a clear error rather than
            # a connected-but-dead socket.
            self._executors.pop(peer_id, None)

    async def transport_payload(self, target_node: str, payload: Dict) -> object:
        """
        Route a framed mesh payload to the target node.

        Calls ensure_link (with cached connection_info) so a recently-dropped
        link is re-established transparently before the send.

        :param target_node: Destination node identifier.
        :param payload: Dict to frame and transmit.
        :raises RuntimeError: If target_node has no cached connection_info.
        """
        executor = await self.ensure_link(target_node, connection_info={})
        return await executor.send_framed_packet(payload)

    async def _watchdog_loop(self) -> None:
        """
        Periodically ping all known peers and trigger recovery for dead links.
        Runs until stop() is called.
        """
        while self._is_active:
            for peer_id, executor in list(self._executors.items()):
                try:
                    alive = await executor.ping()
                except Exception:
                    logger.exception("Ping to %s raised unexpectedly.", peer_id)
                    alive = False

                if not alive:
                    logger.warning("Link to %s severed. Triggering recovery.", peer_id)
                    self._executors.pop(peer_id, None)
                    asyncio.create_task(
                        self._reconnect(peer_id),
                        name=f"reconnect:{peer_id}",
                    )

            await asyncio.sleep(2)

    def _signal_mesh_recovery(self, peer_id: str) -> None:
        """
        Hook for the MeshClient to override and respond to link failures.
        Called from the watchdog when a link is confirmed dead.

        The default implementation is a no-op. Subclass or monkey-patch this
        to propagate the failure to higher-level routing logic.
        """
        pass

    async def bridge_to_peer(self, peer_id: str, connection_metadata: dict) -> None:
        """
        Spin up the appropriate transport link for a peer using TransportFactory.

        connection_metadata is not mutated. Expected keys:
            protocol  — executor type, e.g. "tcp", "grpc" (default: "tcp")
            host      — remote host address
            port      — remote port number
            *rest*    — forwarded as config kwargs (cert paths, timeouts, etc.)

        :raises KeyError: If "host" or "port" are absent from connection_metadata.
        :raises ValueError: If the protocol is not registered in TransportFactory.
        """
        lock = self._peer_locks.setdefault(peer_id, asyncio.Lock())
        async with lock:
            if peer_id in self._executors:
                return

            # Work on a copy — never mutate the caller's dict.
            meta = connection_metadata.copy()
            protocol = meta.pop("protocol", "tcp")
            host = meta.pop("host")
            port = meta.pop("port")

            executor = TransportFactory.create_executor(
                protocol=protocol,
                host=host,
                port=port,
                config=meta,
            )

            await executor.handshake()

            self._executors[peer_id] = executor
            # Store the original metadata so the link can be re-established
            # by ensure_link or _reconnect without the caller supplying it again.
            self._connection_registry[peer_id] = connection_metadata

            logger.info("Successfully bridged to %s via %s.", peer_id, protocol.upper())
