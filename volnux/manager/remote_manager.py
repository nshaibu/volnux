import socket
import zlib
import time
import errno
import ssl
import typing
import pickle
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
from volnux.concurrency.async_utils import to_thread

from .base import BaseManager, Protocol
from volnux.conf import ConfigLoader
from volnux.utils import (
    send_data_over_socket,
    receive_data_from_socket,
    create_server_ssl_context,
)
from volnux.executors.message import TaskMessage, deserialize_message, serialize_dict
from volnux.manager.result_store import get_result_store
from volnux.manager.base import get_client_task_registry
from volnux.utils import create_error_response
from volnux.constants import ErrorCodes


logger = logging.getLogger(__name__)

CONF = ConfigLoader.get_lazily_loaded_config()

DEFAULT_TIMEOUT = CONF.DEFAULT_CONNECTION_TIMEOUT
CHUNK_SIZE = CONF.DATA_CHUNK_SIZE
BACKLOG_SIZE = CONF.CONNECTION_BACKLOG_SIZE
QUEUE_SIZE = CONF.DATA_QUEUE_SIZE
PROJECT_ROOT = CONF.PROJECT_ROOT_DIR


class RemoteTaskManager(BaseManager):
    """
    Server that receives and executes tasks from RemoteExecutor clients.
    Supports SSL/TLS encryption and client certificate verification.
    """

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: typing.Optional[str] = None,
        key_path: typing.Optional[str] = None,
        ca_certs_path: typing.Optional[str] = None,
        require_client_cert: bool = False,
        socket_timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Initialize the task manager.

        Args:
            host: Host to bind to
            port: Port to listen on
            cert_path: Path to server certificate file
            key_path: Path to server private key file
            ca_certs_path: Path to CA certificates for client verification
            require_client_cert: Whether to require client certificates
            socket_timeout: Socket timeout in seconds
        """
        super().__init__(host=host, port=port)
        self._cert_path = cert_path
        self._key_path = key_path
        self._ca_certs_path = ca_certs_path
        self._require_client_cert = require_client_cert
        self._socket_timeout = socket_timeout

        self._shutdown = False
        self._sock: typing.Optional[socket.socket] = None
        # self._process_context = mp.get_context("spawn")
        # self._process_pool = ProcessPoolExecutor(mp_context=self._process_context)
        self._thread_pool = ThreadPoolExecutor(max_workers=8)

    async def _route_tcp_response(self, task_info: typing.Dict, result_data: typing.Dict):
        """Route response via TCP (Async)"""
        client_context = task_info.get("client_context")
        if not client_context or not isinstance(client_context, dict):
            logger.error("Invalid client context for TCP response")
            return

        writer = client_context.get("writer")

        if not writer:
            logger.error(f"No client writer found for TCP response (correlation_id={task_info.get('correlation_id')})")
            return

        try:
            # result_data is a dictionary, use serialize_dict
            response_data = serialize_dict(result_data)

            writer.write(response_data)
            await writer.drain()

            logger.info(f"Routed response via TCP, size: {len(response_data)} bytes")

        except Exception as e:
            logger.error(f"Failed to send response via TCP: {e}. Re-raising for fallback.")
            raise

    def _route_grpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass # Not used in RemoteTaskManager

    def _route_xrpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass # Not used in RemoteTaskManager

    def _create_server_socket(self) -> socket.socket:
        """Create and configure the server socket with proper timeout and SSL if enabled"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # sock.settimeout(self._socket_timeout)
        sock.setblocking(True)

        if not (self._cert_path and self._key_path):
            return sock

        try:
            context = create_server_ssl_context(
                cert_path=self._cert_path,
                key_path=self._key_path,
                ca_certs_path=self._ca_certs_path,
                require_client_cert=self._require_client_cert,
            )

            return context.wrap_socket(sock, server_side=True)
        except (ssl.SSLError, OSError) as e:
            logger.error(f"Failed to create SSL context: {str(e)}", exc_info=e)
            raise

    async def _handle_client_async(self, reader, writer):
        """Handle a client connection (Async)"""
        addr = writer.get_extra_info('peername')
        client_info = f"{addr[0]}:{addr[1]}"
        logger.info(f"New client connection from {client_info}")

        try:
            while not self._shutdown:
                try:
                    # Receive task message
                    # Using reader.read. Protocol needs framing (msg size header) or robust deserialization.
                    # Current impl uses `receive_data_from_socket` which handles size header.
                    # We need an async version of that or use to_thread.
                    # But reader.read is raw bytes.
                    # `receive_data_from_socket` reads 4 bytes (size) then N bytes.

                    # Async framing:
                    try:
                        header_data = await reader.readexactly(4)
                        import struct
                        msg_len = struct.unpack("!I", header_data)[0]
                        msg_data = await reader.readexactly(msg_len)
                    except asyncio.IncompleteReadError:
                         break # EOF

                    if not msg_data:
                        break

                    task_message, is_task_message = deserialize_message(msg_data)
                    if not is_task_message:
                         logger.warning(f"Invalid message received from {client_info}")
                         continue

                    # Handle POLL event
                    if task_message.event == "POLL":
                        target_task_id = task_message.args.get("task_id")
                        if not target_task_id:
                             # We could send error back
                             continue

                        # Using to_thread to be safe if store has locks.
                        result = await to_thread(get_result_store().get, target_task_id)

                        if result:
                            # Found result, send it back immediately
                            response_data = serialize_dict(result)
                            writer.write(response_data)
                            await writer.drain()
                            logger.info(f"Polled result retrieved for {target_task_id}")
                        else:
                            # 2. Check Registry
                            registry = get_client_task_registry()
                            task_info = await to_thread(registry.get_task, target_task_id)

                            status = "PENDING" if task_info else "NOT_FOUND"
                            response_data = serialize_dict({
                                "correlation_id": target_task_id,
                                "status": status
                            })
                            writer.write(response_data)
                            await writer.drain()

                        continue

                    client_context = {
                        "writer": writer,
                    }

                    # Dispatch using BaseManager
                    self.handle_task(task_message, Protocol.TCP, client_context)

                except Exception as e:
                    logger.error(f"Error reading/processing message from {client_info}: {e}", exc_info=e)
                    # Try to send error response
                    try:
                        error_result = create_error_response(
                            code=ErrorCodes.PROCESSING_ERROR,
                            message=str(e)
                        )
                        response_data = serialize_dict(error_result)
                        writer.write(response_data)
                        await writer.drain()
                    except Exception:
                        pass
                    break

        except Exception as e:
            logger.error(f"Error handling connection {client_info}: {str(e)}", exc_info=e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
                logger.debug(f"Closed connection from {client_info}")
            except Exception:
                pass

    async def _start_tcp_server(self):
        """Start the Async TCP Server"""
        try:
            # SSL Context (TODO: Pass ssl context to start_server)
            ssl_ctx = None
            if self._cert_path and self._key_path:
                 ssl_ctx = create_server_ssl_context(
                    cert_path=self._cert_path,
                    key_path=self._key_path,
                    ca_certs_path=self._ca_certs_path,
                    require_client_cert=self._require_client_cert,
                 )

            server = await asyncio.start_server(
                self._handle_client_async,
                self._host,
                self._port,
                ssl=ssl_ctx
            )

            logger.info(f"Async TCP Task manager listening on {self._host}:{self._port}")

            async with server:
                await server.serve_forever()

        except asyncio.CancelledError:
             pass
        except Exception as e:
            logger.error(f"Failed to start Async TCP server: {e}")

    def _get_extra_async_tasks(self) -> typing.List[asyncio.Task]:
        """Start the TCP server as an extra task in the main loop"""
        return [asyncio.create_task(self._start_tcp_server())]

    def start(self) -> None:
        """Start the task manager"""
        # BaseManager.start() starts variables and the thread loop
        # The thread loop calls _get_extra_async_tasks which starts our server.
        super().start()

    def shutdown(self) -> None:
        """Gracefully shutdown the task manager"""
        if self._shutdown:
            return

        self._shutdown = True
        logger.info("Shutting down task manager...")

        # Shutdown BaseManager components
        super().shutdown()

        if self._sock:
            try:
                self._sock.close()
            except Exception as e:
                logger.error(f"Error closing server socket: {str(e)}", exc_info=e)

        if self._thread_pool:
            try:
                self._thread_pool.shutdown(wait=True)
            except Exception as e:
                logger.error(f"Error shutting down thread pool: {str(e)}", exc_info=e)

        logger.info("Task manager shutdown complete")
