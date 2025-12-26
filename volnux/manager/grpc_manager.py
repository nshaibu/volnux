import logging
import typing
import asyncio
import grpc
from concurrent import futures
from .base import BaseManager, Protocol
from volnux.protos import task_pb2, task_pb2_grpc
from volnux.executors.message import TaskMessage, deserialize_message, serialize_dict, serialize_object
from volnux.utils import create_error_response
from volnux.constants import ErrorCodes

logger = logging.getLogger(__name__)


class TaskExecutorServicer(task_pb2_grpc.TaskExecutorServicer):
    """Implementation of TaskExecutor service."""

    def __init__(self, manager):
        self.manager = manager

    async def _process_execution(self, request, context) -> typing.Dict:
        """
        Internal helper to process execution logic (Async).
        Returns the raw result dict (containing status, result, message/error).
        """
        try:
            # Reconstruct TaskMessage from request
            args_tuple, is_task = deserialize_message(request.args)
            kwargs_dict, is_task = deserialize_message(request.kwargs)
            combined_args = kwargs_dict if kwargs_dict else {}
            event_name = request.name

            # Construct TaskMessage locally
            task_msg = TaskMessage(
                event=event_name,
                args=combined_args,
                correlation_id=request.task_id if request.task_id else None
            )

            # Setup async sync
            completion_event = asyncio.Event()
            client_context = {
                "event": completion_event,
                "result_container": {}
            }

            # Dispatch
            self.manager.handle_task(task_msg, Protocol.GRPC, client_context)

            # Wait
            try:
                await asyncio.wait_for(completion_event.wait(), timeout=300)
                result_data = client_context["result_container"].get("data")
                return result_data
            except asyncio.TimeoutError:
                return create_error_response(
                    code=ErrorCodes.TASK_TIMEOUT,
                    message="Task execution timed out",
                    correlation_id=request.task_id
                )

        except Exception as e:
            logger.error(f"Error executing task {request.task_id}: {str(e)}", exc_info=e)
            return create_error_response(
                code=ErrorCodes.INTERNAL_ERROR,
                message=str(e),
                correlation_id=request.task_id
            )

    async def Execute(self, request, context):
        """Execute a task and return the result via TaskResponse (Unary)"""
        result_data = await self._process_execution(request, context)

        is_success = result_data.get("status") == "success"
        error_msg = result_data.get("message", "") if not is_success else ""

        # Serialize result
        inner_result = result_data.get("result")
        if isinstance(inner_result, dict):
            serialized_result = serialize_dict(inner_result)
        else:
            serialized_result = serialize_object(inner_result) if not isinstance(inner_result, bytes) else (inner_result or b"")

        return task_pb2.TaskResponse(
            success=is_success,
            error=error_msg,
            result=serialized_result
        )

    async def ExecuteStream(self, request, context):
        """Execute a task and yield result via TaskStatus (Streaming)"""
        result_data = await self._process_execution(request, context)

        status_str = result_data.get("status")
        msg = result_data.get("message", "")

        # Map string status to Enum
        if status_str == "success":
            status_enum = task_pb2.TaskStatus.COMPLETED
        elif status_str == "error" or status_str == "failed":
            status_enum = task_pb2.TaskStatus.FAILED
        else:
            status_enum = task_pb2.TaskStatus.PENDING # Should not happen after wait

        # Serialize result
        inner_result = result_data.get("result")
        if isinstance(inner_result, dict):
            serialized_result = serialize_dict(inner_result)
        else:
            serialized_result = serialize_object(inner_result) if not isinstance(inner_result, bytes) else (inner_result or b"")

        # Yield final status
        yield task_pb2.TaskStatus(
            status=status_enum,
            result=serialized_result,
            message=msg
        )


class GRPCManager(BaseManager):
    """
    gRPC server that handles remote task execution requests.
    """

    def __init__(
        self,
        host: str,
        port: int,
        max_workers: int = 10,
        use_encryption: bool = False,
        server_cert_path: typing.Optional[str] = None,
        server_key_path: typing.Optional[str] = None,
        require_client_cert: bool = False,
        client_ca_path: typing.Optional[str] = None,
    ) -> None:
        super().__init__(host=host, port=port)
        self._max_workers = max_workers
        self._use_encryption = use_encryption
        self._server_cert_path = server_cert_path
        self._server_key_path = server_key_path
        self._require_client_cert = require_client_cert
        self._client_ca_path = client_ca_path
        self._server = None
        self._shutdown = False

    def _route_tcp_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass

    async def _route_grpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        """
        Route response back to the gRPC handler waiting on event.
        """
        client_context = task_info.get("client_context")
        if not client_context:
            logger.error("No client context for GRPC response")
            return

        completion_event = client_context.get("event")
        result_container = client_context.get("result_container")

        if result_container is not None:
            # result_data contains: status, result, completed_at, etc.
            result_container["data"] = result_data

        if completion_event:
            completion_event.set()

    def _route_xrpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass # Not used in GRPCManager

    async def _start_grpc_server(self):
        """Start the Async gRPC Server"""
        try:
             # Create server
            self._server = grpc.aio.server(
                futures.ThreadPoolExecutor(max_workers=self._max_workers)
            )

            # Add servicer
            task_pb2_grpc.add_TaskExecutorServicer_to_server(
                TaskExecutorServicer(self), self._server
            )

            # Configure encryption if enabled
            if self._use_encryption:
                if not (self._server_cert_path and self._server_key_path):
                    raise ValueError("Server certificate and key required for encryption")

                with open(self._server_key_path, "rb") as f:
                    private_key = f.read()
                with open(self._server_cert_path, "rb") as f:
                    certificate_chain = f.read()

                root_certificates = None
                if self._require_client_cert:
                    if not self._client_ca_path:
                        raise ValueError("Client CA required when client cert is required")
                    with open(self._client_ca_path, "rb") as f:
                        root_certificates = f.read()

                server_credentials = grpc.ssl_server_credentials(
                    ((private_key, certificate_chain),),
                    root_certificates=root_certificates,
                    require_client_auth=self._require_client_cert,
                )
                port = self._server.add_secure_port(
                    f"{self._host}:{self._port}", server_credentials
                )
            else:
                port = self._server.add_insecure_port(f"{self._host}:{self._port}")

            # Start server
            await self._server.start()
            logger.info(f"Async gRPC server listening on {self._host}:{port}")

            await self._server.wait_for_termination()

        except asyncio.CancelledError:
             # Graceful stop on cancel
             if self._server:
                 await self._server.stop(grace=5)
        except Exception as e:
            logger.error(f"Error starting gRPC server: {e}")

    def _get_extra_async_tasks(self) -> typing.List[asyncio.Task]:
        """Start the gRPC server as an extra task"""
        return [asyncio.create_task(self._start_grpc_server())]

    def start(self, *args, **kwargs) -> None:
        """Start the gRPC server"""
        # Start BaseManager components (which starts thread loop -> starts extra tasks)
        super().start()

    def shutdown(self) -> None:
        """Shutdown the gRPC server"""
        super().shutdown()
        # No-op here if async loop handles it, or clean up if manual start
        self._server = None
