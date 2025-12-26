import uuid
import logging
import typing
import grpc
from threading import Lock
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from volnux.protos import task_pb2, task_pb2_grpc
from volnux.executors.message import deserialize_message, serialize_dict

from volnux.executors.message import deserialize_message, serialize_dict

logger = logging.getLogger(__name__)

def get_event_name(fn):
    """Get the name of the function/event"""
    if hasattr(fn, "__name__"):
        return fn.__name__
    return str(fn)


class StreamingFuture(Future):
    """Future that supports status updates via callbacks"""

    def __init__(self):
        super().__init__()
        self._status_callbacks = []

    def add_status_callback(self, fn):
        """Add a callback to be called on status updates"""
        self._status_callbacks.append(fn)

    def status_update(self, status, message):
        """Called when a status update is received"""
        for callback in self._status_callbacks:
            try:
                callback(status, message)
            except Exception as e:
                logger.error(f"Error in status callback: {e}")


class GRPCExecutor(Executor):
    """
    gRPC-based executor that submits tasks to remote servers.
    """

    def __init__(
        self,
        host: str,
        port: int,
        max_workers: int = 4,
        use_encryption: bool = False,
        client_cert_path: typing.Optional[str] = None,
        client_key_path: typing.Optional[str] = None,
        ca_cert_path: typing.Optional[str] = None,
    ):
        """Initialize the gRPC executor with configuration"""
        self._host = host
        self._port = port
        self._max_workers = max_workers
        self._use_encryption = use_encryption
        self._client_cert_path = client_cert_path
        self._client_key_path = client_key_path
        self._ca_cert_path = ca_cert_path

        self._shutdown = False
        self._lock = Lock()
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._futures = set()

        # Setup gRPC channel
        self._channel = grpc.insecure_channel(f"{self._host}:{self._port}")
        if self._use_encryption:
            if not (self._client_cert_path and self._client_key_path):
                raise ValueError("Client certificate and key required for encryption")

            # Load credentials
            with open(self._client_key_path, "rb") as f:
                private_key = f.read()
            with open(self._client_cert_path, "rb") as f:
                certificate_chain = f.read()

            # Create credentials
            credentials = grpc.ssl_channel_credentials(
                root_certificates=None,  # Server verification disabled
                private_key=private_key,
                certificate_chain=certificate_chain,
            )
            self._channel = grpc.secure_channel(
                f"{self._host}:{self._port}", credentials
            )

        # Create stub
        self._stub = task_pb2_grpc.TaskExecutorStub(self._channel)

    def submit(self, fn: typing.Callable, /, *args, **kwargs) -> Future:
        """Submit a task for execution on the remote server"""
        if self._shutdown:
            raise RuntimeError("Executor has been shutdown")

        # Use StreamingFuture if long_running flag is set
        future = StreamingFuture() if kwargs.pop("long_running", False) else Future()

        # Submit task to thread pool
        submission_future = self._thread_pool.submit(
            self._submit_task, future, fn, args, kwargs
        )
        self._futures.add(submission_future)
        submission_future.add_done_callback(
            lambda f: f.result() if not f.cancelled() else None
        )

        return future

    def _submit_task(
        self, future: Future, fn: typing.Callable, args: tuple, kwargs: dict
    ) -> None:
        """Submit a task to the remote server"""
        try:
            # Serialize arguments
            # We wrap args in a dict because serialization requires dicts for signing
            # We ignore fn serialization as we rely on name

            serialized_args = serialize_dict({"_args": args})
            serialized_kwargs = serialize_dict(kwargs)


            # Create request
            request = task_pb2.TaskRequest(
                task_id=str(uuid.uuid4()),
                fn=b"",
                name=get_event_name(fn),
                args=serialized_args,
                kwargs=serialized_kwargs,
            )

            # Use streaming for StreamingFuture
            if isinstance(future, StreamingFuture):
                try:
                    for response in self._stub.ExecuteStream(request):
                        if response.status == task_pb2.TaskStatus.COMPLETED:
                            result, _ = deserialize_message(response.result)
                            future.set_result(result)
                            break
                        elif response.status == task_pb2.TaskStatus.FAILED:
                            future.set_exception(Exception(response.message))
                            break
                        else:
                            future.status_update(response.status, response.message)

                except (grpc.RpcError, Exception) as e:
                    future.set_exception(e)
            else:
                # Use regular unary call
                try:
                    response = self._stub.Execute(request)
                    if response.success:
                        result, _ = deserialize_message(response.result)
                        future.set_result(result)
                    else:
                        future.set_exception(Exception(response.error))

                except (grpc.RpcError, Exception) as e:
                    future.set_exception(e)

        except Exception as e:
            logger.error(f"Error submitting task: {e}", exc_info=True)
            future.set_exception(e)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Clean shutdown of the executor"""
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True

            if cancel_futures:
                for future in self._futures:
                    future.cancel()

            if wait:
                for future in list(self._futures):
                    try:
                        future.result()
                    except Exception:
                        logger.error(
                            "<============= error waiting for future ==============>"
                        )

            self._channel.close()
            self._thread_pool.shutdown(wait=wait, cancel_futures=cancel_futures)
