import unittest
import socket
import threading
import pytest
import os
import zlib
import pickle
import queue
from unittest.mock import Mock, patch
from concurrent.futures import Future
from volnux.manager.remote_manager import RemoteTaskManager
from volnux.executors.tcp import RemoteExecutor, TaskMessage


class MockContextManager:

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return


def dummy_task(x, y):
    return x + y


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


class TestRemoteExecutorWithSSL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cert_dir = BASE_DIR
        cls.server_cert = os.path.join(
            cls.cert_dir, "volnux/scripts/certificates/server.crt"
        )
        cls.server_key = os.path.join(
            cls.cert_dir, "volnux/scripts/certificates/server.key"
        )
        cls.client_cert = os.path.join(
            cls.cert_dir, "volnux/scripts/certificates/client.crt"
        )
        cls.client_key = os.path.join(
            cls.cert_dir, "volnux/scripts/certificates/client.key"
        )
        cls.ca_cert = os.path.join(cls.cert_dir, "volnux/scripts/certificates/ca.crt")

    def setUp(self):
        self.host = "localhost"
        self.port = 12345

        # Start task manager in a separate thread
        self.task_manager = RemoteTaskManager(
            host=self.host,
            port=self.port,
            cert_path=self.server_cert,
            key_path=self.server_key,
            ca_certs_path=self.ca_cert,
            require_client_cert=True,
        )
        self.server_thread = threading.Thread(target=self.task_manager.start)
        self.server_thread.daemon = True
        self.server_thread.start()

    def tearDown(self):
        self.task_manager.shutdown()
        self.server_thread.join(timeout=1)

    def test_executor_initialization(self):
        executor = RemoteExecutor(
            host=self.host,
            port=self.port,
            client_cert_path=self.client_cert,
            client_key_path=self.client_key,
            ca_cert_path=self.ca_cert,
        )
        self.assertFalse(executor._shutdown)
        self.assertTrue(executor._worker.is_alive())
        executor.shutdown()

    @pytest.mark.skip("Figure out generation of CA certificates")
    def test_task_submission_with_encryption(self):
        executor = RemoteExecutor(
            host=self.host,
            port=self.port,
            use_encryption=True,
            client_cert_path=self.client_cert,
            client_key_path=self.client_key,
            ca_cert_path=self.ca_cert,
        )

        future = executor.submit(dummy_task, 5, 3)
        result = future.result(timeout=5)
        self.assertEqual(result, 8)

        executor.shutdown()

    @pytest.mark.skip("Figure out generation of CA certificates")
    def test_multiple_task_submission(self):
        executor = RemoteExecutor(
            host=self.host,
            port=self.port,
            use_encryption=True,
            client_cert_path=self.client_cert,
            client_key_path=self.client_key,
            ca_cert_path=self.ca_cert,
        )

        futures = []
        expected_results = []

        for i in range(5):
            futures.append(executor.submit(dummy_task, i, i * 2))
            expected_results.append(i * 3)

        actual_results = [f.result(timeout=5) for f in futures]
        self.assertEqual(actual_results, expected_results)

        executor.shutdown()

    def test_executor_shutdown(self):
        executor = RemoteExecutor(host=self.host, port=self.port)

        executor.shutdown()
        self.assertTrue(executor._shutdown)

        with self.assertRaises(RuntimeError):
            executor.submit(dummy_task, 1, 2)

    @patch("socket.socket")
    def test_connection_error_handling(self, mock_socket):
        mock_socket.side_effect = socket.error("Connection refused")

        executor = RemoteExecutor(host=self.host, port=self.port)

        future = executor.submit(dummy_task, 1, 2)
        with self.assertRaises(socket.error):
            future.result(timeout=5)

        executor.shutdown()


class TestRemoteTaskManager(unittest.TestCase):
    def setUp(self):
        self.host = "localhost"
        self.port = 12346

    def test_task_manager_initialization(self):
        manager = RemoteTaskManager(
            host=self.host,
            port=self.port,
            cert_path="server.crt",
            key_path="server.key",
        )
        self.assertFalse(manager._shutdown)
        self.assertIsNone(manager._sock)

    @patch("socket.socket")
    def test_task_manager_start_stop(self, mock_socket):
        manager = RemoteTaskManager(self.host, self.port)

        # Start server in a thread
        server_thread = threading.Thread(target=manager.start)
        server_thread.daemon = True
        server_thread.start()

        # Give it time to start
        import time

        time.sleep(0.1)

        # Verify socket was created and bound
        self.assertTrue(mock_socket.called)
        mock_socket.return_value.bind.assert_called_with((self.host, self.port))
        mock_socket.return_value.listen.assert_called_with(5)

        # Shutdown
        manager.shutdown()
        server_thread.join(timeout=1)
        self.assertTrue(manager._shutdown)
        mock_socket.return_value.close.assert_called()


class TestRemoteExecutor(unittest.TestCase):
    def setUp(self):
        self.host = "localhost"
        self.port = 12345
        self.executor = RemoteExecutor(
            host=self.host, port=self.port, use_encryption=False
        )

    def tearDown(self):
        self.executor.shutdown()

    @patch("volnux.executors.remote_executor.socket.create_connection")
    def test_create_secure_connection_without_encryption(self, mock_create_connection):
        mock_socket = Mock()
        mock_create_connection.return_value = mock_socket

        connection = self.executor._create_secure_connection()
        self.assertEqual(connection, mock_socket)
        mock_create_connection.assert_called_with(
            (self.host, self.port), self.executor._timeout
        )

    @patch("volnux.executors.remote_executor.ssl.create_default_context")
    @patch("volnux.executors.remote_executor.socket.create_connection")
    def test_create_secure_connection_with_encryption(
        self, mock_create_connection, mock_ssl_context
    ):
        self.executor._use_encryption = True
        mock_socket = Mock()
        mock_ssl_socket = Mock()
        mock_create_connection.return_value = mock_socket
        mock_ssl_context.return_value.wrap_socket.return_value = mock_ssl_socket

        connection = self.executor._create_secure_connection()
        self.assertEqual(connection, mock_ssl_socket)
        mock_ssl_context.assert_called_once()
        mock_ssl_context.return_value.wrap_socket.assert_called_with(
            mock_socket, server_hostname=self.host
        )

    @patch("volnux.executors.remote_executor.socket.create_connection")
    @patch("volnux.executors.remote_executor.send_data_over_socket")
    @patch("volnux.executors.remote_executor.receive_data_from_socket")
    def test_send_task(self, mock_receive_data, mock_send_data, mock_create_connection):
        mock_receive_data.return_value = zlib.compress(pickle.dumps(42))
        mock_send_data.return_value = 100
        mock_create_connection.return_value = MockContextManager()

        task_message = TaskMessage(task_id="1", fn=dummy_task, args=(1, 2), kwargs={})
        result = self.executor._send_task(task_message)

        self.assertEqual(result, 42)
        mock_send_data.assert_called_once()
        mock_receive_data.assert_called_once()

    @patch("volnux.executors.remote_executor.socket.create_connection")
    @patch("volnux.executors.remote_executor.send_data_over_socket")
    @patch("volnux.executors.remote_executor.receive_data_from_socket")
    def test_send_task_with_error(
        self, mock_receive_data, mock_send_data, mock_create_connection
    ):
        mock_receive_data.return_value = zlib.compress(
            pickle.dumps(ValueError("Test error"))
        )
        mock_send_data.return_value = 100
        mock_create_connection.return_value = MockContextManager()

        task_message = TaskMessage(task_id="1", fn=dummy_task, args=(1, 2), kwargs={})
        with self.assertRaises(ValueError):
            self.executor._send_task(task_message)

    @patch("volnux.executors.remote_executor.send_data_over_socket")
    @patch("volnux.executors.remote_executor.receive_data_from_socket")
    def test_submit_task(self, mock_receive_data, mock_send_data):
        mock_receive_data.return_value = zlib.compress(pickle.dumps(dummy_task))
        mock_send_data.return_value = 100
        future = self.executor.submit(dummy_task, 1, 2)
        self.assertFalse(future.done())
        self.assertIn(str(id(future)), self.executor._futures)

    @patch("volnux.executors.remote_executor.RemoteExecutor.task_queue")
    @patch("volnux.executors.remote_executor.RemoteExecutor._send_task")
    def test_process_queue(self, mock_send_task, mock_queue_get):
        mock_send_task.return_value = 3

        future = Future()
        task_id = str(id(future))

        class MockQueue:
            def __init__(self, executor):
                self.executor = executor

            def get(self, *args, **kwargs):
                self.executor._shutdown = True
                return (
                    task_id,
                    future,
                    TaskMessage(task_id=task_id, fn=dummy_task, args=(1, 2), kwargs={}),
                )

        mock_queue_get.return_value = MockQueue(self.executor)

        self.executor._process_queue()

        self.assertTrue(future.done())
        self.assertEqual(future.result(), 3)

    def test_shutdown(self):
        self.executor.shutdown()
        self.assertTrue(self.executor._shutdown)
        self.executor._worker.join()

    def test_submit_after_shutdown(self):
        self.executor.shutdown()
        with self.assertRaises(RuntimeError):
            self.executor.submit(dummy_task, 1, 2)

    @pytest.mark.skip(reason="Test Hanging")
    @patch("volnux.executors.remote_executor.queue.Queue.get")
    def test_process_queue_with_empty_queue(self, mock_queue_get):
        mock_queue_get.side_effect = queue.Empty
        self.executor._process_queue()  # Should not raise any exceptions

    @patch("volnux.executors.remote_executor.socket.create_connection")
    def test_connection_error_handling(self, mock_create_connection):
        mock_create_connection.side_effect = socket.error("Connection refused")
        future = self.executor.submit(dummy_task, 1, 2)

        with self.assertRaises(socket.error):
            future.result(timeout=5)
