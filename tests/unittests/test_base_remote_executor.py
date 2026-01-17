import sys
import unittest
from unittest.mock import MagicMock, patch
from volnux.executors.base_remote_executor import BaseRemoteExecutor


class TestBaseRemoteExecutor(unittest.TestCase):
    def setUp(self):
        self.executor = BaseRemoteExecutor()
        # Mocking configuration
        patcher = patch("volnux.executors.base_remote_executor.CONF")
        self.mock_conf = patcher.start()
        self.mock_conf.SECRET_KEY = "test_secret_key"
        self.mock_conf.REMOTE_EVENT_TIMEOUT = 30
        self.addCleanup(patcher.stop)

    def test_get_secret_key(self):
        """Test retrieving secret key from configuration"""
        from volnux.executors.base_remote_executor import get_secret_key

        # Test string key
        self.mock_conf.SECRET_KEY = "string_key"
        self.assertEqual(get_secret_key(), b"string_key")

        # Test bytes key
        self.mock_conf.SECRET_KEY = b"bytes_key"
        self.assertEqual(get_secret_key(), b"bytes_key")

    @patch("volnux.utils.generate_hmac")
    @patch("socket.gethostname")
    def test_construct_payload(self, mock_hostname, mock_generate_hmac):
        """Test payload construction with new signature and return type"""
        mock_hostname.return_value = "test_host"
        mock_generate_hmac.return_value = ("test_hmac", "sha256")

        event_name = "test_event"
        args = {"arg1": 1, "arg2": "value"}

        # construct_payload now returns a Payload object
        payload = self.executor.construct_payload(event_name, args)

        # Verify Payload object structure/attributes
        self.assertEqual(payload.event_name, event_name)
        self.assertEqual(payload.args, args)
        self.assertEqual(payload.client_id, "test_host")
        self.assertEqual(payload.hmac, "test_hmac")
        self.assertEqual(payload.timeout, 30)

        # Verify generate_hmac call
        mock_generate_hmac.assert_called_once()
        call_args, _ = mock_generate_hmac.call_args
        data_arg = call_args[0]
        secret_key_arg = call_args[1]

        self.assertEqual(data_arg["event_name"], event_name)
        self.assertEqual(data_arg["args"], args)
        self.assertEqual(data_arg["type"], "submission_event")
        self.assertEqual(secret_key_arg, b"test_secret_key")

    @patch("volnux.utils.generate_hmac")
    @patch("socket.gethostname")
    def test_construct_payload_edge_cases(self, mock_hostname, mock_generate_hmac):
        """Test payload construction with edge cases like empty args or special chars"""
        mock_hostname.return_value = "test_host"
        mock_generate_hmac.return_value = ("test_hmac", "sha256")

        # Special characters
        event_name = "special!@#"
        args = {"key": "val &*("}

        payload = self.executor.construct_payload(event_name, args)
        self.assertEqual(payload.event_name, event_name)
        self.assertEqual(payload.args, args)

        # Empty args
        payload_empty = self.executor.construct_payload("empty_event", {})
        self.assertEqual(payload_empty.args, {})

    @patch("volnux.utils.verify_hmac")
    def test_parse_response_success(self, mock_verify_hmac):
        """Test successful response parsing"""
        from volnux.types import TaskExecutionSuccessResponse

        mock_verify_hmac.return_value = True

        response = TaskExecutionSuccessResponse(
            correlation_id="123",
            result={"output": "success"},
            completed_at=1234567890.0,
            hmac="valid_hmac",
        )

        result = self.executor.parse_task_execution_response(response)

        self.assertEqual(result, {"output": "success"})
        mock_verify_hmac.assert_called_once()

    @patch("volnux.utils.verify_hmac")
    def test_parse_response_invalid_hmac(self, mock_verify_hmac):
        """Test parsing response with invalid HMAC"""
        from volnux.types import TaskExecutionSuccessResponse
        from volnux.exceptions import RemoteExecutionError

        mock_verify_hmac.return_value = False

        response = TaskExecutionSuccessResponse(
            correlation_id="123",
            result={"output": "success"},
            completed_at=1234567890.0,
            hmac="invalid_hmac",
        )

        with self.assertRaises(RemoteExecutionError) as cm:
            self.executor.parse_task_execution_response(response)

        self.assertEqual(str(cm.exception), "INVALID_HMAC")

    def test_parse_response_error(self):
        """Test parsing error response"""
        from volnux.types import TaskExecutionErrorResponse
        from volnux.exceptions import RemoteExecutionError

        response = TaskExecutionErrorResponse(
            correlation_id="123",
            message="Something went wrong",
            code="ERROR_CODE",
            timestamp=1234567890.0,
        )

        with self.assertRaises(RemoteExecutionError) as cm:
            self.executor.parse_task_execution_response(response)

        self.assertIn("ERROR_CODE: Something went wrong", str(cm.exception))

    def test_submit_not_implemented(self):
        """Test that submit raises NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.executor.submit(lambda: None)

    def test_query_event_exists_not_implemented(self):
        """Test that query_event_exists raises NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.executor.query_event_exists(MagicMock())
