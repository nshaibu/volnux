import pytest
import socket
import ssl
from unittest.mock import MagicMock, patch, call
from volnux.manager.remote_manager import RemoteTaskManager


@pytest.fixture
def mock_config_loader():
    """The manager reads its settings from module-level constants (taken from
    ``VolnuxConfig`` at import), so patch those rather than a config loader."""
    with patch(
        "volnux.manager.remote_manager.PROJECT_ROOT", "/mock/project/root"
    ), patch("volnux.manager.remote_manager.BACKLOG_SIZE", 5):
        yield


@pytest.fixture
def remote_task_manager(mock_config_loader):
    return RemoteTaskManager(
        host="127.0.0.1",
        port=8080,
        cert_path=None,
        key_path=None,
        ca_certs_path=None,
        require_client_cert=False,
        socket_timeout=5,
    )


def test_init(remote_task_manager):
    assert remote_task_manager._host == "127.0.0.1"
    assert remote_task_manager._port == 8080
    assert remote_task_manager._cert_path is None
    assert remote_task_manager._key_path is None
    assert remote_task_manager._ca_certs_path is None
    assert remote_task_manager._require_client_cert is False
    assert remote_task_manager._socket_timeout == 5


@patch("socket.socket")
def test_create_server_socket_no_ssl(mock_socket, remote_task_manager):
    mock_socket_instance = mock_socket.return_value
    server_socket = remote_task_manager._create_server_socket()
    assert server_socket == mock_socket_instance
    mock_socket_instance.setsockopt.assert_called_once_with(
        socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
    )
    # mock_socket_instance.settimeout.assert_called_once_with(5)


@patch("ssl.create_default_context")
@patch("socket.socket")
def test_create_server_socket_with_ssl(
    mock_socket, mock_ssl_context, remote_task_manager
):
    remote_task_manager._cert_path = "/path/to/cert"
    remote_task_manager._key_path = "/path/to/key"
    mock_ssl_instance = mock_ssl_context.return_value
    mock_socket_instance = mock_socket.return_value

    server_socket = remote_task_manager._create_server_socket()
    mock_ssl_instance.load_cert_chain.assert_called_once_with(
        certfile="/path/to/cert", keyfile="/path/to/key"
    )
    assert server_socket == mock_ssl_instance.wrap_socket.return_value


@patch("volnux.manager.base.Path.relative_to")
@patch("volnux.manager.base.os.walk")
@patch("volnux.manager.base.import_module")
def test_auto_load_all_task_modules(
    mock_import_module, mock_os_walk, mock_relative_path, remote_task_manager
):
    mock_os_walk.return_value = [
        ("/mock/project/root/module", [], ["__init__.py", "file.py"]),
    ]
    mock_event_class = MagicMock()
    mock_event_class.__module__ = "module"
    mock_import_module.return_value = MagicMock(**{"EventBase": mock_event_class})

    mock_relative_path.return_value = "root/module"

    with patch(
        "volnux.manager.base.inspect.getmembers",
        return_value=[("EventBase", mock_event_class)],
    ):
        with patch("volnux.manager.base.inspect.isclass", return_value=True):
            with patch(
                "volnux.manager.base.inspect.getmro",
                return_value=[object, MagicMock()],
            ):
                remote_task_manager.auto_load_all_task_modules()

    mock_import_module.assert_called()


@pytest.mark.skip(reason="Test hanging")
@patch("volnux.manager.remote_manager.socket.socket")
def test_start_and_shutdown(mock_socket, remote_task_manager):
    mock_socket_instance = mock_socket.return_value
    mock_socket_instance.accept.side_effect = [
        (MagicMock(), ("127.0.0.1", 12345)),
        socket.timeout,
        Exception,
    ]

    with patch.object(remote_task_manager, "_handle_client") as mock_handle_client:
        with patch.object(remote_task_manager, "shutdown") as mock_shutdown:
            with pytest.raises(Exception):
                remote_task_manager.start()

            mock_handle_client.assert_called_once()
            mock_shutdown.assert_called_once()

    mock_socket_instance.close.assert_called_once()
