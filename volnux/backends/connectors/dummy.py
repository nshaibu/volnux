from typing import Any

from volnux.backends.connection import BackendConnectorBase


class DummyConnector(BackendConnectorBase):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._cursor = object()  # type: ignore

    def connect(self) -> Any:
        # Simulate a connection establishment
        return self._cursor

    def disconnect(self) -> bool:
        # Simulate disconnection
        return True

    def is_connected(self) -> bool:
        # Simulate a check for connection status
        return True
