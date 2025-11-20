from volnux import EventBase
from volnux.executors.grpc_executor import GRPCExecutor


class ComputeTask(EventBase):
    # Configure the executor with remote server details
    executor = GRPCExecutor
    executor_config = {
        "host": "localhost",
        "port": 8990,
        "max_workers": 4,
        "use_encryption": False,  # Set to True for SSL/TLS
    }

    def process(self, x: int) -> tuple[bool, int]:
        # Heavy computation to be executed on remote server
        result = sum(i * i for i in range(x))
        return True, result


class SecureComputeTask(EventBase):
    # Example with encryption enabled
    executor = GRPCExecutor
    executor_config = {
        "host": "localhost",
        "port": 8991,
        "max_workers": 4,
        "use_encryption": True,
        "client_cert_path": "/path/to/client.crt",
        "client_key_path": "/path/to/client.key",
    }

    def process(self, data: list) -> tuple[bool, float]:
        # Example secure computation
        return True, sum(data) / len(data)
