from volnux import EventBase
from volnux.executors.grpc_executor import GRPCExecutor
from volnux.executors.remote_executor import RemoteExecutor


class GeneratorEvent(EventBase):
    # executor = RemoteExecutor
    # executor_config = {
    #     "host": "localhost",
    #     "port": 8990,
    # }

    def process(self, name: str):
        self.stop_on_error = True
        raise NotImplementedError
        return True, name


class ParallelAEvent(EventBase):
    def process(self, *args, **kwargs):
        previous_value = self.previous_result[0].content
        return True, previous_value


class ParallelBEvent(EventBase):
    def process(self, *args, **kwargs):
        previous_value = self.previous_result[0].content
        return True, previous_value


class ParallelCEvent(EventBase):
    def process(self, *args, **kwargs):
        previous_value = self.previous_result[0].content
        return True, previous_value


class PrinterEvent(EventBase):
    def process(self, *args, **kwargs):
        for e in self.previous_result:
            print(f"{e.event_name} -> {e.content}")
        return True, None
