import typing
from dataclasses import dataclass, field
from enum import IntFlag

from .operator import PipeType
from .protocols import TaskGroupingProtocol, TaskProtocol


class StandardDescriptor(IntFlag):
    FAILURE = 0
    SUCCESS = 1


@dataclass
class DescriptorConfig:
    descriptor: int
    pipe: "PipeType"
    task: "TaskProtocol"


@dataclass
class ConditionalNode:
    """Conditional node for branching"""

    _descriptors: typing.Dict[int, DescriptorConfig] = field(
        init=False, repr=True, default_factory=dict
    )

    def add_descriptor(
        self,
        descriptor: int,
        pipe: typing.Optional["PipeType"] = None,
        task: typing.Optional[
            typing.Union["TaskProtocol", "TaskGroupingProtocol"]
        ] = None,
    ) -> bool:
        """Add descriptor to conditional node"""
        if not self._is_valid_descriptor(descriptor):
            return False

        self._descriptors[descriptor] = DescriptorConfig(
            pipe=pipe, task=task, descriptor=descriptor
        )
        return True

    @staticmethod
    def _is_valid_descriptor(descriptor: int) -> bool:
        """
        Valid descriptors ranges from 0 to 9. O and 1 are standard descriptors and are used to
        denote failure and success conditions respectively.
        However, 2 to 9 descriptors are available for custom and user-defined conditionals.
        """
        return 0 <= descriptor < 10

    def get_descriptor_config(
        self, descriptor: int
    ) -> typing.Optional[DescriptorConfig]:
        return self._descriptors.get(descriptor)

    def get_descriptors(self) -> typing.List[DescriptorConfig]:
        return list(self._descriptors.values())

    @staticmethod
    def _create_descriptor_property(
        descriptor: int, attr_name: str, attr_type: typing.Type
    ):
        """Factory method to create descriptor properties with getter/setter"""

        def getter(self) -> typing.Optional[attr_type]:
            config = self.get_descriptor_config(descriptor)
            if config:
                return getattr(config, attr_name)
            return None

        def setter(self, value: attr_type):
            config = self.get_descriptor_config(descriptor)
            if config is None:
                # Create new descriptor config
                kwargs = {attr_name: value}
                # Set the other attribute to None
                other_attr = "pipe" if attr_name == "task" else "task"
                kwargs[other_attr] = None
                self.add_descriptor(descriptor, **kwargs)
            else:
                # Update existing config
                setattr(config, attr_name, value)

        return property(getter, setter)

    # Create properties using the factory method
    on_success_event = _create_descriptor_property(
        StandardDescriptor.SUCCESS, "task", TaskProtocol
    )
    on_failure_event = _create_descriptor_property(
        StandardDescriptor.FAILURE, "task", TaskProtocol
    )
    on_success_pipe = _create_descriptor_property(
        StandardDescriptor.SUCCESS, "pipe", PipeType
    )
    on_failure_pipe = _create_descriptor_property(
        StandardDescriptor.FAILURE, "pipe", PipeType
    )
