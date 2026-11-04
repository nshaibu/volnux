import typing
from dataclasses import dataclass, fields

from volnux.typing import ConfigState, ConfigurableValue


@dataclass
class ExecutorInitializerConfig:
    """
    Configuration for executor initialization.

    For each field:
    - UNSET: Use system defaults
    - None: Explicitly disable feature
    - Value: Use provided configuration
    """

    max_workers: ConfigurableValue[int] = ConfigState.UNSET
    max_tasks_per_child: ConfigurableValue[int] = ConfigState.UNSET
    thread_name_prefix: ConfigurableValue[str] = ConfigState.UNSET
    host: ConfigurableValue[str] = ConfigState.UNSET
    port: ConfigurableValue[int] = ConfigState.UNSET
    timeout: ConfigurableValue[int] = 30
    use_encryption: bool = False
    client_cert_path: typing.Optional[str] = None
    client_key_path: typing.Optional[str] = None
    ca_cert_path: typing.Optional[str] = None

    def resolve_max_workers(self, system_default: int = 4) -> typing.Optional[int]:
        if self.max_workers is ConfigState.UNSET:
            return system_default
        return self.max_workers

    def is_configured(self, field_name: str) -> bool:
        value = getattr(self, field_name)
        return value is not ConfigState.UNSET

    @classmethod
    def from_dict(cls, config_dict: dict) -> "ExecutorInitializerConfig":
        kwargs = {}
        for field_name, field_type in cls.__annotations__.items():
            if field_name in config_dict:
                kwargs[field_name] = config_dict[field_name]
            # Leave as default (UNSET) if not in dict
        return cls(**kwargs)

    def to_dict(self) -> dict:
        config_dict = {}
        for fd in fields(self):
            if self.is_configured(fd.name):
                config_dict[fd.name] = getattr(self, fd.name)

        config_dict["max_workers"] = self.resolve_max_workers()
        return config_dict

    def update(self, other: "ExecutorInitializerConfig") -> "ExecutorInitializerConfig":
        """Return a new config: ``self`` with everything ``other`` actually set.

        A field of ``other`` counts as set when it differs from the field's
        default. Defaults such as ``timeout=30`` or ``use_encryption=False``
        therefore never overwrite what ``self`` configured, and fields ``self``
        left UNSET are filled in. (Explicitly passing a value equal to the default
        cannot be told apart from not passing it, so it does not override.)
        """
        if not isinstance(other, ExecutorInitializerConfig):
            raise TypeError(f"Cannot add {self} to {other}")
        merged = {}
        for fd in fields(self):
            value = getattr(self, fd.name)
            candidate = getattr(other, fd.name)
            if candidate is not ConfigState.UNSET and candidate != fd.default:
                value = candidate
            merged[fd.name] = value
        return ExecutorInitializerConfig(**merged)
