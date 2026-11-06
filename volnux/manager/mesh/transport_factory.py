import typing
from volnux.executors.base import BaseRemoteExecutor
from volnux.executors.tcp import TCPRemoteExecutor
from volnux.executors.grpc import GRPCRemoteExecutor
from volnux.executors.xmlrpc import XMLRPCRemoteExecutor


class TransportFactory:
    """
    Provisions protocol-specific RemoteExecutor instances.

    Decouples ConnectivityManager from concrete executor implementations.
    New protocols can be registered at runtime via register_custom_protocol().

    All protocol keys are normalised to lowercase on both registration and
    lookup, so "TCP", "tcp", and "Tcp" are equivalent.
    """

    _REGISTRY: typing.Dict[str, typing.Type[BaseRemoteExecutor]] = {
        "tcp": TCPRemoteExecutor,
        "grpc": GRPCRemoteExecutor,
        "xmlrpc": XMLRPCRemoteExecutor,
    }

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        """Give each subclass its own registry copy so registrations don't leak upward."""
        super().__init_subclass__(**kwargs)
        cls._REGISTRY = dict(TransportFactory._REGISTRY)

    @classmethod
    def create_executor(
        cls,
        protocol: str,
        host: str,
        port: int,
        config: typing.Optional[typing.Dict[str, typing.Any]] = None,
    ) -> BaseRemoteExecutor:
        """
        Instantiate the correct executor for the given protocol.

        :param protocol: Protocol key, e.g. "tcp", "grpc", "xmlrpc".
        :param host: Remote host address.
        :param port: Remote port number.
        :param config: Additional executor kwargs (cert paths, timeouts, etc.).
            Must not contain "host" or "port" — pass those as explicit args.
        :raises ValueError: For unknown protocols or reserved keys in config.
        :raises TypeError: If config values are incompatible with the executor.
        """
        config = config or {}

        conflicts = {"host", "port"} & config.keys()
        if conflicts:
            raise ValueError(
                f"config must not contain reserved keys {conflicts}. "
                "Pass host and port as explicit arguments to create_executor()."
            )

        key = protocol.lower()
        executor_cls = cls._REGISTRY.get(key)
        if executor_cls is None:
            available = ", ".join(sorted(cls._REGISTRY))
            raise ValueError(
                f"Unsupported protocol {protocol!r}. "
                f"Available: {available}. "
                "Register new protocols with register_custom_protocol()."
            )

        return executor_cls(host=host, port=port, **config)

    @classmethod
    def register_custom_protocol(
        cls,
        name: str,
        executor_cls: typing.Type[BaseRemoteExecutor],
        *,
        force: bool = False,
    ) -> None:
        """
        Register a new protocol at runtime.

        :param name: Protocol key (case-insensitive).
        :param executor_cls: Must be a concrete subclass of BaseRemoteExecutor.
        :param force: If True, silently overrides an existing registration.
            If False (default), raises ValueError on collision.
        :raises ValueError: If name is already registered and force is False.
        :raises TypeError: If executor_cls is not a subclass of BaseRemoteExecutor.
        """
        if not (
            isinstance(executor_cls, type)
            and issubclass(executor_cls, BaseRemoteExecutor)
        ):
            raise TypeError(
                f"{executor_cls!r} must be a concrete subclass of BaseRemoteExecutor."
            )

        key = name.lower()
        if key in cls._REGISTRY and not force:
            raise ValueError(
                f"Protocol {key!r} is already registered as {cls._REGISTRY[key].__name__}. "
                "Pass force=True to override."
            )

        cls._REGISTRY[key] = executor_cls

    @classmethod
    def available_protocols(cls) -> typing.List[str]:
        """Return the sorted list of registered protocol keys."""
        return sorted(cls._REGISTRY)
