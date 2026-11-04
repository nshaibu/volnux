import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

try:
    from confluent_kafka import Consumer, KafkaException, Producer
    from confluent_kafka.admin import AdminClient
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "The Kafka backend requires the 'confluent-kafka' package. "
        "Install it with: pip install 'volnux[kafka]'"
    ) from exc

from volnux.backends.connection import BackendConnectorBase

logger = logging.getLogger(__name__)


class KafkaConnector(BackendConnectorBase[Producer]):
    """Kafka connector built on ``confluent-kafka`` (librdkafka).

    Kafka has no single "connection": the connector owns one thread-safe
    ``Producer`` (returned by ``get_cursor``) and one ``AdminClient``, and acts as
    a factory for ``Consumer`` instances, which are stateful and therefore
    created per use by the backend (see ``new_consumer``).

    Example:
        >>> connector = KafkaConnector(host="localhost", port=9092)
        >>> connector.connect()
        >>> connector.get_cursor().produce("topic", b"value")

    ``host``/``port`` form a single bootstrap server; pass ``bootstrap_servers``
    ("h1:9092,h2:9092") for a cluster. Any librdkafka property can be supplied
    through ``producer_config`` / ``consumer_config`` / ``admin_config`` (applied
    on top of the defaults below) or ``client_config`` (applied to all three).

    Kafka transactions across records are not supported: ``transaction()``
    raises ``NotImplementedError``.
    """

    scheme = "kafka"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9092,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
        bootstrap_servers: Optional[str] = None,
        client_id: str = "volnux",
        security_protocol: Optional[str] = None,
        sasl_mechanism: str = "PLAIN",
        ssl_enabled: bool = False,
        request_timeout: float = 10.0,
        client_config: Optional[Dict[str, Any]] = None,
        producer_config: Optional[Dict[str, Any]] = None,
        consumer_config: Optional[Dict[str, Any]] = None,
        admin_config: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """Initialize the Kafka connector.

        Args:
            host: Bootstrap server hostname (ignored if ``bootstrap_servers``).
            port: Bootstrap server port.
            username: SASL username, if the cluster requires authentication.
            password: SASL password.
            timeout: General operation timeout in seconds.
            bootstrap_servers: Comma separated ``host:port`` list.
            client_id: ``client.id`` reported to the brokers.
            security_protocol: ``PLAINTEXT``/``SSL``/``SASL_PLAINTEXT``/``SASL_SSL``.
                Derived from ``username``/``ssl_enabled`` when omitted.
            sasl_mechanism: SASL mechanism used when ``username`` is set.
            ssl_enabled: Use TLS.
            request_timeout: Seconds for metadata/admin round trips.
            client_config: librdkafka properties shared by all clients.
            producer_config: Extra producer properties.
            consumer_config: Extra consumer properties.
            admin_config: Extra admin properties.
            **kwargs: Backend options kept in ``config.extra_params``
                (e.g. ``num_partitions``, see ``KafkaStoreBackend``).
        """
        super().__init__(
            host=host,
            port=port,
            username=username,
            password=password,
            timeout=timeout,
            ssl_enabled=ssl_enabled,
            **kwargs,
        )
        self.bootstrap_servers = bootstrap_servers or f"{host}:{port}"
        self.client_id = client_id
        self.request_timeout = request_timeout
        self._client_config = dict(client_config or {})
        self._producer_config = dict(producer_config or {})
        self._consumer_config = dict(consumer_config or {})
        self._admin_config = dict(admin_config or {})

        if security_protocol is None:
            if username:
                security_protocol = "SASL_SSL" if ssl_enabled else "SASL_PLAINTEXT"
            elif ssl_enabled:
                security_protocol = "SSL"
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism

        self._producer: Optional[Producer] = None
        self._admin: Optional[AdminClient] = None

    def base_config(self) -> Dict[str, Any]:
        """librdkafka properties common to every client."""
        conf: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
        }
        if self._security_protocol:
            conf["security.protocol"] = self._security_protocol
        if self.config.username:
            conf["sasl.mechanism"] = self._sasl_mechanism
            conf["sasl.username"] = self.config.username
            conf["sasl.password"] = self.config.password or ""
        conf.update(self._client_config)
        return conf

    def connect(self) -> None:
        if self._is_connected and self._producer is not None:
            return
        try:
            producer_conf = {
                # Durable by default: wait for all in-sync replicas, no
                # duplicates or reordering on producer retries.
                "acks": "all",
                "enable.idempotence": True,
                **self.base_config(),
                **self._producer_config,
            }
            self._producer = Producer(producer_conf)
            self._admin = AdminClient({**self.base_config(), **self._admin_config})
            # Producer construction is lazy; a metadata call proves the cluster
            # is reachable and surfaces auth/network errors now.
            self._admin.list_topics(timeout=self.request_timeout)
            self._is_connected = True
            logger.info("Connected to Kafka at %s", self.bootstrap_servers)
        except KafkaException as exc:
            self._reset()
            raise ConnectionError(
                f"Kafka connection to {self.bootstrap_servers} failed: {exc}"
            ) from exc

    def _reset(self) -> None:
        self._producer = None
        self._admin = None
        self._is_connected = False

    def disconnect(self) -> None:
        producer = self._producer
        self._reset()
        if producer is not None:
            try:
                producer.flush(self.config.timeout)
            except Exception as exc:
                logger.debug("Error flushing Kafka producer on disconnect: %s", exc)

    def is_connected(self) -> bool:
        return self._is_connected and self._producer is not None

    def ping(self) -> bool:
        if self._admin is None:
            return False
        try:
            self._admin.list_topics(timeout=self.request_timeout)
            return True
        except KafkaException:
            return False

    def get_cursor(self) -> Producer:
        """Return the shared (thread-safe) producer."""
        if self._producer is None:
            raise ConnectionError("Kafka connector is not connected")
        return self._producer

    @property
    def producer(self) -> Producer:
        return self.get_cursor()

    @property
    def admin(self) -> AdminClient:
        if self._admin is None:
            raise ConnectionError("Kafka connector is not connected")
        return self._admin

    def new_consumer(self, overrides: Optional[Dict[str, Any]] = None) -> Consumer:
        """Create a new, independent ``Consumer``.

        Consumers are not shared: each carries its own group membership and
        position. The caller must ``close()`` it.
        """
        conf = {
            "enable.auto.commit": False,
            **self.base_config(),
            **self._consumer_config,
            **(overrides or {}),
        }
        return Consumer(conf)

    def begin_transaction(self) -> None:
        raise NotImplementedError("Kafka backend does not support transactions")

    def commit(self) -> None:
        raise NotImplementedError("Kafka backend does not support transactions")

    def rollback(self) -> None:
        raise NotImplementedError("Kafka backend does not support transactions")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        raise NotImplementedError("Kafka backend does not support transactions")
        yield  # pragma: no cover
