API Reference - Backend
=======================

The backend system provides a flexible storage and persistence layer with support for multiple storage backends.

Core Components
---------------

BackendConnectorBase
~~~~~~~~~~~~~~~~~~~~
.. py:class:: BackendConnectorBase

   Abstract base class for handling backend connections.

   **Attributes:**

   :param host: Backend server host
   :param port: Backend server port
   :param username: Authentication username
   :param password: Authentication password
   :param db: Database name/number

   **Methods:**

   .. py:method:: connect()

      Establish connection to the backend.

   .. py:method:: disconnect()

      Close the backend connection.

   .. py:method:: is_connected() -> bool

      Check if currently connected to the backend.

Key-Value Store
---------------

KeyValueStoreBackendBase
~~~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: KeyValueStoreBackendBase

   Abstract base class for key-value store implementations.

   **Attributes:**

   :param connector_klass: The connector class to use for this store

   **Methods:**

   .. py:method:: exists(schema_name: str, record_key: str) -> bool

      Check if a record exists.

   .. py:method:: count(schema_name: str) -> int

      Get the number of records in a schema.

   .. py:method:: insert_record(schema_name: str, record_key: str, record: BackendIntegrationMixin)

      Insert a new record.

   .. py:method:: update_record(schema_name: str, record_key: str, record: BackendIntegrationMixin)

      Update an existing record.

   .. py:method:: delete_record(schema_name: str, record_key: str)

      Delete a record.

   .. py:method:: get_record(schema_name: str, klass: Type[BackendIntegrationMixin], record_key: Union[str, int])

      Retrieve a specific record.

   .. py:method:: reload_record(schema_name: str, record: BackendIntegrationMixin)

      Reload a record's data from storage.

   .. py:method:: filter_record(schema_name: str, record_klass: Type[BackendIntegrationMixin], **filter_kwargs)

      Filter records based on criteria.

Backend Implementations
-----------------------

InMemoryKeyValueStoreBackend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: InMemoryKeyValueStoreBackend

   In-memory implementation of key-value store.
   
   Uses a simple dictionary-based storage for testing and development.

RedisStoreBackend
~~~~~~~~~~~~~~~~~
.. py:class:: RedisStoreBackend

   Redis-based implementation of key-value store.

   Uses Redis hash sets for schema-based record storage with support for atomic operations.

HDFSStoreBackend 
~~~~~~~~~~~~~~~~
.. py:class:: HDFSStoreBackend

   HDFS implementation of key-value store.

   Stores records as JSON files in HDFS with schema-based directory organization.

   **Additional Configuration:**

   :param base_path: Base HDFS path for storage (default: "/volnux")

Connectors
----------

RedisConnector
~~~~~~~~~~~~~~
.. py:class:: RedisConnector

   Redis connection handler.

   **Parameters:**

   :param host: Redis server host
   :param port: Redis server port
   :param db: Redis database number

HDFSConnector
~~~~~~~~~~~~~
.. py:class:: HDFSConnector

   HDFS connection handler.

   **Parameters:**

   :param host: HDFS namenode host
   :param port: HDFS namenode port
   :param username: HDFS username

Connection Management
---------------------

ConnectionMode
~~~~~~~~~~~~~~
.. py:class:: ConnectionMode

   Enumeration of connection management modes.

   **Values:**

   - SINGLE: Single shared connection
   - POOLED: Connection pooling

ConnectorManagerFactory
~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: ConnectorManagerFactory

   Factory for creating appropriate connector managers.

   **Methods:**

   .. py:classmethod:: create_manager(connector_class, connector_config: dict, connection_mode: ConnectionMode = None, **kwargs)

      Create a connector manager based on backend type and configuration.

SingleConnectorManager
~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: SingleConnectorManager

   Manages a single shared connection to a backend store.

PooledConnectorManager
~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: PooledConnectorManager

   Manages a pool of connections to a backend store.

   **Parameters:**

   :param max_connections: Maximum number of connections in pool
   :param connection_timeout: Timeout for connection acquisition
   :param idle_timeout: Maximum idle time before connection cleanup