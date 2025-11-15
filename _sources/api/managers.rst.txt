API Reference - Managers
===================

Base Manager
-----------

BaseManager
~~~~~~~~~
.. py:class:: BaseManager

   Abstract base class for task execution managers.

   **Methods:**

   .. py:method:: start(*args, **kwargs)

      Start the task manager server.

   .. py:method:: shutdown()

      Shutdown the task manager server.

   .. py:method:: auto_load_all_task_modules()

      Auto-discover and load all task modules in the project.

   .. py:method:: register_task_module(module_name: str, module: ModuleType)

      Register a module containing event classes.

      :param module_name: Fully qualified module name
      :param module: Module object to register

Remote Managers
------------

RemoteTaskManager
~~~~~~~~~~~~~~
.. py:class:: RemoteTaskManager

   Server that receives and executes tasks from RemoteExecutor clients.
   Supports SSL/TLS encryption and client certificate verification.

   **Parameters:**

   :param host: Server hostname/IP
   :param port: Server port
   :param cert_path: Path to server certificate
   :param key_path: Path to server private key
   :param ca_certs_path: Path to CA certificates
   :param require_client_cert: Whether to require client certificates
   :param socket_timeout: Socket timeout in seconds

   **Methods:**

   .. py:method:: start()

      Start the task manager with proper error handling.

   .. py:method:: shutdown()

      Gracefully shutdown the task manager.

XMLRPCManager
~~~~~~~~~~
.. py:class:: XMLRPCManager

   XML-RPC server that handles remote task execution requests.

   **Parameters:**

   :param host: Server hostname/IP
   :param port: Server port
   :param use_encryption: Whether to use SSL/TLS
   :param cert_path: Path to server certificate
   :param key_path: Path to server private key
   :param ca_certs_path: Path to CA certificates
   :param require_client_cert: Whether to require client certificates

   **Methods:**

   .. py:method:: start()

      Start the RPC server.

   .. py:method:: execute(name: str, message: bytes) -> Any

      Execute a function received via RPC.

      :param name: Function name
      :param message: Function pickled body
      :return: Function result or error dict

   .. py:method:: shutdown()

      Shutdown the RPC server.

GRPCManager
~~~~~~~~~
.. py:class:: GRPCManager

   gRPC server that handles remote task execution requests.

   **Parameters:**

   :param host: Server hostname/IP
   :param port: Server port
   :param max_workers: Maximum worker threads
   :param use_encryption: Whether to use SSL/TLS
   :param server_cert_path: Path to server certificate
   :param server_key_path: Path to server private key
   :param require_client_cert: Whether to require client certificates
   :param client_ca_path: Path to client CA certificate

   **Methods:**

   .. py:method:: start()

      Start the gRPC server.

   .. py:method:: shutdown()

      Shutdown the gRPC server with a grace period.

TaskExecutorServicer
~~~~~~~~~~~~~~~~~
.. py:class:: TaskExecutorServicer

   gRPC service implementation for task execution.

   **Methods:**

   .. py:method:: Execute(request, context)

      Execute a task and return the result.

      :param request: Task execution request
      :param context: RPC context
      :return: Task execution response

   .. py:method:: ExecuteStream(request, context)

      Execute a task with streaming status updates.

      :param request: Task execution request
      :param context: RPC context
      :return: Stream of task status updates