API Reference - Executors
=========================

Core Executor Classes
---------------------

DefaultExecutor
~~~~~~~~~~~~~~~
.. py:class:: DefaultExecutor

   The simplest executor that runs tasks synchronously in the current thread.

   **Methods:**

   .. py:method:: submit(fn, /, *args, **kwargs) -> Future

      Executes the function immediately in the current thread.

      :param fn: Function to execute
      :param args: Positional arguments
      :param kwargs: Keyword arguments
      :return: Future object with the result
      :rtype: Future

ThreadPoolExecutor & ProcessPoolExecutor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Standard Python concurrent.futures executors for parallel execution.

**Configuration:**

- max_workers: Number of worker threads/processes
- thread_name_prefix: Prefix for worker thread names (ThreadPoolExecutor only)
- max_tasks_per_child: Maximum tasks per worker process (ProcessPoolExecutor only)

Remote Executors
----------------

RemoteExecutor
~~~~~~~~~~~~~~
.. py:class:: RemoteExecutor

   Executes tasks on remote servers using raw socket connections.

   **Parameters:**

   :param host: Remote server hostname/IP
   :param port: Remote server port
   :param timeout: Connection timeout in seconds
   :param use_encryption: Whether to use SSL/TLS encryption
   :param client_cert_path: Path to client certificate file
   :param client_key_path: Path to client private key file
   :param ca_cert_path: Path to CA certificate file

   **Methods:**

   .. py:method:: submit(fn, /, *args, **kwargs) -> Future

      Submit task for remote execution.

      :param fn: Function to execute
      :param args: Positional arguments
      :param kwargs: Keyword arguments
      :return: Future representing the remote execution
      :rtype: Future

XMLRPCExecutor
~~~~~~~~~~~~~~
.. py:class:: XMLRPCExecutor

   Executes tasks on remote servers using XML-RPC protocol.

   **Parameters:**

   :param host: Remote server hostname/IP
   :param port: Remote server port
   :param max_workers: Maximum number of worker threads
   :param use_encryption: Whether to use SSL/TLS
   :param client_cert_path: Path to client certificate
   :param client_key_path: Path to client key
   :param ca_cert_path: Path to CA certificate

GRPCExecutor
~~~~~~~~~~~~
.. py:class:: GRPCExecutor

   Executes tasks on remote servers using gRPC protocol.

   **Parameters:**

   :param host: Remote server hostname/IP
   :param port: Remote server port
   :param max_workers: Maximum number of worker threads
   :param use_encryption: Whether to use SSL/TLS
   :param client_cert_path: Path to client certificate
   :param client_key_path: Path to client key
   :param ca_cert_path: Path to CA certificate

Distributed Executors
---------------------

HadoopExecutor
~~~~~~~~~~~~~~
.. py:class:: HadoopExecutor

   Executes tasks on a Hadoop cluster using MapReduce.

   **Parameters:**

   :param host: Hadoop namenode hostname
   :param port: Hadoop namenode port
   :param username: Username for authentication
   :param password: Password for authentication
   :param kerb_ticket: Path to Kerberos ticket file
   :param hdfs_config: Additional HDFS configuration
   :param yarn_config: Additional YARN configuration
   :param max_workers: Maximum concurrent workers
   :param poll_interval: Job status polling interval

   **Methods:**

   .. py:method:: submit(fn, /, *args, **kwargs) -> Future

      Submit task for Hadoop execution.

      :param fn: Function to execute (must be EventBase instance)
      :param args: Positional arguments
      :param kwargs: Keyword arguments
      :return: Future representing the Hadoop job
      :rtype: Future

   .. py:method:: submit_batch(fns: List[Callable], *args_list) -> List[Future]

      Submit multiple tasks as a batch.

      :param fns: List of functions to execute
      :param args_list: Arguments for each function
      :return: List of Futures
      :rtype: List[Future]

   .. py:method:: cancel(future: Future) -> bool

      Cancel a submitted job.

      :param future: Future representing the job
      :return: True if cancellation successful
      :rtype: bool