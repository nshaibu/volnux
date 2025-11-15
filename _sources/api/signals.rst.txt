API Reference - Signals
===================

Core signal system components and built-in signals.

Signal System
-----------

SoftSignal
~~~~~~~~~
.. py:class:: SoftSignal

   Base class for implementing the soft signal system.

   **Methods:**

   .. py:method:: connect(sender: Any, listener: Callable) -> None

      Connect a listener function to this signal.

      :param sender: The sender class/instance to listen for
      :param listener: The callback function to execute

   .. py:method:: disconnect(sender: Any, listener: Callable) -> None

      Disconnect a listener from this signal.

   .. py:method:: emit(sender: Any, **kwargs) -> List[Tuple[Any, Any]]

      Emit the signal to all registered listeners.

      :param sender: The sender instance triggering the signal
      :param kwargs: Additional data to pass to listeners
      :returns: List of (listener, response) tuples

Built-in Signals
--------------

Pipeline Lifecycle
~~~~~~~~~~~~~~~

.. py:data:: pipeline_pre_init

   Signal emitted before pipeline initialization.

   **Arguments:**
      - cls: The pipeline class
      - args: Initialization positional arguments
      - kwargs: Initialization keyword arguments

.. py:data:: pipeline_post_init

   Signal emitted after pipeline initialization.

   **Arguments:**
      - pipeline: The initialized pipeline instance

.. py:data:: pipeline_shutdown

   Signal emitted during pipeline shutdown.

   **Arguments:**
      - pipeline: The pipeline instance
      - execution_context: Current execution context

.. py:data:: pipeline_stop

   Signal emitted when pipeline is stopped.

   **Arguments:**
      - pipeline: The pipeline instance
      - execution_context: Current execution context

Pipeline Execution
~~~~~~~~~~~~~~~

.. py:data:: pipeline_execution_start

   Signal emitted when pipeline execution begins.

   **Arguments:**
      - pipeline: The pipeline instance

.. py:data:: pipeline_execution_end

   Signal emitted when pipeline execution completes.

   **Arguments:**
      - execution_context: Final execution context

Event Execution
~~~~~~~~~~~~

.. py:data:: event_execution_init

   Signal emitted when event execution is initialized.

   **Arguments:**
      - event: The event being executed
      - execution_context: Current execution context
      - executor: The executor instance
      - call_kwargs: Event call arguments

.. py:data:: event_execution_start

   Signal emitted when event execution starts.

   **Arguments:**
      - event: The event being executed
      - execution_context: Current execution context

.. py:data:: event_execution_end

   Signal emitted when event execution ends.

   **Arguments:**
      - event: The completed event
      - execution_context: Current execution context
      - future: The execution future object

.. py:data:: event_execution_retry

   Signal emitted when event execution is retried.

   **Arguments:**
      - event: The event being retried
      - execution_context: Current execution context
      - task_id: ID of the task being retried
      - backoff: Backoff strategy/duration
      - retry_count: Current retry attempt number
      - max_attempts: Maximum retry attempts allowed

.. py:data:: event_execution_retry_done

   Signal emitted when event retry is completed.

   **Arguments:**
      - event: The retried event
      - execution_context: Current execution context
      - task_id: ID of the retried task
      - max_attempts: Maximum attempts allowed