Tutorials - Events
===========================

Introduction
------------

This documentation provides a comprehensive guide to defining and configuring events in the event pipeline system.
Events are the core components that define the processing logic and execution behavior within the pipeline.

Define the Event Class
~~~~~~~~~~~~~~~~~~~~~~

To define an event, you need to inherit from the ``EventBase`` class and override the ``process`` method. This method
contains the logic that will be executed when the event is processed.

.. code-block:: python

   from nexus import EventBase

   class MyEvent(EventBase):
       def process(self, *args, **kwargs):
           # Event processing logic here
           return True, "Event processed successfully"

The ``process`` method should return a tuple containing:

1. A boolean indicating success (``True``) or failure (``False``)
2. A result value or message

Specify the Executor for the Event
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every event must specify an executor that defines how the event will be executed. Executors manage the concurrency or parallelism when the event is being processed.

Executors implement the ``Executor`` interface from the ``concurrent.futures._base`` module in the Python standard library. If no executor is specified, the ``DefaultExecutor`` will be used.

.. code-block:: python

   from nexus.executors import ThreadPoolExecutor

   class MyEvent(EventBase):
       executor = ThreadPoolExecutor  # Specify executor for the event

       def process(self, *args, **kwargs):
           # Event processing logic here
           return True, "Event processed successfully"

Executor Configuration
~~~~~~~~~~~~~~~~~~~~~~

The ``ExecutorInitializerConfig`` class is used to configure the initialization of an executor that manages event processing. This class allows you to control several aspects of the executor's behavior.

Configuration Fields
--------------------

The ``ExecutorInitializerConfig`` class contains the following configuration fields:

1. ``max_workers``

   - **Type**: ``int`` or ``EMPTY``
   - **Description**: Specifies the maximum number of workers (processes or threads) that can be used to execute the event.
   - **Default**: If not provided (``EMPTY``), the number of workers defaults to the number of processors available on the machine.

2. ``max_tasks_per_child``

   - **Type**: ``int`` or ``EMPTY``
   - **Description**: Defines the maximum number of tasks a worker can complete before being replaced by a new worker.
   - **Default**: If not provided (``EMPTY``), workers will live for as long as the executor runs.

3. ``thread_name_prefix``

   - **Type**: ``str`` or ``EMPTY``
   - **Description**: A string to use as a prefix when naming threads.
   - **Default**: If not provided (``EMPTY``), threads will not have a prefix.

Example Configuration
---------------------

Here's an example of how to use the ``ExecutorInitializerConfig`` class:

.. code-block:: python

   from nexus import ExecutorInitializerConfig, EventBase
   from nexus.executors import ThreadPoolExecutor

   # Configuring an executor with specific settings
   config = ExecutorInitializerConfig(
       max_workers=4,
       max_tasks_per_child=50,
       thread_name_prefix="event_executor_"
   )

   class MyEvent(EventBase):
       executor = ThreadPoolExecutor

       # Configure the executor
       executor_config = config

       def process(self, *args, **kwargs):
           # Event processing logic here
           return True, "Event processed successfully"

Alternatively, you can provide configuration as a dictionary:

.. code-block:: python

   class MyEvent(EventBase):
       executor = ThreadPoolExecutor

       # Configure the executor using a dictionary
       executor_config = {
           "max_workers": 4,
           "max_tasks_per_child": 50,
           "thread_name_prefix": "event_executor_"
       }

       def process(self, *args, **kwargs):
           # Event processing logic here
           return True, "Event processed successfully"

Default Behavior
----------------

If no fields are specified or left as ``EMPTY``, the executor will use the following default behavior:

- ``max_workers``: The number of workers will default to the number of processors on the machine.
- ``max_tasks_per_child``: Workers will continue processing tasks indefinitely, with no limit.
- ``thread_name_prefix``: Threads will not have a custom prefix.

For example:

.. code-block:: python

   config = ExecutorInitializerConfig()  # Default configuration

Function-Based Events
~~~~~~~~~~~~~~~~~~~~~

In addition to defining events using classes, you can also define events as functions using the ``event`` decorator from the ``decorators`` module.

Basic Function-Based Event
--------------------------

.. code-block:: python

   from nexus.decorators import event

   # Define a function-based event using the @event decorator
   @event()
   def my_event(*args, **kwargs):
       # Event processing logic here
       return True, "Event processed successfully"

Configuring Function-Based Events
---------------------------------

The ``event`` decorator allows you to configure the executor for the event's execution:

.. code-block:: python

   from nexus.decorators import event
   from nexus.executors import ThreadPoolExecutor

   # Define a function-based event with configuration
   @event(
       executor=ThreadPoolExecutor,               # Define the executor to use
       max_workers=4,                             # Specify max workers
       max_tasks_per_child=10,                    # Limit tasks per worker
       thread_name_prefix="my_event_executor",    # Prefix for thread names
       stop_on_exception=True                     # Stop execution on exception
   )
   def my_event(*args, **kwargs):
       # Event processing logic here
       return True, "Event processed successfully"

Event Result Evaluation
~~~~~~~~~~~~~~~~~~~~~~~

The ``EventExecutionEvaluationState`` class defines the criteria for evaluating the success or failure of an event based on the outcomes of its tasks.

Available States
----------------

- ``SUCCESS_ON_ALL_EVENTS_SUCCESS``: The event is considered successful only if all tasks succeeded. This is the **default** state.
- ``FAILURE_FOR_PARTIAL_ERROR``: The event is considered a failure if any task fails.
- ``SUCCESS_FOR_PARTIAL_SUCCESS``: The event is considered successful if at least one task succeeds.
- ``FAILURE_FOR_ALL_EVENTS_FAILURE``: The event is considered a failure only if all tasks fail.

Example Usage
-------------

.. code-block:: python

   from nexus import EventBase, EventExecutionEvaluationState

   class MyEvent(EventBase):
       execution_evaluation_state = EventExecutionEvaluationState.SUCCESS_ON_ALL_EVENTS_SUCCESS

       def process(self, *args, **kwargs):
           return True, "obrafour"

Specifying a Retry Policy for Events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For handling events that may fail intermittently, you can define a retry policy. The retry policy allows you to configure settings like maximum retry attempts, backoff strategy, and which exceptions should trigger a retry.

RetryPolicy Class
-----------------

The ``RetryPolicy`` class has the following parameters:

.. code-block:: python

   @dataclass
   class RetryPolicy(object):
       max_attempts: int   # Maximum retry attempts
       backoff_factor: float  # Backoff time between retries
       max_backoff: float  # Maximum allowed backoff time
       retry_on_exceptions: typing.List[typing.Type[Exception]]  # Exceptions that trigger a retry

Configuring the RetryPolicy
---------------------------

You can create an instance of ``RetryPolicy`` or define it as a dictionary:

.. code-block:: python

   from nexus.base import RetryPolicy

   # Define a custom retry policy as an instance
   retry_policy = RetryPolicy(
       max_attempts=5,  # Maximum number of retries
       backoff_factor=0.1,  # 10% backoff factor
       max_backoff=5.0,  # Max backoff of 5 seconds
       retry_on_exceptions=[ConnectionError, TimeoutError]  # Specific exceptions
   )

   # Or define as a dictionary
   retry_policy_dict = {
       "max_attempts": 5,
       "backoff_factor": 0.1,
       "max_backoff": 5.0,
       "retry_on_exceptions": [ConnectionError, TimeoutError]
   }

The configuration parameters are:

- ``max_attempts``: The maximum number of times the event will be retried.
- ``backoff_factor``: How long the system will wait between retry attempts, increasing with each retry.
- ``max_backoff``: The maximum time to wait between retries.
- ``retry_on_exceptions``: A list of exception types that should trigger a retry.

Assigning the Retry Policy to an Event
--------------------------------------

Once defined, you can assign the retry policy to your event class:

.. code-block:: python

   import typing
   from nexus import EventBase

   class MyEvent(EventBase):
       # Assign instance of RetryPolicy or RetryPolicy dictionary
       retry_policy = retry_policy

       def process(self, *args, **kwargs) -> typing.Tuple[bool, typing.Any]:
           pass

How the Retry Policy Works
--------------------------

When an event is processed, if it fails due to an exception in the ``retry_on_exceptions`` list:

1. The system will retry the event based on the ``max_attempts``.
2. After each retry attempt, the system waits for a time interval determined by the ``backoff_factor`` and will not exceed the ``max_backoff``.
3. If the maximum retry attempts are exceeded, the event will be marked as failed.

This retry mechanism ensures that intermittent failures do not cause a complete halt in processing and allows for better fault tolerance in your system.