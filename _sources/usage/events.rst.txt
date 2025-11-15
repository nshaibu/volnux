Events
======

This section covers how to create and work with events in the volnux framework.

Defining Events
-------------

Events can be defined either as classes or functions. Here are both approaches:

Class-Based Events
~~~~~~~~~~~~~~~~

.. code-block:: python

    from volnux import EventBase

    class MyEvent(EventBase):
        def process(self, *args, **kwargs):
            # Event processing logic here
            return True, "Event processed successfully"

Function-Based Events
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from volnux.decorators import event

    @event()
    def my_event(*args, **kwargs):
        # Event processing logic here
        return True, "Event processed successfully"

Executor Configuration
-------------------

Every event needs an executor that defines how it will be executed. The framework provides the ExecutorInitializerConfig class for configuring executors:

.. code-block:: python

    from concurrent.futures import ThreadPoolExecutor
    from volnux import EventBase, ExecutorInitializerConfig

    class MyEvent(EventBase):
        executor = ThreadPoolExecutor
        
        # Configure the executor using ExecutorInitializerConfig
        executor_config = ExecutorInitializerConfig(
            max_workers=4,
            max_tasks_per_child=50,
            thread_name_prefix="event_executor_"
        )

        def process(self, *args, **kwargs):
            return True, "Event processed successfully"

Or using dictionary configuration:

.. code-block:: python

    class MyEvent(EventBase):
        executor = ThreadPoolExecutor
        
        executor_config = {
            "max_workers": 4,
            "max_tasks_per_child": 50,
            "thread_name_prefix": "event_executor_"
        }

Event Result Evaluation
--------------------

The EventExecutionEvaluationState class defines how event success or failure is determined:

.. code-block:: python

    from volnux import EventBase, EventExecutionEvaluationState

    class MyEvent(EventBase):
        # Set the execution evaluation state
        execution_evaluation_state = EventExecutionEvaluationState.SUCCESS_ON_ALL_EVENTS_SUCCESS
        
        def process(self, *args, **kwargs):
            return True, "Success"

Available Evaluation States:

- **SUCCESS_ON_ALL_EVENTS_SUCCESS**: Success only if all tasks succeed (default)
- **FAILURE_FOR_PARTIAL_ERROR**: Failure if any task fails
- **SUCCESS_FOR_PARTIAL_SUCCESS**: Success if at least one task succeeds
- **FAILURE_FOR_ALL_EVENTS_FAILURE**: Failure only if all tasks fail

Retry Policy
----------

Events can be configured with retry policies for handling failures:

.. code-block:: python

    import typing
    from volnux import EventBase
    from volnux.base import RetryPolicy

    # Define retry policy
    retry_policy = RetryPolicy(
        max_attempts=5,
        backoff_factor=0.1,
        max_backoff=5.0,
        retry_on_exceptions=[ConnectionError, TimeoutError]
    )

    class MyEvent(EventBase):
        # Assign retry policy to event
        retry_policy = retry_policy
        
        def process(self, *args, **kwargs) -> typing.Tuple[bool, typing.Any]:
            # Event processing logic
            pass

Alternative dictionary configuration:

.. code-block:: python

    class MyEvent(EventBase):
        retry_policy = {
            "max_attempts": 5,
            "backoff_factor": 0.1,
            "max_backoff": 5.0,
            "retry_on_exceptions": [ConnectionError, TimeoutError]
        }

The retry policy determines:
- Maximum number of retry attempts
- Backoff time between retries
- Maximum allowed backoff time
- Which exceptions trigger retries