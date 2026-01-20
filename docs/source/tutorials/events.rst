Tutorials - Events
===========================

Introduction
------------

This documentation provides a comprehensive guide to defining and configuring events in the event pipeline system.
Events are the building blocks of the event pipeline system. They represent individual units of work that take in input data, 
process the data, perform operations, return an output, and determine the flow of execution. 
Think of events as steps in a recipe - each one performs a specific task, and together they create a complete processing pipeline.

Key Concepts:

- Events encapsulate processing logic

- They can succeed or fail, affecting pipeline flow

- Multiple events can run concurrently using executors

- Events support retry mechanisms for fault tolerance


Events are flexible:

- They can be defined as classes or functions.

- They can run concurrently using executors.

- They can be configured to retry on failure, switch tasks, or even skip execution under certain conditions.

This guide will show you how to define, configure, and use events step by step.


Quick Start
-----------

Let's create a simple event that processes user data:

.. code-block:: python

    from nexus import EventBase

    class UserRegistrationEvent(EventBase):
        def process(self, user_data, *args, **kwargs):
            # Simple validation and processing
            if not user_data.get('email'):
                return False, "Email is required"
            
            # Process the user registration
            user_id = self._create_user(user_data)
            return True, {"user_id": user_id, "status": "registered"}
        
        def _create_user(self, user_data):
            # Your user creation logic here
            return "u983"  # Return generated user ID
..


Defining events
---------------

**Class Based Events**

To define an event, you need to inherit from the ``EventBase`` class and override the ``process`` method. This method
contains the logic that will be executed when the event is processed.

.. code-block:: python

    from nexus import EventBase

    class MyCustomEvent(EventBase):
        def process(self, input_data, *args, **kwargs):
            """
            Process input data and return success status with result
            
            Args:
                input_data: Data to process
                *args, **kwargs: Additional parameters
                
            Returns:
                Tuple[bool, Any]: (success_status, result_message_or_data)
            """
            try:
                # Your processing logic here
                processed_data = self._transform_data(input_data)
                return True, processed_data
            except Exception as e:
                return False, f"Processing failed: {str(e)}"
..

The ``process`` method should return a tuple containing:

1. A boolean indicating success (``True``) or failure (``False``)
2. A result value or message, which can be a string message or dict, list, etc.


**Function Based Events**

In addition to defining events using classes, you can also define events as functions using the ``@event`` decorator from the ``decorators`` module.

.. code-block:: python

    from nexus.decorators import event

    # Define a function-based event using the @event decorator
    @event()
    def my_event(input_data, *args, **kwargs):
        """
            Process input data and return success status with result
            
            Args:
                input_data: Data to process
                *args, **kwargs: Additional parameters
                
            Returns:
                Tuple[bool, Any]: (success_status, result_message_or_data)
        """
        try:
            # Your processing logic here
            processed_data = self._transform_data(input_data)
            return True, processed_data
        except Exception as e:
            return False, f"Processing failed: {str(e)}"
..

Configuring Executors
---------------------

Executors control how events run - whether they execute sequentially, in threads, or in separate processes.
Every event must specify an ``executor`` that defines how the event will be executed. 
Executors manage the concurrency or parallelism when the event is being processed.
Executors implement the ``Executor`` interface from the ``concurrent.futures._base`` module in the Python standard library. 
If no executor is specified, the ``DefaultExecutor`` will be used.

**Default Behavior**

If you don't specify an executor, events use the DefaultExecutor which runs tasks sequentially:

.. code-block:: python

    from nexus import EventBase

    class SimpleEvent(EventBase):
        # Uses DefaultExecutor (sequential execution)
        def process(self, data):
            return True, f"Processed: {data}"
..

**Using Thread Pool Executor**

For concurrent execution using threads:

.. code-block:: python

    from concurrent.futures import ThreadPoolExecutor
    from nexus import EventBase

    class ConcurrentEvent(EventBase):
        executor = ThreadPoolExecutor  # Run in thread pool
        
        def process(self, data):
            # This will run concurrently with other events
            return True, data
..


**Using Process Pool Executor**

For CPU-intensive tasks using separate processes:

.. code-block:: python

    from concurrent.futures import ProcessPoolExecutor
    from nexus import EventBase

    class CPUIntensiveEvent(EventBase):
        executor = ProcessPoolExecutor  # Run in separate processes
        
        def process(self, data):
            # CPU-intensive processing here
            return True, data
..


Simple Executor Configuration
-----------------------------

The ``ExecutorInitializerConfig`` class is used to configure the initialization of an executor that manages event processing. 
This class allows you to control several aspects of the executor's behavior.


**Example Configuration**

Here's an example of how to use the ``ExecutorInitializerConfig`` class:

.. code-block:: python

    from nexus import ExecutorInitializerConfig, EventBase
    from concurrent.futures import ThreadPoolExecutor

    # Configuring an executor with specific settings
    config = ExecutorInitializerConfig(
        max_workers=4,
        max_tasks_per_child=50,
        thread_name_prefix="event_executor_"
    )

    class ConfiguredEvent(EventBase):
        executor = ThreadPoolExecutor
        
        # Simple dictionary configuration
        executor_config = config
        
        def process(self, data):
            # Event processing logic here
            return True, data
..


Alternatively, you can provide configuration as a dictionary:

.. code-block:: python

    from nexus import EventBase
    from concurrent.futures import ThreadPoolExecutor

    class ConfiguredEvent(EventBase):
        executor = ThreadPoolExecutor

        # Configure the executor using a dictionary
        executor_config = {
            "max_workers": 4,
            "max_tasks_per_child": 50,
            "thread_name_prefix": "event_executor_"
        }

        def process(self, *args, **kwargs):
            # Event processing logic here
            return True, data
..


The ``@event`` decorator allows you to also configure the executor for the event's execution:

.. code-block:: python

    from nexus.decorators import event
    from concurrent.futures import ThreadPoolExecutor

    # Define a function-based event with configuration
    @event(
        executor=ThreadPoolExecutor,               # Define the executor to use
        executor_config={
            "max_workers": 4,                             # Specify max workers
            "max_tasks_per_child": 10,                    # Limit tasks per worker
            "thread_name_prefix": "my_event_executor",    # Prefix for thread names
        }
    )
    def my_event(*args, **kwargs):
        # Event processing logic here
        return True, data
..


**Configuration Fields**

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


Here's an example of how to use the ``ExecutorInitializerConfig`` class:

.. code-block:: python

    from nexus import ExecutorInitializerConfig, EventBase
    from concurrent.futures import ThreadPoolExecutor

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

    from nexus import EventBase
    from concurrent.futures import ThreadPoolExecutor

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

    from nexus import ExecutorInitializerConfig

    config = ExecutorInitializerConfig()  # Default configuration
..

Retry Policies
--------------

Retry policies help handle temporary failures by automatically retrying events.
Events may fail because of temporary errors (network issues, timeouts, etc.).
For handling events that may fail intermittently, you can define a retry policy. 
The retry policy allows you to configure settings like maximum retry attempts, backoff strategy, and which exceptions should trigger a retry.

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
    from concurrent.futures import ThreadPoolExecutor
    from requests.exceptions import ConnectionError

    # Define a function-based event with configuration
    @event(
        executor=ThreadPoolExecutor,
        executor_config={
            "max_workers": 4,
            "max_tasks_per_child": 10,
            "thread_name_prefix": "my_event_executor",
        },
        retry_policy={
            "max_attempts": 3,
            "backoff_factor": 0.5,
            "retry_on_exceptions": [ConnectionError]
        }
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

    from nexus import RetryPolicy
    import typing
    from dataclasses import dataclass

    @dataclass
    class RetryPolicy(object):
        max_attempts: int   # Maximum retry attempts
        backoff_factor: float  # Backoff time between retries
        max_backoff: float  # Maximum allowed backoff time
        retry_on_exceptions: typing.List[typing.Type[Exception]]  # Exceptions that trigger a retry
..


**Basic Retry Configuration**

You can create an instance of ``RetryPolicy`` or define it as a dictionary:

.. code-block:: python

    from nexus import EventBase
    from requests.exceptions import ConnectionError, Timeout

    class NetworkEvent(EventBase):
        # Retry on network errors
        retry_policy = {
            "max_attempts": 3,  # Try up to 3 times
            "backoff_factor": 1.0,  # Wait 1, 2, 4 seconds between retries
            "max_backoff": 10.0,  # Never wait more than 10 seconds
            "retry_on_exceptions": [ConnectionError, Timeout]
        }
        
        def process(self, url):
            response = requests.get(url)  # This might fail temporarily
            return True, response.json()
..

OR

.. code-block:: python

    from nexus import EventBase, RetryPolicy
    from requests.exceptions import ConnectionError, Timeout

    _retry_policy = RetryPolicy(
        max_attempts=3,  # Try up to 3 times
        backoff_factor=1.0,  # Wait 1, 2, 4 seconds between retries
        max_backoff=10.0,  # Never wait more than 10 seconds
        retry_on_exceptions=[ConnectionError, Timeout]
    )

    class NetworkEvent(EventBase):
        # Retry on network errors
        retry_policy = _retry_policy
        
        def process(self, url):
            response = requests.get(url)  # This might fail temporarily
            return True, response.json()
..


**Understanding Retry Parameters**

The configuration parameters are:

- ``max_attempts``: The maximum number of times the event will be retried (initial + retries).
- ``backoff_factor``: How long the system will wait between retry attempts, increasing with each retry.
- ``max_backoff``: The maximum time to wait between retries.
- ``retry_on_exceptions``: A list of exception types that should trigger a retry.


**Another Practical Retry Example**

.. code-block:: python

    from nexus import EventBase
    
    class DatabaseEvent(EventBase):
        retry_policy = {
            "max_attempts": 5,
            "backoff_factor": 0.5,  # Wait 0.5, 1, 2, 4, 8 seconds
            "retry_on_exceptions": [DatabaseConnectionError]
        }
        
        def process(self, query):
            # Database operation that might fail temporarily
            result = database.execute(query)
            return True, result
..


**How the Retry Policy Works**

When an event is processed, if it fails due to an exception in the ``retry_on_exceptions`` list:

1. The system will retry the event based on the ``max_attempts``.
2. After each retry attempt, the system waits for a time interval determined by the ``backoff_factor`` and will not exceed the ``max_backoff``.
3. If the maximum retry attempts are exceeded, the event will be marked as failed.

This retry mechanism ensures that intermittent failures do not cause a complete halt in processing and allows for better fault tolerance in your system.


Advanced Features
------------------

**Event Result Evaluation**

The ``EventExecutionEvaluationState`` class defines the criteria for evaluating the success or failure of an event based on the outcomes of its tasks.

**Available States**

- ``SUCCESS_ON_ALL_EVENTS_SUCCESS``: The event is considered successful only if all tasks succeeded. This is the **default** state.
- ``FAILURE_FOR_PARTIAL_ERROR``: The event is considered a failure if any task fails.
- ``SUCCESS_FOR_PARTIAL_SUCCESS``: The event is considered successful if at least one task succeeds.
- ``FAILURE_FOR_ALL_EVENTS_FAILURE``: The event is considered a failure only if all tasks fail.

**Example Usage**

.. code-block:: python

    from nexus import EventBase, EventExecutionEvaluationState

    class StrictEvent(EventBase):
        # Only succeed if ALL tasks succeed (default)
        execution_evaluation_state = EventExecutionEvaluationState.SUCCESS_ON_ALL_EVENTS_SUCCESS

        def process(self, *args, **kwargs):
            return True, "obrafour"

    class LenientEvent(EventBase):
        # Succeed if ANY task succeeds
        execution_evaluation_state = EventExecutionEvaluationState.SUCCESS_FOR_PARTIAL_SUCCESS

        def process(self, *args, **kwargs):
            return True, "obrafour"
..

**Stopping Conditions**

Events can control whether the pipeline should stop early after their execution.
This is useful for cases where certain outcomes (such as success, error, or exception) should halt further event processing.
The stop condition for an event is defined using the `stop_on_exception`, `stop_on_success`, and `stop_on_error` boolean flags in the `EventBase` constructor.

**Available stop conditions:**

- `stop_on_exception`: Stop if an exception occurs.
- `stop_on_success`: Stop if the event succeeds.
- `stop_on_error`: Stop if the event fails.

**Example Usage**

.. code-block:: python

    from nexus import EventBase

    class MyCustomEvent(EventBase):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, stop_on_error=True, **kwargs)

        def process(self, *args, **kwargs):
            if kwargs.get("fail", False):
                return False, "Something went wrong"
            return True, "Event processed successfully"
..

In this example:

- If `MyCustomEvent` fails (process returns `False`), the pipeline will stop immediately.
- If it succeeds, the pipeline continues to the next event.


**Task Transitions (Goto)**

Jump to different tasks based on results. The `goto` method allows you to dynamically change the flow of your pipeline by jumping to a different task. This is useful for implementing conditional logic in your pipeline.

**Example Usage**

.. code-block:: python

    from nexus import EventBase

    class ConditionalEvent(EventBase):
        def process(self, data, *args, **kwargs):
            if data.get('type') == 'premium':
                # Jump to premium processing task (task ID 101)
                self.goto(
                    descriptor=101,                 # The identifier of the next task to switch to.
                    result_status=True,             # Indicates if the current task succeeded or failed.
                    result=data,                    # The result data to pass to the next task.
                    reason="Premium user detected"  # Reason for the task switch. Defaults to "manual".
                )
            
            # Normal processing for regular users
            return True, self._process_regular_user(data)
..


**Event Bypassing**

Sometimes you may want to skip certain events based on specific conditions without affecting the overall pipeline flow. 
The ``can_bypass_current_event`` method allows you to define custom rules for bypassing events when appropriate.

When an event is about to execute, the system first checks if it can be bypassed. 
If bypass conditions are met, the event is skipped entirely, and processing continues with the next event in the pipeline.

**Example Usage**

.. code-block:: python

    from nexus import EventBase

    class UserValidationEvent(EventBase):
        def can_bypass_current_event(self):
            """
            Determine if this validation event should be skipped
            
            Returns:
                Tuple[bool, Any]: (should_skip, data_to_pass_forward)
            """
            # Skip validation for admin users
            if self._execution_context.user_role == 'admin':
                return True, {"bypass_reason": "Admin users skip validation"}
            
            # Skip validation during system maintenance
            if self._execution_context.is_maintenance_mode:
                return True, {"bypass_reason": "Maintenance mode active"}
                
            # Normal execution for all other cases
            return False, None
        
        def process(self, user_data):
            # This only runs if can_bypass_current_event returns False
            return self._validate_user(user_data)
..


**Returned Values Explained**

The method returns a tuple with two elements:

1. Bypass Decision (bool):
    - True: Skip this event entirely
    - False: Execute the event normally

2. Result Data (Any):
    - Data to pass to the next event when bypassing
    - Can be None if no specific data needs to be passed


Examples of real world application
-----------------------------------

**E-commerce Order Processing**

.. code-block:: python

    from nexus import EventBase
    from concurrent.futures import ThreadPoolExecutor

    class ValidateOrderEvent(EventBase):
        def process(self, order_data):
            if not order_data.get('items'):
                return False, "Order has no items"
            return True, order_data

    class ProcessPaymentEvent(EventBase):
        executor = ThreadPoolExecutor
        retry_policy = {
            "max_attempts": 3,
            "retry_on_exceptions": [PaymentGatewayError]
        }
        
        def process(self, order_data):
            payment_result = payment_gateway.charge(order_data['total'])
            return True, payment_result

    class SendConfirmationEvent(EventBase):
        def process(self, order_data, payment_result):
            email_service.send_confirmation(
                order_data['email'],
                order_data['order_id']
            )
            return True, "Confirmation sent"
..


**Data Processing Pipeline**

.. code-block:: python

    from nexus import EventBase
    from concurrent.futures import ProcessPoolExecutor

    class DataExtractionEvent(EventBase):
        def process(self, source):
            data = extract_from_source(source)
            return True, data

    class DataTransformationEvent(EventBase):
        executor = ProcessPoolExecutor  # CPU-intensive
        
        def process(self, raw_data):
            transformed = transform_data(raw_data)
            return True, transformed

    class DataLoadEvent(EventBase):
        retry_policy = {
            "max_attempts": 5,
            "retry_on_exceptions": [DatabaseError]
        }
        
        def process(self, transformed_data):
            load_to_database(transformed_data)
            return True, "Load successful"
..


FAQs
----

Q: Why is my event not executing?
A: Check that:
    - You've implemented the process method
    - The event is properly registered in your pipeline
    - You're passing the required parameters


Q: How do I handle exceptions in events?
A: Either:
    - Catch exceptions and return False with error message
    -Let the exception bubble up for the retry mechanism

.. code-block:: python

    def process(self, data):
        try:
            result = risky_operation(data)
            return True, result
        except SpecificError as e:
            return False, f"Operation failed: {str(e)}"
..


Q: When should I use different executors?
A:
    - DefaultExecutor: Simple sequential execution
    - ThreadPoolExecutor: I/O-bound operations (HTTP requests, file I/O)
    - ProcessPoolExecutor: CPU-intensive computations


Best Practices
--------------

**Keep Events Focused**

.. code-block:: python

    from nexus import EventBase

    # Good: Single responsibility
    class ValidateEmailEvent(EventBase):
        def process(self, email):
            return is_valid_email(email), email

    # Avoid: Doing too much
    class ProcessUserEvent(EventBase):
        def process(self, user_data):
            # Validation goes here
            # processing goes here
            # notification goes here

            # too many responsibilities in this process method alone, 
            # if possible break them into different events on their own
            pass
..


**Use Meaningful Error Messages**

.. code-block:: python

    # Good: return meaningful and specific error messages
    def process(self, data):
        if not data.get('required_field'):
            return False, "Missing required_field parameter"  # Specific
    
    # Avoid: using vague and generic error messages
    def process(self, data):
        if not data.get('required_field'):
            return False, "Error occurred"  # Vague
..


Troubleshooting Common Issues
-----------------------------

**Event Not Being Called**

Check your pipeline configuration and ensure the event is properly registered.

**Retries Not Working**

Verify that:
    - The exception type is in retry_on_exceptions
    - max_attempts is greater than 1
    - The exception is actually being raised

**Performance Issues**

- Use ProcessPoolExecutor for CPU-bound tasks
- Use ThreadPoolExecutor for I/O-bound tasks
- Monitor executor worker counts