API Reference - Events
===================

Core Event Classes
----------------

EventBase
~~~~~~~~
.. py:class:: EventBase

   Abstract base class for events in the pipeline system. This class serves as a foundation for event-related tasks and 
   defines common properties for event execution.

   **Class Attributes:**

   .. py:attribute:: executor
      :type: Type[Executor]

      The executor type used to handle event execution. Defaults to DefaultExecutor.

   .. py:attribute:: executor_config
      :type: ExecutorInitializerConfig

      Configuration settings for the executor. Defaults to None.

   .. py:attribute:: execution_evaluation_state
      :type: EventExecutionEvaluationState 

      Defines how event success/failure is determined. Defaults to SUCCESS_ON_ALL_EVENTS_SUCCESS.

   **Methods:**

   .. py:method:: process(*args, **kwargs) -> Tuple[bool, Any]

      Abstract method that must be implemented by subclasses. Defines the core event processing logic.

      :return: A tuple containing (success_status, result_data)
      :rtype: Tuple[bool, Any]

   .. py:method:: on_success(execution_result) -> EventResult

      Handles successful event execution.

      :param execution_result: The result of the event execution
      :return: EventResult instance with success status
      :rtype: EventResult

   .. py:method:: on_failure(execution_result) -> EventResult 

      Handles failed event execution.

      :param execution_result: The execution error or failure data
      :return: EventResult instance with error status
      :rtype: EventResult

   .. py:method:: goto(descriptor: int, result_status: bool, result: Any, reason="manual", execute_on_event_method: bool = True) -> None

      Switches execution to a different task based on descriptor.

      :param descriptor: Task descriptor number to switch to
      :param result_status: Success/failure status
      :param result: Result data
      :param reason: Reason for switching
      :param execute_on_event_method: Whether to execute on_success/on_failure methods

RetryPolicy
~~~~~~~~~~
.. py:class:: RetryPolicy

   Configuration class for event retry behavior.

   **Attributes:**

   .. py:attribute:: max_attempts
      :type: int

      Maximum number of retry attempts. Defaults to MAX_RETRIES.

   .. py:attribute:: backoff_factor
      :type: float 

      Factor used to calculate delay between retries. Defaults to MAX_BACKOFF_FACTOR.

   .. py:attribute:: max_backoff
      :type: float

      Maximum delay between retries. Defaults to MAX_BACKOFF.

   .. py:attribute:: retry_on_exceptions
      :type: List[Type[Exception]]

      List of exception types that should trigger retries. Defaults to empty list.

ExecutorInitializerConfig
~~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: ExecutorInitializerConfig

   Configuration class for executor initialization.

   **Attributes:**

   .. py:attribute:: max_workers
      :type: Union[int, EMPTY]

      Maximum number of workers for parallel execution.

   .. py:attribute:: max_tasks_per_child
      :type: Union[int, EMPTY] 

      Maximum number of tasks per worker process.

   .. py:attribute:: thread_name_prefix
      :type: Union[str, EMPTY]

      Prefix for naming executor threads.

   .. py:attribute:: host
      :type: Union[str, EMPTY]

      Host address for remote executors.

   .. py:attribute:: port
      :type: Union[int, EMPTY]

      Port number for remote executors.

   .. py:attribute:: timeout
      :type: Union[int, EMPTY]

      Execution timeout in seconds. Defaults to 30.

   .. py:attribute:: use_encryption
      :type: bool

      Whether to use encryption for remote execution. Defaults to False.

EventExecutionEvaluationState
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. py:class:: EventExecutionEvaluationState

   Enum defining different strategies for evaluating event execution success/failure.

   **Members:**

   .. py:attribute:: SUCCESS_ON_ALL_EVENTS_SUCCESS

      Event is successful only if all tasks succeed. Any failure marks event as failed.

   .. py:attribute:: FAILURE_FOR_PARTIAL_ERROR

      Event fails if any task fails, regardless of other successes.

   .. py:attribute:: SUCCESS_FOR_PARTIAL_SUCCESS

      Event is successful if at least one task succeeds.

   .. py:attribute:: FAILURE_FOR_ALL_EVENTS_FAILURE 

      Event fails only if all tasks fail. Any success marks event as successful.

   **Methods:**

   .. py:method:: context_evaluation(result: ResultSet, errors: List[Exception], context: EvaluationContext = EvaluationContext.SUCCESS) -> bool

      Evaluates execution results based on state and context.

      :param result: Set of execution results
      :param errors: List of execution errors
      :param context: Evaluation context (success/failure)
      :return: True if evaluation matches state's criteria
      :rtype: bool