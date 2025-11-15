API Reference - Fields and Signals
===========================

Field Classes
-------------

InputDataField
~~~~~~~~~~~~~~
.. py:class:: InputDataField

   Base field class for defining pipeline input data.

   **Parameters:**

   :param name: Field name
   :type name: Optional[str]
   :param required: Whether field is required
   :type required: bool
   :param data_type: Expected data type(s)
   :type data_type: Union[Type, Tuple[Type]]
   :param default: Default value
   :type default: Any
   :param batch_processor: Function for batch processing
   :type batch_processor: Optional[Callable]
   :param batch_size: Size of batches for processing
   :type batch_size: int

   **Properties:**

   .. py:property:: has_batch_operation
      :type: bool

      Whether field has batch processing configured.

FileInputDataField
~~~~~~~~~~~~~~~~
.. py:class:: FileInputDataField

   Field class for handling file inputs in pipelines.

   **Parameters:**

   :param path: File path
   :type path: Union[str, PathLike]
   :param required: Whether file is required
   :type required: bool
   :param chunk_size: Size of chunks when reading file
   :type chunk_size: int
   :param mode: File open mode
   :type mode: str
   :param encoding: File encoding
   :type encoding: Optional[str]

   **Methods:**

   Inherits all methods from InputDataField and adds file handling capabilities.

Signal Classes
--------------

SoftSignal
~~~~~~~~~~
.. py:class:: SoftSignal

   Implementation of the soft signaling system.

   **Parameters:**

   :param name: Signal name
   :type name: str
   :param provide_args: Arguments provided by signal
   :type provide_args: Optional[List[str]]

   **Methods:**

   .. py:method:: connect(sender: Any, listener: Callable) -> None

      Connects a listener function to the signal.

      :param sender: Object sending the signal
      :param listener: Callback function

   .. py:method:: disconnect(sender: Any, listener: Callable) -> None

      Disconnects a listener from the signal.

      :param sender: Object sending the signal
      :param listener: Callback function to disconnect

   .. py:method:: emit(sender: Any, **kwargs) -> List[Tuple[Any, Any]]

      Emits the signal to all connected listeners.

      :param sender: Object sending the signal
      :param kwargs: Additional arguments for listeners
      :return: List of (receiver, response) tuples

Built-in Signals
----------------

Pipeline Lifecycle
~~~~~~~~~~~~~~~~~~

.. py:data:: pipeline_pre_init
   :type: SoftSignal

   Emitted before pipeline initialization.
   Arguments: cls, args, kwargs

.. py:data:: pipeline_post_init
   :type: SoftSignal

   Emitted after pipeline initialization.
   Arguments: pipeline

.. py:data:: pipeline_execution_start
   :type: SoftSignal 

   Emitted when pipeline execution starts.
   Arguments: pipeline

.. py:data:: pipeline_execution_end
   :type: SoftSignal

   Emitted when pipeline execution ends.
   Arguments: execution_context

Event Lifecycle
~~~~~~~~~~~~~~~

.. py:data:: event_init
   :type: SoftSignal

   Emitted when an event is initialized.
   Arguments: event, init_kwargs

.. py:data:: event_execution_init
   :type: SoftSignal

   Emitted before event execution.
   Arguments: event, execution_context, executor, call_kwargs

.. py:data:: event_execution_start
   :type: SoftSignal 

   Emitted when event execution starts.
   Arguments: event, execution_context

.. py:data:: event_execution_end
   :type: SoftSignal

   Emitted when event execution completes.
   Arguments: event, execution_context, future