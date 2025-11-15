API Reference - Pipeline
====================

Core Pipeline Classes
------------------

Pipeline 
~~~~~~~
.. py:class:: Pipeline

   Base class for defining pipelines that process events in sequence.

   **Class Attributes:**

   .. py:attribute:: __signature__
      :type: Optional[Signature]

      Cached signature for pipeline initialization.

   **Methods:**

   .. py:method:: start(force_rerun: bool = False)

      Starts pipeline execution.

      :param force_rerun: Whether to force rerun of already executed events
      :type force_rerun: bool

   .. py:method:: shutdown()

      Shuts down the pipeline and cleans up resources.

   .. py:method:: stop()

      Stops pipeline execution.

   .. py:method:: get_pipeline_tree() -> Tree

      Gets a tree representation of the pipeline structure.

      :return: Tree object representing pipeline structure
      :rtype: Tree

   .. py:method:: draw_ascii_graph()

      Generates ASCII representation of pipeline graph.

   .. py:method:: draw_graphviz_image(directory="pipeline-graphs")

      Generates GraphViz visualization of pipeline.

      :param directory: Output directory for graph images
      :type directory: str

BatchPipeline
~~~~~~~~~~~
.. py:class:: BatchPipeline

   Pipeline class for handling batch processing of events.

   **Class Attributes:**

   .. py:attribute:: pipeline_template
      :type: Type[Pipeline]

      Template pipeline class to use for batch processing.

   .. py:attribute:: listen_to_signals
      :type: List[str]

      List of signals to monitor during batch processing.

   **Methods:**

   .. py:method:: execute()

      Executes batch processing of pipelines.

   .. py:method:: get_pipeline_template()

      Gets the template pipeline class.

      :return: Pipeline class used as template
      :rtype: Type[Pipeline]

PipelineTask
~~~~~~~~~~
.. py:class:: PipelineTask

   Represents a task node in a pipeline execution graph.

   **Attributes:**

   .. py:attribute:: event
      :type: Union[Type[EventBase], str]

      Event class or name to execute.

   .. py:attribute:: parent_node
      :type: Optional[PipelineTask]

      Parent task in execution tree.

   .. py:attribute:: on_success_event
      :type: Optional[PipelineTask]

      Task to execute on success.

   .. py:attribute:: on_failure_event  
      :type: Optional[PipelineTask]

      Task to execute on failure.

   **Properties:**

   .. py:property:: is_conditional
      :type: bool

      Whether task has conditional branches.

   .. py:property:: is_parallel_execution_node
      :type: bool

      Whether task is part of parallel execution.

   **Methods:**

   .. py:method:: get_children() -> List[PipelineTask]

      Gets child task nodes.

   .. py:method:: get_root() -> PipelineTask  

      Gets root task in tree.

   .. py:method:: get_descriptor(descriptor: int) -> Optional[PipelineTask]

      Gets task associated with descriptor.

      :param descriptor: Descriptor number
      :type descriptor: int
      :return: Associated task if found
      :rtype: Optional[PipelineTask]

EventExecutionContext
~~~~~~~~~~~~~~~~~~
.. py:class:: EventExecutionContext

   Manages execution context for pipeline events.

   **Attributes:**

   .. py:attribute:: task_profiles
      :type: List[PipelineTask]

      Tasks being executed.

   .. py:attribute:: pipeline
      :type: Pipeline

      Pipeline instance being executed.

   .. py:attribute:: execution_result
      :type: ResultSet

      Results of task execution.

   .. py:attribute:: state
      :type: ExecutionState

      Current execution state.

   **Methods:**

   .. py:method:: dispatch() -> Optional[SwitchTask]

      Dispatches task execution.

      :return: Task switch request if needed
      :rtype: Optional[SwitchTask]

   .. py:method:: cancel()

      Cancels task execution.

   .. py:method:: execution_failed() -> bool

      Checks if execution failed.

      :return: True if execution failed
      :rtype: bool

   .. py:method:: execution_success() -> bool

      Checks if execution succeeded.
      
      :return: True if execution succeeded
      :rtype: bool