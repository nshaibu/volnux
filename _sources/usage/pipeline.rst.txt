Pipeline
========

This section covers everything you need to know about creating and working with pipelines in the Nexus framework.

Defining Pipeline
---------------

To define a pipeline, you need to inherit from the Pipeline class and create your custom implementation:

.. code-block:: python

    from nexus import Pipeline

    class MyPipeline(Pipeline):
        # Your input data fields will go here
        pass

Defining Input Data Field
------------------------

Input fields are defined using the InputDataField class or its variants. These fields represent the data that will flow through your pipeline:

.. code-block:: python

    from nexus import Pipeline
    from nexus.fields import InputDataField

    class MyPipeline(Pipeline):
        # Define input fields as attributes
        input_field = InputDataField(data_type=str, required=True)

Defining Pipeline Structure
-------------------------

Pipeline structure is defined using the Pointy language, which provides a structured format to describe task execution flow.

Basic Pointy File
~~~~~~~~~~~~~~~

Create a file with the `.pty` extension matching your pipeline class name (e.g., `MyPipeline.pty`):

.. code-block:: text

    Fetch->Process->Execute->SaveToDB->Return

Alternative Configuration
~~~~~~~~~~~~~~~~~~~~~~

You can also define the pipeline structure within your class using a Meta subclass:

.. code-block:: python

    class MyPipeline(Pipeline):
        class Meta:
            pointy = "A->B->C"  # Pointy script
            # OR
            file = "/path/to/your/custom_pipeline.pty"

        # Or using dictionary format
        meta = {
            "pointy": "A->B->C",
            # OR
            "file": "/path/to/your/custom_pipeline.pty"
        }

Pointy Language Syntax
--------------------

Operators
~~~~~~~~

- **Directional Operator (->)**:
    Defines sequential flow:
    
    .. code-block:: text

        A -> B   # Execute event A, then B

- **Parallel Operator (||)**:
    Executes events concurrently:
    
    .. code-block:: text

        A || B   # Execute A and B in parallel

- **Pipe Result Operator (|->)**:
    Pipes results between events:
    
    .. code-block:: text

        A |-> B  # Pipe result of A into B

- **Conditional Branching**:
    Define execution paths based on event outcomes:
    
    .. code-block:: text

        A -> B (0 -> C, 1 -> D)  # If B fails (0) execute C, if succeeds (1) execute D

- **Retry Operator (*)**:
    Specifies retry attempts for events:
    
    .. code-block:: text

        A * 3  # Retry event A up to 3 times

Executing Pipeline
---------------

To execute your pipeline:

.. code-block:: python

    # Instantiate your pipeline class
    pipeline = MyPipeline(input_field="value")

    # Execute the pipeline
    pipeline.start()

Visualizing Pipeline
-----------------

You can visualize your pipeline structure using ASCII or graphviz:

.. code-block:: python

    # ASCII representation
    pipeline.draw_ascii_graph()

    # Graphical representation (requires graphviz)
    pipeline.draw_graphviz_image(directory="path/to/output")

Batch Processing
--------------

For processing multiple batches of data in parallel:

Creating Pipeline Template
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from nexus import Pipeline
    from nexus.fields import InputDataField, FileInputDataField

    class Simple(Pipeline):
        name = InputDataField(data_type=list, batch_size=5)
        book = FileInputDataField(required=True, chunk_size=1024)

Creating Batch Processing Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from nexus.pipeline import BatchPipeline
    from nexus.signal import SoftSignal

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple
        listen_to_signals = [SoftSignal('task_completed'), SoftSignal('task_failed')]

The batch processing system will automatically handle parallel execution of pipeline instances based on your configuration.