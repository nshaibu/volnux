Tutorials - Batch Processing
=====================================

The **Pipeline Batch Processing** feature allows for parallel processing of multiple batches of data, improving performance
and efficiency when working with large datasets or time-sensitive operations. This is accomplished using a *pipeline template*
that defines the structure of the pipeline, and the `BatchPipeline` class, which handles the parallel execution of pipeline instances.

-------------------------------
1. Creating a Pipeline Template
-------------------------------

The first step is to define a pipeline class that inherits from the `Pipeline` class. This class will act as a *template*,
laying out the structure, input fields, and logic for your data processing pipeline.

This template serves as the blueprint for executing multiple customized pipeline instances depending on the incoming data.

**Example:**

.. code-block:: python

    from nexus import Pipeline
    from nexus.fields import InputDataField, FileInputDataField

    class Simple(Pipeline):
        name = InputDataField(data_type=list, batch_size=5)
        book = FileInputDataField(required=True, chunk_size=1024)

**Explanation:**

- ``Simple`` is a subclass of ``Pipeline`` and serves as the pipeline template.
- ``name`` is an ``InputDataField`` that accepts a list and processes data in batches of 5.
- ``book`` is a ``FileInputDataField`` that requires file input and processes data in 1024-byte chunks.

--------------------------------------
2. Creating the Batch Processing Class
--------------------------------------

Next, define the batch processing class by inheriting from the `BatchPipeline` class. This class is responsible for managing
parallel execution using the pipeline template you just created.

**Example:**

.. code-block:: python

    from nexus.pipeline import BatchPipeline
    from nexus.signal import SoftSignal

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple
        listen_to_signals = [SoftSignal('task_completed'), SoftSignal('task_failed')]

**Explanation:**

- ``SimpleBatch`` inherits from ``BatchPipeline``.
- The ``pipeline_template`` attribute is set to the ``Simple`` pipeline.
- ``listen_to_signals`` is used to define events that the batch pipeline reacts to (e.g., task completion or failure).

---------------------------------------
3. Using Custom Batch Processor Methods
---------------------------------------

You can define **custom batch processor methods** to control how specific fields are batched or chunked during execution.

**Requirements:**

- The method **must be a generator**.
- The method must match the following type signature:

.. code-block:: python

    typing.Callable[
        [
            typing.Union[typing.Collection, typing.Any],
            typing.Optional[typing.Union[int, float]],
        ],
        typing.Union[typing.Iterator[typing.Any], typing.Generator],
    ]

- The method **name must follow this naming scheme**: ``{field_name}_batch``.
  - Replace `{field_name}` with the name of the pipeline field you want the processor to apply to.

**Example:**

.. code-block:: python

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple
        listen_to_signals = [SoftSignal('task_completed'), SoftSignal('task_failed')]

        def name_batch(self, data, batch_size):
            # Custom batching for the 'name' field
            for i in range(0, len(data), batch_size or 1):
                yield data[i:i + batch_size]

        def book_batch(self, file_stream, chunk_size):
            # Custom chunking for the 'book' field
            while True:
                chunk = file_stream.read(int(chunk_size or 1024))
                if not chunk:
                    break
                yield chunk

**Explanation:**

- ``name_batch`` handles custom batching for the ``name`` field.
- ``book_batch`` handles custom chunking of a file stream for the ``book`` field.
- Both methods are generators and follow the required annotation.

---------------------------------------
4. Defining the Data Set for Processing
---------------------------------------

Prepare the dataset that you wish to process. This dataset will be split into batches or chunks automatically,
based on the default or custom batch processor logic.

For example, if ``batch_size=5`` is defined in the template, the data will be processed in chunks of 5 items
unless overridden by a custom method.

-----------------------------------------------
5. Configuring and Executing the Batch Pipeline
-----------------------------------------------

Once the batch pipeline class is defined, you can instantiate and execute it to process data in parallel.

**Trigger Execution:**

.. code-block:: python

    simple_batch = SimpleBatch(name=["item1", "item2", ...], book="/path/to/book.csv")
    simple_batch.execute()

--------------------------------------------------
6. Monitoring and Optimizing Execution
--------------------------------------------------

Monitoring and optimization are essential for scaling.

**Monitoring:**

- Integrate with **OpenTelemetry** to gather telemetry data like execution time, throughput, and error rates.
- Leverage **SoftSignal** events (e.g., ``task_completed``, ``task_failed``) to monitor status and handle failures.

**Optimization Tips:**

- Adjust the ``max number of tasks per child`` configuration to balance performance and system resource usage.
- Tune batch sizes, concurrency, and system parameters to optimize throughput and minimize bottlenecks.

----------------------------
7. Full Working Example
----------------------------

.. code-block:: python

    from nexus import Pipeline
    from nexus.fields import InputDataField, FileInputDataField
    from nexus.pipeline import BatchPipeline
    from nexus.signal import SoftSignal

    class Simple(Pipeline):
        name = InputDataField(data_type=list, batch_size=5)
        book = FileInputDataField(required=True, chunk_size=1024)

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple
        listen_to_signals = [SoftSignal('task_completed'), SoftSignal('task_failed')]

        def name_batch(self, data, batch_size):
            for i in range(0, len(data), batch_size or 1):
                yield data[i:i + batch_size]

        def book_batch(self, file_stream, chunk_size):
            while True:
                chunk = file_stream.read(int(chunk_size or 1024))
                if not chunk:
                    break
                yield chunk

    # Instantiate and execute the batch processor
    simple_batch = SimpleBatch(name=["item1", "item2", ...], book="/path/to/book.csv")
    simple_batch.execute()

**Explanation:**

- The `Simple` pipeline defines `name` and `book` as inputs.
- `SimpleBatch` defines custom processing logic for both fields via generator methods.
- `simple_batch.execute()` runs the pipeline and processes both inputs in parallel.

-----------------------------
Conclusion
-----------------------------

The Pipeline Batch Processing system allows for powerful, scalable batch execution using templates and the `BatchPipeline` engine.
With support for custom batch processors and signal-driven execution, it is both flexible and robust for a variety of data workflows.
