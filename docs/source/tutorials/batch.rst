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

    from volnux import Pipeline
    from volnux.execution.pipeline.fields import InputDataField, FileInputDataField

    class Simple(Pipeline):
        name = InputDataField(data_type=list, batch_size=5)
        book = FileInputDataField(required=True, chunk_size=1024)

**Explanation:**

- ``Simple`` is a subclass of ``Pipeline`` and serves as the pipeline template.
- ``name`` is an ``InputDataField`` that accepts a list and processes data in batches of 5.
- ``book`` is a ``FileInputDataField`` that requires file input and is streamed in chunks of 1024 characters (bytes in binary mode).

--------------------------------------
2. Creating the Batch Processing Class
--------------------------------------

Next, define the batch processing class by inheriting from ``BatchPipeline``. It takes the same fields as the
template, splits the batched ones into chunks, and runs one pipeline instance per chunk in a process pool.

**Example:**

.. code-block:: python

    from volnux import BatchPipeline

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple
        max_workers = 4          # process pool size (default: MAX_BATCH_PROCESSING_WORKERS)
        max_in_flight = 8        # pipelines running or queued at once (default: 2 * max_workers)

**Explanation:**

- ``pipeline_template`` is the ``Pipeline`` subclass to run for each chunk.
- Pipelines are created lazily, only as in-flight slots free up, so memory stays flat however large the input is.
- ``listen_to_signals`` (optional) lists the import strings of the signals to forward from the worker processes
  to the signal handlers registered in your process. By default every registered signal is forwarded.

---------------------------------------
3. Using Custom Batch Processor Methods
---------------------------------------

A **custom batch processor method** replaces how one field is split into chunks.

**Requirements:**

- The method is named ``{field_name}_batch``.
- It takes ``(self, values, batch_size)`` - the parameters must be named ``values`` and ``batch_size``
  (``chunk_size`` is also accepted in place of ``batch_size``).
- It returns an iterable of chunks (usually it is a generator).

**Example:**

.. code-block:: python

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple

        def name_batch(self, values, batch_size):
            # Custom batching for the 'name' field
            for i in range(0, len(values), batch_size or 1):
                yield values[i:i + batch_size]

**Notes:**

- ``values`` is what you passed for the field (not the default chunks).
- A chunk is converted to the field's type: a tuple chunk becomes a ``list`` for a ``list`` or ``List[str]`` field.
- A ``FileInputDataField`` is streamed automatically: each pipeline receives a ``FileChunk`` with ``data``,
  ``source`` (the file path) and ``index``. Define ``book_batch`` only to chunk the file differently (it receives the path).
- When several fields are batched they advance together, one chunk of each per pipeline.

---------------------------------------
4. Defining the Data Set for Processing
---------------------------------------

Pass the data when you create the batch. Batched fields are split by the field's ``batch_size`` (or ``chunk_size``)
unless overridden by a custom method; the remaining fields are shared by every pipeline.

-----------------------------------------------
5. Configuring and Executing the Batch Pipeline
-----------------------------------------------

``execute`` is a coroutine. Pass a callback that is called for every pipeline as it finishes - it may be a plain
function or a coroutine function. It receives a result with the finished ``pipeline`` and an ``exception``
(``None`` on success).

.. code-block:: python

    async def on_result(result):
        if result.exception is None:
            print("done:", result.pipeline.name)
        else:
            print("failed:", result.exception)

    summary = await SimpleBatch(name=["item1", "item2"], book="/path/to/book.csv").execute(on_result)
    # {"completed": 2, "failed": 0, "total": 2}

An exception raised by the callback is logged and does not stop the batch. A chunk that cannot be turned into a
pipeline (for example a value of the wrong type) is reported as a failed result and the remaining chunks still run.
Cancelling the task running ``execute`` stops the batch and cleans up the worker processes.

--------------------------------------------------
6. Monitoring and Optimizing Execution
--------------------------------------------------

**Monitoring:**

- Signals fired inside the workers are re-emitted in your process, so handlers registered there receive them.
- ``batch_pipeline_started`` and ``batch_pipeline_finished`` report the whole batch, with metrics (pipelines
  started, completed, failed, durations and success rate).
- Integrate with **OpenTelemetry** for execution time, throughput and error rates.

**Optimization Tips:**

- ``max_workers`` and ``max_in_flight`` bound the parallelism and the memory held by queued pipelines.
- When this process goes above ``max_memory_percent`` (default 90) the in-flight window shrinks by 20%, down to 1.
- Tune ``batch_size`` / ``chunk_size``: larger chunks mean fewer, bigger pipelines.

----------------------------
7. Full Working Example
----------------------------

.. code-block:: python

    import asyncio

    from volnux import BatchPipeline, Pipeline
    from volnux.execution.pipeline.fields import InputDataField

    class Simple(Pipeline):
        name = InputDataField(data_type=list, batch_size=5)

        class Meta:
            pointy = "Greet -> Save"

    class SimpleBatch(BatchPipeline):
        pipeline_template = Simple
        max_workers = 4

    async def main():
        results = []
        summary = await SimpleBatch(name=[f"item{i}" for i in range(100)]).execute(results.append)
        print(summary)  # {"completed": 20, "failed": 0, "total": 20}

    if __name__ == "__main__":
        asyncio.run(main())

**Explanation:**

- ``Simple`` defines ``name`` as a batched input and the workflow to run for each chunk.
- 100 names with ``batch_size=5`` become 20 pipelines, streamed through the process pool.
- ``execute`` returns when every pipeline has finished.

-----------------------------
Conclusion
-----------------------------

The Pipeline Batch Processing system allows for scalable batch execution using templates and the ``BatchPipeline`` engine.
With lazily created pipelines, bounded in-flight work and custom batch processors, it stays flat in memory and flexible
across a variety of data workflows.
