Signals
=======

This section covers the Soft Signaling Framework in the Event Pipeline library.

Overview
--------

The Signaling Framework enables you to connect custom behaviors to specific points in the lifecycle of a pipeline
and its events. This is achieved through the SoftSignal class and a system of signal listeners.

Default Signals
-------------

The framework provides several built-in signals for different stages of pipeline execution:

Initialization Signals
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from volnux.signal.signals import pipeline_pre_init, pipeline_post_init

    # Signal emitted before pipeline initialization
    # Arguments: cls, args, kwargs
    pipeline_pre_init

    # Signal emitted after pipeline initialization
    # Arguments: pipeline
    pipeline_post_init

Shutdown Signals
~~~~~~~~~~~~~

.. code-block:: python

    from volnux.signal.signals import pipeline_shutdown, pipeline_stop

    # Signal emitted during pipeline shutdown
    pipeline_shutdown

    # Signal emitted when pipeline is stopped
    pipeline_stop

Execution Signals
~~~~~~~~~~~~~~

.. code-block:: python

    from volnux.signal.signals import (
        pipeline_execution_start,
        pipeline_execution_end
    )

    # Signal emitted when pipeline execution begins
    # Arguments: pipeline
    pipeline_execution_start

    # Signal emitted when pipeline execution completes
    # Arguments: execution_context
    pipeline_execution_end

Event Execution Signals
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from volnux.signal.signals import (
        event_execution_init,
        event_execution_start,
        event_execution_end,
        event_execution_retry,
        event_execution_retry_done
    )

    # Signal emitted when event execution is initialized
    # Arguments: event, execution_context, executor, call_kwargs
    event_execution_init

    # Signal emitted when event execution starts
    # Arguments: event, execution_context
    event_execution_start

    # Signal emitted when event execution ends
    # Arguments: event, execution_context, future
    event_execution_end

    # Signal emitted when event execution is retried
    # Arguments: event, execution_context, task_id, backoff, retry_count, max_attempts
    event_execution_retry

    # Signal emitted when event retry is completed
    # Arguments: event, execution_context, task_id, max_attempts
    event_execution_retry_done

Connecting Signal Listeners
------------------------

There are two ways to connect listeners to signals:

Using the connect Method
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from volnux.signal.signals import pipeline_execution_start
    from volnux import Pipeline

    def my_listener(pipeline):
        print(f"Execution starting for pipeline: {pipeline}")

    # Connect listener to signal
    pipeline_execution_start.connect(my_listener, sender=Pipeline)

Using the @listener Decorator
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from volnux.decorators import listener
    from volnux.signal.signals import pipeline_pre_init
    from volnux import Pipeline

    @listener(pipeline_pre_init, sender=Pipeline)
    def my_listener(sender, signal, *args, **kwargs):
        print("Pipeline execution starting")

Best Practices
------------

Signal Handler Performance
~~~~~~~~~~~~~~~~~~~~~~~

Keep signal handlers lightweight and fast:

.. code-block:: python

    # Good - Fast operation
    @listener(pipeline_execution_start)
    def good_listener(pipeline):
        log.info("Pipeline started")

    # Bad - Slow operation that could block pipeline
    @listener(pipeline_execution_start)
    def bad_listener(pipeline):
        time.sleep(5)  # Blocking operation
        perform_heavy_computation()

Error Handling
~~~~~~~~~~~~

Always handle exceptions in signal listeners:

.. code-block:: python

    @listener(pipeline_execution_start)
    def safe_listener(pipeline):
        try:
            # Your signal handling logic
            pass
        except Exception as e:
            log.error(f"Error in signal handler: {e}")
            # Don't re-raise the exception to avoid affecting the pipeline