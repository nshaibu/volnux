Scheduling
==========

This section covers how to schedule pipeline execution in the volnux framework.

Overview
--------

The Pipeline Scheduler component allows you to manage and schedule pipeline jobs for execution at specified times or intervals.
It provides a reliable way to automate pipeline execution based on time-based triggers.

CRON-based Scheduling
------------------

The framework supports CRON-style scheduling for periodic execution of pipelines:

.. code-block:: python

    from volnux import Pipeline
    from volnux.scheduler import PipelineScheduler
    from datetime import datetime

    class MyPipeline(Pipeline):
        pass

    # Create scheduler instance
    scheduler = PipelineScheduler()

    # Schedule pipeline to run every day at midnight
    scheduler.add_job(MyPipeline(), 
                     trigger='cron', 
                     hour=0, 
                     minute=0)

    # Start the scheduler
    scheduler.start()

Interval-based Scheduling
----------------------

For regular interval-based execution:

.. code-block:: python

    # Run pipeline every 30 minutes
    scheduler.add_job(MyPipeline(),
                     trigger='interval',
                     minutes=30)

One-time Scheduling
----------------

To schedule a pipeline to run once at a specific time:

.. code-block:: python

    # Run pipeline at a specific datetime
    scheduler.add_job(MyPipeline(),
                     trigger='date',
                     run_date=datetime(2025, 12, 31, 23, 59, 59))

Advanced Configuration
-------------------

Customizing Job Execution
~~~~~~~~~~~~~~~~~~~~~~~

You can customize various aspects of scheduled jobs:

.. code-block:: python

    scheduler.add_job(
        MyPipeline(),
        trigger='cron',
        hour=0,
        minute=0,
        max_instances=3,  # Maximum concurrent instances
        coalesce=True,    # Combine missed executions
        misfire_grace_time=3600  # Grace period for missed jobs
    )

Job Management
~~~~~~~~~~~~

Managing scheduled jobs:

.. code-block:: python

    # Add job with an ID
    job = scheduler.add_job(MyPipeline(), 
                           trigger='interval',
                           minutes=5,
                           id='my_pipeline_job')

    # Pause a job
    scheduler.pause_job('my_pipeline_job')

    # Resume a job
    scheduler.resume_job('my_pipeline_job')

    # Remove a job
    scheduler.remove_job('my_pipeline_job')

Error Handling
~~~~~~~~~~~~

Configure error handling for scheduled jobs:

.. code-block:: python

    def error_handler(event):
        print(f"Job failed: {event.job_id}")
        print(f"Exception: {event.exception}")

    scheduler.add_listener(error_handler, 
                         EVENT_JOB_ERROR | EVENT_JOB_MISSED)

Shutdown
-------

Properly shutting down the scheduler:

.. code-block:: python

    try:
        # Your application code
        pass
    finally:
        scheduler.shutdown()