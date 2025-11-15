API Reference - Scheduling
=====================

Scheduling Components
------------------

ScheduleMixin
~~~~~~~~~~~
.. py:class:: ScheduleMixin

   Mixin class that adds scheduling capabilities to pipelines.

   **Inner Classes:**

   .. py:class:: ScheduleTrigger
      
      Enum defining supported schedule trigger types.

      .. py:attribute:: DATE
         
         One-time execution at a specific date/time

      .. py:attribute:: INTERVAL
         
         Recurring execution at fixed intervals

      .. py:attribute:: CRON
         
         Cron-style scheduled execution

   **Methods:**

   .. py:method:: schedule_job(trigger: ScheduleTrigger, **scheduler_kwargs) -> Job

      Schedules pipeline execution according to the specified trigger.

      :param trigger: Type of schedule trigger to use
      :param scheduler_kwargs: Arguments for trigger configuration
      :return: Scheduled job instance
      :rtype: Job

      **Trigger Configuration Options:**

      DATE trigger kwargs:
         - run_date (datetime|str): The date/time to run the job at
         - timezone (datetime.tzinfo|str): Time zone for run_date

      INTERVAL trigger kwargs:
         - weeks (int): Number of weeks between runs
         - days (int): Number of days between runs
         - hours (int): Number of hours between runs
         - minutes (int): Number of minutes between runs
         - start_date (datetime|str): When to start
         - end_date (datetime|str): When to end
         - timezone (datetime.tzinfo|str): Timezone to use

      CRON trigger kwargs:
         - year (int|str): Year to run on
         - month (int|str): Month to run on
         - day (int|str): Day of month to run on
         - week (int|str): Week of year to run on
         - day_of_week (int|str): Day of week to run on
         - hour (int|str): Hour to run on
         - minute (int|str): Minute to run on
         - timezone (datetime.tzinfo|str): Timezone to use

_PipelineJob
~~~~~~~~~~
.. py:class:: _PipelineJob

   Internal class that wraps pipeline execution for scheduling.

   **Methods:**

   .. py:method:: run(*args, **kwargs)

      Executes the scheduled pipeline.

   .. py:method:: __call__(*args, **kwargs)

      Makes the job callable by the scheduler.

Utility Functions
--------------

.. py:function:: get_pipeline_scheduler()

   Gets the global background scheduler instance.

   :return: Background scheduler for pipeline scheduling
   :rtype: BackgroundScheduler