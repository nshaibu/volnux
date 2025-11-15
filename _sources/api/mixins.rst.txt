API Reference - Mixins
==================

Core mixins that provide reusable functionality across the Event Pipeline framework.

Identity Mixin
------------

ObjectIdentityMixin
~~~~~~~~~~~~~~~~
.. py:class:: ObjectIdentityMixin

   Provides unique identification and state management functionality.

   **Methods:**

   .. py:method:: id

      Property that returns a unique identifier for the object.

   .. py:method:: __object_import_str__

      Property that returns the import string for the object's class.

   .. py:method:: get_state() -> Dict[str, Any]

      Get the serializable state of the object.

   .. py:method:: set_state(state: Dict[str, Any]) -> None

      Restore the object's state from a dictionary.

Backend Integration
----------------

BackendIntegrationMixin
~~~~~~~~~~~~~~~~~~~~
.. py:class:: BackendIntegrationMixin

   Provides integration with backend storage systems.

   **Methods:**

   .. py:method:: save()

      Save the object to the backend store.

   .. py:method:: reload()

      Reload the object's state from the backend store.

   .. py:method:: delete()

      Delete the object from the backend store.

   .. py:method:: update()

      Update the object's state in the backend store.

   .. py:method:: get_schema_name() -> str

      Get the schema name for backend storage.

Scheduling
--------

ScheduleMixin
~~~~~~~~~~~
.. py:class:: ScheduleMixin

   Provides scheduling capabilities for pipelines.

   **Inner Classes:**

   .. py:class:: ScheduleTrigger

      Enumeration of available trigger types.

      - DATE: One-time scheduled execution
      - INTERVAL: Repeating execution at fixed intervals
      - CRON: Cron-style scheduled execution

   **Methods:**

   .. py:method:: schedule_job(trigger: ScheduleTrigger, **scheduler_kwargs)

      Schedule a job for execution.

      :param trigger: The type of trigger to use
      :param scheduler_kwargs: Arguments for the specific trigger type

   .. py:classmethod:: get_pipeline_scheduler()

      Get the global pipeline scheduler instance.