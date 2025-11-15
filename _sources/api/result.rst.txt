API Reference - Results
==================

Event Results
-----------

EventResult
~~~~~~~~~
.. py:class:: EventResult

   Represents the result of an event execution with metadata.

   **Attributes:**

   :param error: Whether the execution resulted in an error
   :param event_name: Name of the event that produced this result
   :param content: Result content/data
   :param task_id: Unique identifier for the task
   :param init_params: Event initialization parameters
   :param call_params: Event call parameters
   :param process_id: ID of the process that executed the event
   :param creation_time: Timestamp of result creation

   **Methods:**

   .. py:method:: get_state() -> Dict[str, Any]

      Get the serializable state of the result.

   .. py:method:: set_state(state: Dict[str, Any])

      Restore the result state from a dictionary.

   .. py:method:: is_error() -> bool

      Check if the result represents an error.

   .. py:method:: as_dict() -> dict

      Convert the result to a dictionary.

Result Collections
---------------

ResultSet
~~~~~~~~
.. py:class:: ResultSet

   A collection of Result objects with advanced filtering and query capabilities.
   Implements MutableSet interface for collection operations.

   **Methods:**

   .. py:method:: add(result: Union[Result, ResultSet])

      Add a result or merge another ResultSet.

   .. py:method:: discard(result: Union[Result, ResultSet])

      Remove a result or results from another ResultSet.

   .. py:method:: clear()

      Remove all results.

   .. py:method:: copy() -> ResultSet

      Create a shallow copy of this ResultSet.

   .. py:method:: get(**filters) -> Result

      Get a single result matching the filters. Raises MultiValueError if more than one match found.

      :raises: KeyError if no match found
      :raises: MultiValueError if multiple matches found

   .. py:method:: filter(**filter_params) -> ResultSet

      Filter results by attribute values with support for nested fields.

      **Features:**

      - Basic attribute matching (user=x)
      - Nested dictionary lookups (profile__name=y)
      - List/iterable searching (tags__contains=z)

      **Special Operators:**

      - __contains: Check if value is in a list/iterable
      - __startswith: String starts with value
      - __endswith: String ends with value
      - __icontains: Case-insensitive contains
      - __gt, __gte, __lt, __lte: Comparisons
      - __in: Check if field value is in provided list
      - __exact: Exact matching (default behavior)
      - __isnull: Check if field is None

      **Examples:**

      .. code-block:: python

         rs.filter(name="Alice")  # Basic field matching
         rs.filter(user__profile__city="New York")  # Nested lookup
         rs.filter(tags__contains="urgent")  # Check if list contains value
         rs.filter(name__startswith="A")  # String prefix matching

   .. py:method:: first() -> Optional[Result]

      Return the first result or None if empty.

Entity Types
----------

EntityContentType
~~~~~~~~~~~~~~
.. py:class:: EntityContentType

   Represents the content type information for an entity.

   **Attributes:**

   :param backend_import_str: Import string for the backend
   :param entity_content_type: Content type identifier

   **Methods:**

   .. py:classmethod:: add_entity_content_type(record: Result) -> Optional[EntityContentType]

      Create and register a content type for a result record.