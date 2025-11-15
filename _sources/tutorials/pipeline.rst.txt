Tutorials - Pipeline
====================

This comprehensive tutorial will guide you through creating powerful event-based workflows using the ``nexus`` module and Pointy Language syntax. By the end, you'll understand how to define, visualize, and execute complex pipelines for your applications.


Setting Up Your First Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To begin using ``nexus``, you'll need to create a custom pipeline class by inheriting from the base ``Pipeline`` class::

    from nexus import Pipeline

    class MyPipeline(Pipeline):
        # Pipeline definition will go here
        pass

This basic structure serves as the foundation for your pipeline. Next, we'll add input fields and define the workflow structure.

Defining Input Fields
~~~~~~~~~~~~~~~~~~~~~

Input fields define the data that flows through your pipeline. The ``nexus.fields`` module provides field types to specify data requirements::

    from nexus import Pipeline
    from nexus.fields import InputDataField

    class MyPipeline(Pipeline):
        # Define input fields as class attributes
        username = InputDataField(data_type=str, required=True)
        email = InputDataField(data_type=str, required=True)
        age = InputDataField(data_type=int, required=False, default=0)

Events in your pipeline can access these fields by including the field name in their ``process`` method arguments. For example::

    def process(self, username, email):
        # Access the input fields directly
        return f"Processing data for {username} ({email})"

Structuring Workflows with Pointy Language
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pointy Language Basics
----------------------

Pointy Language uses arrow-based syntax to define the flow between events. Here are the key operators:

- **Directional Operator (->)**: Sequential execution of events
- **Parallel Operator (||)**: Concurrent execution of events
- **Pipe Result Operator (|->)**: Pass output from one event as input to another
- **Conditional Branching**: Define different paths based on success/failure
- **Retry Operator (*)**: Automatically retry failed events

Defining Workflow Structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are three ways to define your pipeline structure using Pointy Language:

1. Using External ``.pty`` Files
--------------------------------

Create a file with the same name as your pipeline class (e.g., ``MyPipeline.pty``)::

    FetchData -> ValidateData (
        0 -> HandleValidationError,
        1 -> ProcessData
    ) -> SaveResults

The ``nexus`` module will automatically load this file if it has the same name as your pipeline class.

2. Using the Meta Subclass
--------------------------

You can embed the Pointy Language script directly in your class::

    class MyPipeline(Pipeline):
        username = InputDataField(data_type=str, required=True)

        class Meta:
            pointy = "FetchData -> ValidateData (0 -> HandleError, 1 -> ProcessData) -> SaveResults"

3. Using Meta Dictionary
------------------------

Alternatively, use a dictionary to define meta options::

    class MyPipeline(Pipeline):
        username = InputDataField(data_type=str, required=True)

        meta = {
            "pointy": "FetchData -> ValidateData -> ProcessData"
        }

You can also specify a custom file path::

    class MyPipeline(Pipeline):
        class Meta:
            file = "/path/to/your/workflow.pty"

Advanced Pipeline Techniques
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sequential Execution
--------------------

The most basic flow pattern executes events in sequence::

    A -> B -> C

This ensures that event B only starts after event A completes, and event C only starts after event B completes.

Parallel Execution
------------------

For concurrent operations, use the parallel operator::

    A -> (B || C) -> D

In this example, events B and C execute simultaneously after A completes, and D executes once both B and C are finished.

Result Piping
-------------

When one event depends on data from another::

    A |-> B

The output of event A becomes the input for event B.

Conditional Branching
---------------------

Create different execution paths based on success or failure::

    ValidateData (
        0 -> HandleValidationError,
        1 -> ProcessData
    )

In this example:

- If ``ValidateData`` fails (0), execute ``HandleValidationError``
- If ``ValidateData`` succeeds (1), execute ``ProcessData``

Custom Descriptors
------------------

Beyond success (1) and failure (0), you can define custom conditions with descriptors 3-9::

    AnalyzeData (
        0 -> HandleError,
        1 -> ProcessNormalData,
        3 -> ProcessPriorityData
    )

Automatic Retries
-----------------

Implement retry logic for events that might fail temporarily::

    SendNotification * 3 -> LogResult

This will retry the ``SendNotification`` event up to 3 times if it fails.

Complex Workflow Example
------------------------

Here's a more complex example combining multiple features::

    Initialize -> FetchData * 2 (
        0 -> LogFetchError |-> NotifyAdmin,
        1 -> ValidateData (
            0 -> CleanData |-> RetryValidation,
            1 -> ProcessMetadata || EnrichData |-> SaveResults
        )
    ) -> SendNotification (
        0 -> LogNotificationError,
        1 -> MarkComplete
    )

This workflow:

1. Initializes the pipeline
2. Fetches data with up to 2 retries
3. On fetch failure, logs an error and notifies an admin
4. On fetch success, validates the data
5. On validation failure, cleans the data and retries validation
6. On validation success, processes metadata and enriches data in parallel
7. Saves the results
8. Sends a notification
9. Either logs notification errors or marks the process complete

Visualizing Pipelines
~~~~~~~~~~~~~~~~~~~~~

The ``nexus`` module provides tools to visualize your pipeline structure:

ASCII Representation
--------------------

Generate a text-based diagram of your pipeline::

    # Instantiate your pipeline
    pipeline = MyPipeline()

    # Print ASCII representation
    pipeline.draw_ascii_graph()

Graphviz Visualization
----------------------

For a more detailed graphical representation (requires Graphviz and xdot)::

    # Generate and save a graphical representation
    pipeline.draw_graphviz_image(directory="/path/to/output/directory")

This creates a visual diagram showing the flow between events, including branches and conditions.

Executing Pipelines
~~~~~~~~~~~~~~~~~~~

To run your pipeline, instantiate it with input values and call the ``start`` method::

    # Create pipeline instance with input values
    pipeline = MyPipeline(
        username="john_doe",
        email="john@example.com",
        age=30
    )

    # Execute the pipeline
    result = pipeline.start()

    # Access the result
    print(result)

You can also provide input values when starting the pipeline::

    pipeline = MyPipeline()
    result = pipeline.start(username="john_doe", email="john@example.com")

Complete Example: Document Processing Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's put everything together with a complete example of a document processing pipeline::

    from nexus import Pipeline
    from nexus.fields import InputDataField

    class DocumentProcessingPipeline(Pipeline):
        """Pipeline for processing document files."""

        # Define input fields
        document_path = InputDataField(data_type=str, required=True)
        user_id = InputDataField(data_type=int, required=True)
        priority = InputDataField(data_type=str, required=False, default="normal")

        class Meta:
            pointy = """
            ValidateDocument -> ExtractMetadata (
                0 -> LogExtractionError |-> NotifyUser,
                1 -> (
                    IndexContent ||
                    GenerateThumbnail ||
                    AnalyzeContent
                ) |-> SaveResults
            ) -> UpdateUserQuota (
                0 -> LogQuotaError,
                1 -> SendSuccessNotification
            ) * 3
            """

    # Example usage
    if __name__ == "__main__":
        # Create pipeline instance
        pipeline = DocumentProcessingPipeline(
            document_path="/path/to/document.pdf",
            user_id=12345,
            priority="high"
        )

        # Visualize the pipeline structure
        pipeline.draw_ascii_graph()

        # Execute the pipeline
        result = pipeline.start()
        print(f"Pipeline execution completed with result: {result}")

In this example:

1. We define a document processing pipeline with three input fields
2. The pipeline structure:

   - Validates the document
   - Extracts metadata
   - On extraction failure, logs an error and notifies the user
   - On extraction success, performs three operations in parallel (indexing, thumbnail generation, and content analysis)
   - Saves the combined results
   - Updates the user's quota with up to 3 retries
   - Either logs quota errors or sends a success notification

3. We visualize the pipeline using ASCII representation
4. We execute the pipeline with sample input values

Summary
~~~~~~~

The ``nexus`` module combined with Pointy Language provides a powerful framework for defining, visualizing, and executing complex workflows. By leveraging the arrow-based syntax, you can create sophisticated processing pipelines with conditional branching, parallel execution, result piping, and automatic retries.

Key takeaways:

1. Create pipeline classes by inheriting from the ``Pipeline`` class
2. Define input fields using ``InputDataField`` and other field types
3. Structure your workflow using Pointy Language syntax
4. Visualize pipelines with ASCII or Graphviz representations
5. Execute pipelines by instantiating with input values and calling ``start()``

With these tools, you can build reliable, maintainable, and visually comprehensible workflows for a wide range of applications.