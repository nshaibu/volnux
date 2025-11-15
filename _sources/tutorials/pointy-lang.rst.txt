Tutorials - Pointy-lang
=======================

Pointy Language is a powerful domain-specific language (DSL) for creating event-based workflows. This tutorial will
guide you from basic concepts to advanced workflows using Pointy Language's intuitive arrow-based syntax.



1. Introduction to Pointy Language
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pointy Language uses arrows and operators to represent event flow, making it easy to visualize and define workflows.
Its primary purpose is to model sequences of operations, conditional branching, parallel execution, and result piping.

Core Features:
--------------

- Sequential execution of events
- Parallel processing
- Conditional branching based on success/failure
- Result piping between events
- Retry mechanisms for handling failures

Defined Events
--------------

Events like ``Submit``, ``Validate`` or ``Reject`` are predefined operations that are automatically resolved during pipeline execution.
These events are defined in two main ways:

- By inheriting from the EventBase class inside the nexus module.
- By decorating a Python function with the @event decorator from the nexus.decorators module.

Once defined, these events are automatically detected and registered into the pipeline runtime, requiring no additional configuration
to participate in the event graph execution. When any of these event names are used within a Pointy script, they are automatically resolved
by the pipeline engine and executed according to their Python definitions.

This mechanism enables Pointy Language to serve as a thin, expressive orchestration layer on top of reusable Python logic,
allowing you to abstract low-level function implementation while building high-level, declarative workflows.

2. Basic Syntax and Operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Single Events
-------------

The most basic element in Pointy Language is a single event, represented by a name::

    A    # This defines a single event named A

Events are units of work that get executed as part of your workflow.

Directional Operator (->)
-------------------------

The arrow operator defines sequential flow between events::

    A -> B    # Execute event A, then execute event B

This indicates that event B will only start after event A has completed.

Parallel Operator (||)
----------------------

The parallel operator runs events concurrently::

    A || B    # Execute event A and event B in parallel

Both events start at the same time and run independently.

Pipe Result Operator (``|->``)
------------------------------

This operator passes the output from one event as input to another::

    A |-> B    # The result of event A becomes the input for event B

This is useful when one event depends on data produced by another.

Conditional Branching
---------------------

Pointy Language uses descriptors (numbers) to define different execution paths::

    A -> B (0 -> C, 1 -> D)

In this example:

- If B fails (descriptor 0), execute C
- If B succeeds (descriptor 1), execute D

Retry Operator (*)
------------------

The asterisk operator defines retry behavior for events::

    A * 3    # Retry event A up to 3 times if it fails

This increases reliability by automatically retrying failed events.

3. Building Your First Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's create a simple workflow for processing a document::

    ReceiveDocument -> ValidateFormat (
        0 -> NotifyFormatError,
        1 -> ExtractData
    ) -> SaveToDatabase (
        0 -> LogDatabaseError,
        1 -> SendConfirmation
    )

This workflow:

1. Receives a document
2. Validates its format
3. On valid format, extracts data; otherwise notifies of an error
4. Attempts to save to a database
5. On successful save, sends confirmation; otherwise logs the error

4. Intermediate Concepts
~~~~~~~~~~~~~~~~~~~~~~~~

Combining Parallel and Sequential Operations
--------------------------------------------

You can mix parallel operations with sequential ones::

    A -> B || C -> D

This executes A first, then B and C in parallel, and finally D after both B and C complete.

Piping Results from Parallel Operations
---------------------------------------

Results from parallel operations can be piped to subsequent events::

    A || B |-> C

Here, the results of both A and B are combined and passed to C.

Multiple Branches with Sink
---------------------------

Create complex decision trees with multiple branches that reunite::

    A (0 -> B, 1 -> C) -> D

This executes A, then either B (if A fails) or C (if A succeeds), and finally D regardless of which branch was taken.

5. Advanced Workflows
~~~~~~~~~~~~~~~~~~~~~

Error Handling with Result Piping
---------------------------------

::

    ProcessOrder -> ValidatePayment (
        0 |-> LogPaymentError -> NotifyCustomer,
        1 -> FulfillOrder
    )

This workflow pipes error information from a failed validation directly to the logging step.

Custom Descriptors
------------------

Pointy Language allows descriptors 3-9 for user-defined conditions::

    AnalyzeData -> EvaluateResults (
        0 -> HandleError,
        1 -> ProcessSuccess,
        3 -> ReviewManually  # Custom condition for cases requiring human review
    )

Complex Nested Workflow
-----------------------

Let's build a more sophisticated order processing workflow::

    ReceiveOrder -> ValidateInventory (
        0 -> NotifyOutOfStock |-> SuggestAlternatives,
        1 -> ProcessPayment (
            0 -> RefundCustomer,
            1 -> PrepareShipment || GenerateInvoice |-> NotifyWarehouse
        )
    ) -> UpdateOrderStatus * 3

This workflow:

1. Validates inventory availability
2. Handles out-of-stock situations with notifications and suggestions
3. Processes payment when items are available
4. On payment success, prepares shipment and generates invoice in parallel
5. Notifies the warehouse with combined shipment and invoice data
6. Updates order status with retry capability (up to 3 attempts)

6. Best Practices
~~~~~~~~~~~~~~~~~

Naming Conventions
------------------

Use descriptive event names that clearly indicate the action being performed.

Error Handling
--------------

Always define paths for both success and failure cases to ensure robust workflows.

Modularization
--------------

Break complex workflows into smaller, reusable components.

Documentation
-------------

Comment complex sections of your workflow to explain decision points and conditions.

7. Practical Example: Customer Onboarding Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    ReceiveApplication -> ValidateInformation * 2 (
        0 -> RequestCorrections |-> NotifyApplicant,
        1 -> PerformCreditCheck (
            0 -> AssessRisk || OfferLimitedServices |-> NotifyDecision,
            1 -> CreateAccount || PrepareWelcomePackage || SetupAutopay |-> ActivateServices
        )
    ) -> SendConfirmationEmail (
        0 -> LogEmailFailure -> AttemptSMS,
        1 -> ScheduleFollowUp
    )

This comprehensive workflow handles a new customer application with:

1. Information validation with retry capability
2. Credit check with different paths based on results
3. Parallel processes for account setup
4. Fallback communication methods
5. Follow-up scheduling

8. Real-World Example: Data Ingestion Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    Preprocess * 3 -> Validate (
        0 -> FailHandler,
        1 -> Enrich
    ) || LogPreprocessing -> Enrich -> StoreData (
        0 -> RetryStore * 2,
        1 -> NotifySuccess
    )

Explanation:

1. Retry Preprocess up to 3 times.
2. If Validate fails, run FailHandler; else go to Enrich.
3. LogPreprocessing executes in parallel.
4. Enrich is followed by StoreData.
5. If StoreData fails, RetryStore executes up to 2 retries.
6. On success, NotifySuccess runs.

9. Use Case: Scheduled ETL Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    Extract -> Clean || Validate || Deduplicate |-> Transform -> Load (0 -> AlertAdmin, 1 -> SuccessLog)

Explanation:

1. Data is extracted and passed to three parallel stages: cleaning, validation, and deduplication.
2. Results are piped into a transformation step.
3. Load step attempts to store data; on failure, notify admin, otherwise log success.

10. Highly Parallel Workflow with Result Routing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    A || B || C |-> D (0 -> ErrorHandler, 1 -> Finalize)

Use Case:

Concurrent processing of services A, B, and C, combined into D. Depending on D's result,
route to either ErrorHandler or Finalize.

11. Dynamic Routing with Custom Condition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    Review -> Decision (0 -> Rework, 1 -> Approve, 3 -> AuditTrail)

Explanation:

1. Descriptor 3 can represent "needs manual audit".
2. Route outcome accordingly.

12. Nested Parallel with Retry and Conditional Sink
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    Init * 2 -> (Fetch || Parse || Normalize) |-> Aggregate (0 -> AlertOps, 1 -> Save)

Explanation:

1. Init is retried twice.
2. Three events execute in parallel.
3. Results are aggregated.
4. If aggregation fails, notify ops. Else save results.

13. Complex Approval Workflow with Decision Sink
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    Submit -> Validate (
        0 -> Reject,
        1 -> AutoApprove || HumanReview |-> FinalApproval (0 -> Escalate, 1 -> NotifyUser)
    )

Scenario:

1. A form submission is validated.
2. On success, it goes to approval, which can be automatic or manual.
3. Results go to final decision.
4. If failed, escalate; if passed, notify the user.

14. Conclusion
~~~~~~~~~~~~~~

Pointy Language provides an elegant solution for defining complex workflows with its intuitive arrow-based syntax. By combining
sequential operations, parallel processing, conditional branching, and result piping, you can create sophisticated event-based
systems that handle both happy paths and error conditions seamlessly.

As you become more familiar with Pointy Language, you'll find it increasingly natural to express even the most complex business
processes in this concise and visual format.

This makes it ideal for event-driven systems, data processing flows, microservice orchestration, ETL pipelines,
and any scenario requiring dynamic, observable execution control.

Appendix: Quick Reference
~~~~~~~~~~~~~~~~~~~~~~~~~

Operators
---------

- ``->`` : Sequential execution
- ``||`` : Parallel execution
- ``|->`` : Result piping
- ``*`` : Retry mechanism

Descriptors
-----------

- ``0`` : Failure path
- ``1`` : Success path
- ``3-9`` : User-defined conditions

Common Patterns
---------------

- ``A -> B`` : Basic sequence
- ``A || B`` : Parallel execution
- ``A |-> B`` : Result piping
- ``A -> B (0 -> C, 1 -> D)`` : Conditional branching
- ``A * 3`` : Retry logic
- ``A || B |-> C`` : Parallel with combined results
- ``A (0 -> B, 1 -> C) -> D`` : Multiple branches with sink