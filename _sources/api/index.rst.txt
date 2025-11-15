API Reference
=============

This section provides detailed API documentation for the Event Pipeline framework.

.. toctree::
   :maxdepth: 2
   :caption: Core Components:

   events
   pipeline
   fields
   scheduling
   metrics
   executors
   managers
   telemetry
   result
   backend
   signals
   mixins
   parser

Core Components
---------------

The Event Pipeline framework consists of several core components:

Events & Pipeline
~~~~~~~~~~~~~~~~~
Core execution components that define event behavior and pipeline structure:

- Event system (:doc:`events`)
- Pipeline execution (:doc:`pipeline`) 
- Event results (:doc:`result`)

Data & Signals
~~~~~~~~~~~~~~
Data handling and communication components:

- Data fields (:doc:`fields`)
- Signaling system (:doc:`signals`)
- Pipeline parsers (:doc:`parser`)

Infrastructure
~~~~~~~~~~~~~~
Backend and execution infrastructure:

- Backend storage (:doc:`backend`)
- Execution management (:doc:`executors`)
- Task managers (:doc:`managers`)
- Reusable mixins (:doc:`mixins`)

Monitoring & Management  
~~~~~~~~~~~~~~~~~~~~~~~
Operational management components:

- Scheduling (:doc:`scheduling`)
- Metrics collection (:doc:`metrics`)
- Telemetry (:doc:`telemetry`)