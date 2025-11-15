.. volnux documentation master file, created by
   sphinx-quickstart on Sat Mar  1 02:47:26 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

volnux
=====

.. image:: ../../img/volnux.svg
   :height: 100px
   :width: 100px
   :align: left
   :alt: pipeline

|build-status| |code-style| |status| |latest| |pyv| |prs|

.. |build-status| image:: https://github.com/nshaibu/volnux/actions/workflows/python_package.yml/badge.svg
   :target: https://github.com/nshaibu/volnux/actions

.. |code-style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black

.. |status| image:: https://img.shields.io/pypi/status/volnux.svg
   :target: https://pypi.python.org/pypi/volnux

.. |latest| image:: https://img.shields.io/pypi/v/volnux.svg
   :target: https://pypi.python.org/pypi/volnux

.. |pyv| image:: https://img.shields.io/pypi/pyversions/volnux.svg
   :target: https://pypi.python.org/pypi/volnux

.. |prs| image:: https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square
   :target: http://makeapullrequest.com

Introduction
-----------
**Simplify complex process automation with a flexible, high-performance framework.**

This library tackles the challenges of building reliable, scalable workflows by
providing a clear separation between coordination and execution. It uses a declarative DSL,
**Pointy-Lang**, to model your pipelines while managing the underlying complexity of concurrency,
state, and task dependencies.

Build resilient automation that can handle anything from simple data processing to distributed, event-driven systems.


Features
~~~~~~~~
- Define and manage events and pipelines in Python
- Support for conditional task execution
- Easy integration of custom event processing logic
- Supports remote task execution and distributed processing
- Seamless handling of task dependencies and event execution flow

Installation
~~~~~~~~~~~
To install the library, simply use pip:

.. code-block:: bash

   pip install volnux

Requirements
~~~~~~~~~~~
- Python>=3.8
- ply==3.11 
- treelib==1.7.0
- more-itertools<=10.6.0
- apscheduler<=3.11.0
- graphviz==0.20.3 (Optional)

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   tutorials/index
   usage/pipeline
   usage/events
   usage/scheduling
   usage/signals
   usage/telemetry
   usage/contributing
   api/index

