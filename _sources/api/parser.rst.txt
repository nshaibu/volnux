API Reference - Parser
==================

Pipeline task expression parsing and grammar components.

Lexer
----

PointyLexer
~~~~~~~~~~
.. py:class:: PointyLexer

   Lexical analyzer for pipeline task expressions.

   **Tokens:**

   - SEPERATOR: Comma separator between tasks (,)
   - POINTER: Task flow indicator (->)
   - PPOINTER: Parallel task pointer (|->)
   - PARALLEL: Parallel execution indicator (||)
   - RETRY: Task retry indicator (*)
   - TASKNAME: Valid task identifier
   - LPAREN, RPAREN: Parentheses for grouping
   - NUMBER: Numeric literals
   - DIRECTIVE: Parser directives
   - COMMENT: Task expression comments (#)

Grammar Parser
-----------

pointy_parser
~~~~~~~~~~~
.. py:data:: pointy_parser

   PLY-based parser for task expressions. Processes pipeline task definitions and builds an abstract syntax tree.

   **Grammar Rules:**
   
   - Task Flow: ``task -> task``
   - Parallel Tasks: ``task || task``
   - Task Groups: ``(task1, task2)``
   - Task Retry: ``task *``
   - Conditional Flow: ``task(condition)``

AST Components
------------

.. py:class:: TaskName

   Represents a task identifier in the AST.

   **Attributes:**
      - name: The task name

.. py:class:: Descriptor

   Represents a task descriptor/number in the AST.

   **Attributes:**
      - value: The descriptor value

.. py:class:: BinOp

   Represents a binary operation between tasks.

   **Attributes:**
      - left: Left task operand
      - right: Right task operand
      - op: Operation type (pointer, parallel, etc)

.. py:class:: ConditionalBinOP

   Represents a conditional task flow.

   **Attributes:**
      - task: The task to execute
      - condition: The condition group to evaluate

.. py:class:: ConditionalGroup

   Represents a group of conditions.

   **Attributes:**
      - tasks: List of conditional tasks

Example Usage
-----------

.. code-block:: python

   # Simple sequential flow
   "task1 -> task2 -> task3"

   # Parallel execution
   "task1 || task2 -> task3"

   # Task groups with retry
   "(task1, task2) * -> task3"

   # Conditional flow
   "task1(task2, task3) -> task4"