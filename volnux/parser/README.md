# Pointy Language

Pointy is a small domain-specific language for describing event/task workflows. Arrows show how work flows, `||` runs work in parallel, and parenthesised branches react to the outcome of a task. This document describes the language as implemented by the parser in this package (`lexer.py`, `grammar.py`, `sema.py`).

```pointy
@author: "Jane Doe"
@mode: "DAG"

ReceiveOrder -> ValidateOrder (
    failure -> RejectOrder,
    success -> (ChargeCard || ReserveStock) -> ShipOrder
)
```

Contents

1. [Program structure](#1-program-structure)
2. [Tasks](#2-tasks)
3. [Chaining and operators](#3-chaining-and-operators)
4. [Conditional branching and descriptors](#4-conditional-branching-and-descriptors)
5. [Retry](#5-retry)
6. [Groupings](#6-groupings)
7. [Meta events](#7-meta-events)
8. [Directives](#8-directives)
9. [Variables](#9-variables)
10. [Types and expressions](#10-types-and-expressions)
11. [Semantic analysis](#11-semantic-analysis)
12. [Grammar summary](#12-grammar-summary)

## 1. Program structure

A program is any number of directives and variable declarations, with **at most one workflow chain** before, between or after them:

```pointy
# comments start with '#'
@description: "Order processing"
@threshold = 100

Fetch -> Process
```

- Directives (`@name: value`) and variable declarations (`@name = value`) may appear before or after the chain.
- A variable must be declared earlier in the text than the place where it is used (see [Variables](#9-variables)).
- An empty program is valid and has no chain.
- Whitespace and newlines are insignificant; there are no statement terminators.

The parser returns a `ProgramNode` with `chain`, `global_variables`, `directives`, `descriptors`, `author` and `description`.

## 2. Tasks

A task is an identifier. Tasks are the units of work in a workflow.

```pointy
FetchData
```

### Namespaces

A task can be prefixed with a namespace using `::`:

```pointy
billing::ChargeCard -> notifications::SendReceipt
```

Namespaces are user defined and only resolved at runtime, so the parser and semantic analysis accept any identifier. A task with no namespace is in the default `local` namespace.

### Attributes

Attach options to a task with a bracketed list of `name = expression` pairs:

```pointy
Worker[retry_attempts = 3, executor = "MyExecutor", extras = {"queue": "high"}]
```

Known option names are `retry_attempts`, `max_concurrency` (only meaningful on a `{}` group), `executor`, `executor_config`, `extras`, `result_evaluation_strategy`, `stop_condition` and `bypass_event_checks`. Other names are accepted but reported as warnings by semantic analysis (they end up in `extras`). Executor names are resolved at runtime and are not validated by the parser. Attribute values can be any [expression](#10-types-and-expressions), including variables.

## 3. Chaining and operators

| Operator | Meaning |
|---|---|
| `A -> B` | Run `B` after `A` completes; `B` receives `A`'s results as `previous_result` |
| `A \|-> B` | Stream `A`'s output into `B` while `A` is still running (see [Streaming](#streaming-with--)); an ordinary `->` when `A` does not stream |
| `A \|\| B` | Run `A` and `B` in parallel |
| `A * N` | Retry `A` up to `N` times (see [Retry](#5-retry)) |

### Precedence

`||` binds tighter than `->` and `|->`; `->` and `|->` have equal precedence and associate left to right.

```pointy
A -> B || C -> D
```

is `A -> (B || C) -> D`. Use parentheses to group a sequence inside a parallel block:

```pointy
(A -> B) || (C -> D)
```

Parallel results continue with `->`:

```pointy
(FetchUsers || FetchOrders) -> Merge
```

(`|->` cannot follow a parallel group: that would merge two streams into one consumer, which is not supported. See [Streaming](#streaming-with--).)

### Streaming with `|->`

`A |-> B` links two tasks by a **stream**. When `A`'s `process()` is a generator (`async def` with `yield`, or a plain generator) it yields `(success, data)` many times, and each is appended to a stream on the messaging backend (Redis or Kafka streams, or the in-memory backend). `B` reads the stream while `A` is still producing, with `async for chunk in self.input_stream()`.

```pointy
Numbers |-> Double |-> Collect       # three tasks, running together
Numbers |-> (AuditLog || Collect)    # fan-out: both read every chunk
(Orders || Refunds) |-> Ledger       # fan-in: Ledger reads one merged stream
{ Orders, Refunds -> Audit } |-> Ledger   # a cooperative group feeding a consumer
Numbers |-> Collect (failure -> Alert, success -> Publish)
```

**Fan-in.** `(A || B) |-> C` and `{ A, B -> D } |-> C` make every producer (for a group: the last task of each chain) feed `C`, which reads one merged stream: each source keeps its own order, chunks of different sources arrive in arrival order, `chunk.task_id` says where a chunk came from, and the stream ends when every source has ended. A producer that is not a generator contributes its return value as a single chunk; if *no* producer streams, `|->` is an ordinary `->`.

A run of `|->` tasks is one *segment*. Its members run at the same time; the segment succeeds when all of them do. If a consumer fails, its producers are asked to stop; if a producer fails, its stream is closed as failed and its consumers fail with it. When `A` is **not** a generator, `A |-> B` behaves exactly like `A -> B`.

Because the members run together, some constructs cannot sit inside a segment, and semantic analysis reports them as errors:

| Written | Why it is rejected |
|---|---|
| `A (1 \|-> B)`, `A (success \|-> B, failure \|-> C)` | A branch is chosen by `A`'s outcome, which is only known when its stream ends, but a streaming consumer has to start earlier. Use `->` for the branch. |
| `A (failure -> X) \|-> B` | Same reason: branches on a task that is followed by a stream edge. Put the branches on the **last** task of the segment (`A \|-> B (failure -> X)`). |
| `A * 3 \|-> B`, `(A * 2 \|\| B) \|-> C` | A second attempt of one member would replay chunks into a consumer that already read the first. Retry the whole segment: `{ A \|-> B }[retry_attempts = 3]`. |
| `{ A, B (failure -> X) } \|-> C` | The tail of a chain that feeds a stream cannot branch, for the same reason as above. |

Allowed: branches after the **end** of a segment (`A |-> B (failure -> X, success -> Y)`), and `A (1 -> C)` where `A` is a generator (not streaming: `C` starts when `A` has finished and receives a reference to the retained stream, readable with `self.replay()`).

## 4. Conditional branching and descriptors

A task can be followed by a parenthesised list of branches. Each branch maps a **descriptor** (an outcome) to a chain to run:

```pointy
Validate (failure -> HandleError, success -> Continue)
```

Branches are separated by commas, use `->`, and may contain whole chains. (A branch cannot use the streaming `|->`: see [Streaming](#streaming-with--).)

```pointy
Validate (
    failure -> LogError -> NotifyAdmin,
    success -> Save (failure -> Rollback, success -> Done)
)
```

### Descriptors

A descriptor is any non-negative integer. `0` and `1` are built in and can be written by name:

| Descriptor | Name | Meaning |
|---|---|---|
| `0` | `failure` | The task failed |
| `1` | `success` | The task succeeded |
| `2` and above | user defined | Custom outcomes |

```pointy
A (0 -> B, 1 -> C)
```

```pointy
A (failure -> B, success -> C)
```

`failure` and `success` are reserved words, so they cannot be used as task names.

### Custom descriptors

Name your own outcomes with the `@descriptor` directive. The parser numbers them from 2 in order and stores the mapping in `ProgramNode.descriptors`:

```pointy
@descriptor: out_of_balance, foreign_bank_not_supported

Transfer (
    success -> Done,
    out_of_balance -> TopUp,
    foreign_bank_not_supported -> Reject
)
```

Here `out_of_balance` is `2` and `foreign_bank_not_supported` is `3`. Labels may be declared before or after the chain, may not repeat, and may be spread over several `@descriptor` directives. Plain numbers (`25 -> X`) need no declaration. Using an undeclared label is a parse error.

### Invalid conditionals

A conditional with no task in front of it, and retry applied to a conditional expression, parse but are reported as errors by semantic analysis:

```text
(failure -> C, success -> B)        # error: no subject task
(failure -> C, success -> B) * 2    # error: retry cannot be applied to a conditional
```

Duplicate descriptors in one conditional (`A (failure -> B, 0 -> C)`) are also an error.

## 5. Retry

`* N` retries the thing on its left up to `N` times. `N` must be at least 2.

```pointy
Upload * 3
```

Retry works on tasks, meta events, groups and parenthesised chains, and the retried item may still have branches:

```pointy
(FetchA || FetchB) * 2
```

```pointy
{ Extract -> Transform } * 3
```

```pointy
Upload * 2 (failure -> Alert, success -> Done)
```

Retry is also allowed inside a branch:

```pointy
Validate (failure -> Repair * 2, success -> Continue)
```

Retry cannot be applied to a whole conditional expression: `A (failure -> B) * 3` is a syntax error.

## 6. Execution paradigms: `->`, `||` and `{}`

Three constructs decide *how* work runs, not just in which order:

| Paradigm | Written | Contents | How it runs |
|---|---|---|---|
| Sequential | `A -> B -> C` | tasks and subgraphs | one after another |
| Parallel | `(A \|\| B \|\| C)` | **tasks only** (`A \|\| (B -> C)` is rejected; a `{}` group counts as one unit and is allowed beside tasks) | the tasks are dispatched together as one unit, on the executor chosen for them |
| Cooperative | `{ A, B -> C, D -> Z \|-> M }` | **tasks or whole subgraphs**, separated by commas | every item runs concurrently in a sub-engine of its own, as async tasks on the engine's loop (and threads/processes through the executors of the tasks inside) |

`{}` is not mere grouping: its items are independent chains (each may branch, retry, stream with `|->` or hold nested `{}`), they run at the same time, and the group succeeds when all of them do. It can carry attributes and branches of its own, and what follows it continues once every chain has finished:

```pointy
{ Extract -> Transform -> Load }[retry_attempts = 2] -> Notify         # retried as one unit
{ FetchUsers, FetchOrders -> Enrich }[max_concurrency = 8] -> Merge    # at most 8 chains at once
{ A, B } (failure -> Alert, success -> Publish)                        # failure if any chain failed
{ Source |-> Sink, AuditTrail }                                        # a stream segment inside one item
```

A chain of the group failed when its engine failed or the last task it ran failed (a failure handled by a branch leaves a later task as the last one).

**Bounded by design.** Cooperative execution must not starve other work or multiply processes:

* the chains of one group run at most `max_concurrency` at a time (default: the `COOPERATIVE_MAX_CONCURRENCY` setting, 64; `0` is unlimited); the rest wait for a slot, none is dropped;
* process and thread pools are shared, fixed-size and created once (`SHARED_PROCESS_POOL_SIZE`, `SHARED_THREAD_POOL_SIZE`), so nested groups cannot each build a pool; work queues in submission order;
* a stream segment that is put on a pool smaller than the number of tasks that wait on each other runs on the async executor instead, so a consumer can never hold the only worker while its producer waits for one.

## 7. Meta events

Meta events apply a *template task* across data, controlling how it runs. The syntax is `MODE<Template>` with optional attributes, and the template may be namespaced:

```pointy
Fetch |-> MAP<ProcessItem>[concurrent = true] |-> Merge
```

```pointy
FOREACH<audit::Record> -> REDUCE<Summarize>
```

| Mode | Purpose |
|---|---|
| `MAP` | Apply the template to each item |
| `FILTER` | Keep the items the template accepts |
| `REDUCE` | Fold the items into a single result |
| `FOREACH` | Run the template for each item, for its side effects |
| `FLATMAP` | Map, then flatten the results |
| `FANOUT` | Fan the input out to several runs of the template |

Meta events are chain elements like tasks: they can be piped, grouped, retried and given attribute lists. The mode names are reserved words.

## 8. Directives

Directives configure or document the program. They are written `@name: value`, where the value is an [expression](#10-types-and-expressions); string values are quoted.

| Directive | Value | Purpose |
|---|---|---|
| `@mode` | `"DAG"` or `"CFG"` | Parser mode. `"DAG"` rejects cyclic workflows; `"CFG"` (the default) allows them |
| `@author` | string | Author, kept in `ProgramNode.author` |
| `@description` | string | Description, kept in `ProgramNode.description` |
| `@descriptor` | comma-separated names | Custom descriptors (see [section 4](#custom-descriptors)) |

```pointy
@mode: "DAG"
@author: "Platform Team"
@description: "Nightly settlement"
@descriptor: needs_review
```

All directives are also available in `ProgramNode.directives`. Repeating a directive keeps the last value (with a logged warning), except `@descriptor`, which accumulates. Semantic analysis reports unknown directive names and non-string `@author`, `@description` and `@mode` values.

## 9. Variables

Declare a variable with `@name = expression` and read it with `$name`:

```pointy
@workers = 4
@config = {"size": 100, "timeout": 30}
@regions = ["eu", "us"]

Fetch[count = $workers, limit = $config["size"]] -> Process[region = $regions[0]]
```

- `$name[expr]` indexes a list or map variable.
- `$env.NAME` reads an environment variable: `Connect[url = $env.DATABASE_URL]`.
- Referencing a variable that has not been declared earlier in the text is a parse error.
- Variables are scoped to the program being parsed; nothing carries over between parses.
- A variable that is declared but never used is a semantic warning.

## 10. Types and expressions

### Types

| Type | Examples |
|---|---|
| Integer | `42` |
| Float | `3.14`, `.5`, `1.0e3` (a decimal point is required; `1e3` is not a float) |
| String | `"hello"`, with escapes `\n \t \r \" \' \\` |
| Boolean | `true`, `false` |
| Null | `null` |
| List | `[1, "two", [3]]` |
| Map | `{"key": 1, "nested": {"a": true}}` (keys are strings) |

### Operators

Expressions can be used in attribute values, variable declarations and directive values. From loosest to tightest binding:

| Level | Operators |
|---|---|
| Ternary | `cond ? a : b` (right associative) |
| Null coalescing | `a ?? b` (left associative) |
| Logical or | `\|\|` |
| Logical and | `&&` |
| Bitwise or | `\|` |
| Bitwise xor | `^` |
| Bitwise and | `&` |
| Comparison | `==` `!=` `<` `>` `<=` `>=` |
| Shift | `<<` `>>` `>>>` |
| Additive | `+` `-` |
| Multiplicative | `*` `/` `%` |
| Unary | `!` `~` `-` |

Parentheses override precedence.

```pointy
@base = 8

Worker[
    port = $base * 1000 + 80,
    mode = $base > 4 ? "fast" : "slow",
    token = $env.TOKEN ?? "dev",
    flags = (1 << 3) | 1
]
```

The ternary condition is an arithmetic/logical expression, and the `??` operands cannot themselves be ternaries unless parenthesised.

## 11. Semantic analysis

After parsing, `Sema().analyse(program)` returns diagnostics.

Errors:

- duplicate attribute names on a task, meta event or group
- duplicate descriptors in a conditional
- unknown directive names; non-string `@author` / `@description`; `@mode` other than exactly `"DAG"` or `"CFG"`
- a conditional with no subject task, and retry applied to a conditional
- streaming misuse: a branch written with `|->`, branches on a task (or on the tail of a `{}` chain) that is followed by `|->`, and retry of one member of a stream
- `max_concurrency` below 1 (and a warning when it is put on something that is not a `{}` group)
- division or modulo by a literal zero
- a negative `retry_attempts`

Warnings:

- unknown option names, and option values of the wrong type
- `executor` that is not a string, and unrecognised `result_evaluation_strategy` or `stop_condition` values
- variables declared but never referenced
- task and template names that are not PascalCase
- operator misuse on literals (arithmetic on strings or booleans, bitwise operations on floats or strings) and constant `&&` / `||` operands

Namespaces and executors are runtime concerns and are not checked.

## 12. Grammar summary

```text
program        := declaration* [ chain declaration* ]
declaration    := '@' name '=' expression
                | '@' name ':' expression
                | '@descriptor' ':' name (',' name)*

chain          := parallel (('->' | '|->') parallel)*
parallel       := step ('||' step)*
step           := unit | unit '*' INT | conditional
conditional    := step '(' branch (',' branch)* ')'
branch         := descriptor ('->' | '|->') chain      # '|->' parses but is rejected by semantic analysis
descriptor     := INT | 'failure' | 'success' | name
unit           := task | meta | group | '(' chain ')'
task           := [namespace '::'] name [attributes]
meta           := MODE '<' [namespace '::'] name '>' [attributes]
group          := '{' chain '}' [attributes]
attributes     := '[' name '=' expression (',' name '=' expression)* ']'
MODE           := MAP | FILTER | REDUCE | FOREACH | FLATMAP | FANOUT
```

Reserved words: `MAP FILTER REDUCE FOREACH FLATMAP FANOUT true false null failure success`.
