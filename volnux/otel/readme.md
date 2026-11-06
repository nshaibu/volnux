# volnux.otel — OpenTelemetry for Volnux

Tracing and metrics for workflows, from the API call to each event, across Celery
workers and Kubernetes Jobs. This is the reference; the short version is in the
"Observability" section of the project README.

## Contents

1. [Module map](#module-map)
2. [Setup](#setup)
3. [Spans](#spans)
4. [Metrics](#metrics)
5. [Traces across processes](#traces-across-processes)
6. [Application helpers](#application-helpers)
7. [How the instrumentation works](#how-the-instrumentation-works)
8. [Adding instrumentation](#adding-instrumentation)
9. [Testing](#testing)
10. [Troubleshooting](#troubleshooting)

## Module map

| Module | What it is |
| --- | --- |
| `volnux.otel` | `initialize_otel()`, `shutdown_otel()`, `VolnuxObservability`, `ObservabilityBackend`. The one entry point. |
| `volnux.observability` | What `initialise_workflows` uses: `start_tracing_for_setup()`, `setup_phase()`, `record_setup_duration()`, `shutdown_observability()`. Imports no OpenTelemetry until it is enabled. |
| `volnux.otel.bootstrap` | `setup_otel()`: `initialize_otel` driven by the `OTEL_*` settings (idempotent). |
| `volnux.otel.tracer_setup` | `VolnuxTracerConfig`, `VolnuxTracer`, `get_tracer()`: the tracer provider, sampler and OTLP exporters. |
| `volnux.otel.metrics` | `VolnuxMetricsRegistry` (every metric definition), `VolnuxMetricsCollector`, `initialize_metrics()`. |
| `volnux.otel.context_manager` | `OTelContextManager` (the span/context of each `ExecutionContext`), `SpanHelper` (attributes). |
| `volnux.otel.propagation` | Carrying the active trace in the `volnux.context` carrier. |
| `volnux.otel.utils` | `get_current_trace_id()`, `add_span_attribute()`, `add_span_event()`, ... for application code. |
| `volnux.otel.instrumentations.signal` | `Pipeline.start` root span and the 17 signal hooks. |
| `volnux.otel.instrumentations.engine` | `engine.execute`. |
| `volnux.otel.instrumentations.context_coordinator` | `ExecutionContext` and `ExecutionCoordinator`. |
| `volnux.otel.instrumentations.flow` | Flows, events, flow metrics. |
| `volnux.otel.instrumentations.framework` | Triggers, executors, registry, checkpoints, rehydration, sagas, messaging, remote streaming (one table). |
| `volnux.otel.instrumentations._patching` | `patch_attr` / `unpatch_all`. |
| `volnux.otel.instrumentations._common` | `traced`, `clean_attributes`, `get_tracer`, ... shared building blocks. |

## Setup

### From the settings (`initialise_workflows`)

`volnux.setup.initialise_workflows()` starts telemetry itself. It loads the
project's configuration (`settings.py`, `init.py`), configures logging, and then
calls `volnux.observability.start_tracing_for_setup()`, which reads `OTEL_ENABLED`
and, if set, runs `volnux.otel.bootstrap.setup_otel()` (below). Every entry point
goes through it: the CLI commands, `volnux serve|dev`, the Celery worker's
`register_worker_init`, the Kubernetes job runner. Nothing needs to be called from
`init.py`.

* **Off by default.** `volnux.observability` imports nothing from OpenTelemetry (an
  optional install) until `OTEL_ENABLED` is set; if the install is missing a
  warning says so and setup goes on.
* **Idempotent.** A second `initialise_workflows` in the process (hot reload, tests)
  or a project that called `initialize_otel` itself first is left as it is.
* **Role.** The `ROLE` setting (`VOLNUX_ROLE`, default `api`) labels the telemetry
  `volnux.role`; `volnux serve --role worker` exports it.
* **Never fatal.** A failing telemetry start is logged and setup carries on.

What it traces (spans in `setup.*`, all children of the first):

| Span | Attributes |
| --- | --- |
| `setup.initialise_workflows` | `project.path`, `workflow.filter`, `role`, `setup.config_load_ms` (the configuration is loaded before telemetry can start), `setup.workflows` |
| `setup.configure_reporters` | |
| `setup.stop_running_engine` | only when a trigger engine was running (hot reload) |
| `setup.discover_workflows` | `setup.sources` (not when the registry was already loaded) |
| `setup.load_workflows` | `setup.workflows`, `setup.failed_sources`; `registry.load` and its sources nest under it |
| `setup.configure_executor` | `executor.class` (only when the engine had no executor yet) |

and records the `setup.duration` histogram (ms, attribute `outcome`: `success` /
`error`, measured from the start of the function, configuration included). A
failure marks the spans as errors and raises the original exception.

`volnux.setup.shutdown_observability()` shuts telemetry down for a process that
owns its lifetime (the servers do it through their own shutdown function).

### From the settings (servers)

```bash
VOLNUX_OTEL_ENABLED=true
VOLNUX_OTEL_BACKEND=grafana          # grafana | tempo | datadog | jaeger | otlp (anything else = otlp)
VOLNUX_OTEL_ENDPOINT=http://otel-collector:4317
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `OTEL_ENABLED` | `False` | Off: nothing is patched, nothing recorded. |
| `OTEL_BACKEND` | `"grafana"` | Picks the backend and which endpoint setting applies. |
| `OTEL_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint. |
| `OTEL_TEMPO_URL` | `None` | Endpoint for `grafana` / `tempo` (else `OTEL_ENDPOINT`). |
| `OTEL_DATADOG_URL` | `None` | Datadog agent OTLP URL (else `OTEL_ENDPOINT`). |
| `OTEL_SERVICE_NAME` | project name | `service.name`. |
| `OTEL_SAMPLE_RATE` | `1.0` | Fraction of traces kept. |

`volnux serve --role api|worker` and `volnux dev` call `setup_otel()` once the engine
exists; telemetry is normally running by then (started by `initialise_workflows`), and
`setup_otel()` then only returns the function that shuts it down. It also instruments FastAPI (a root span per
HTTP request, except health and metrics). It never raises: a telemetry problem logs
a warning and the server starts without telemetry.

### In code

```python
from volnux.otel import initialize_otel, shutdown_otel

tracer = initialize_otel(
    "my-service",
    service_version="1.4.0",
    environment="production",
    backends=["tempo", "datadog"],
    config={
        "tempo": {"endpoint": "http://tempo:4317"},
        "datadog": {"agent_url": "http://datadog-agent:4317"},
    },
    custom_attributes={"team": "data-platform", "region": "eu-west-1"},
    sample_rate=0.25,
)
...
shutdown_otel()
```

Parameters: `backends` (default `["otlp"]`, names are case-insensitive, duplicates
ignored, an unknown name raises `ValueError` before anything is set up), `config`
(`{backend: {"endpoint": ...}}`, `"agent_url"` for Datadog), `custom_attributes`
(added to the resource), `sample_rate` (validated first, 0.0 to 1.0),
`enable_metrics`, `enable_logging_instrumentation` (put trace ids in log records),
`patch_components` (execution, pipeline, engine and framework parts),
`patch_flow_components`, `debug`.

* Each backend writes to its own endpoint. A backend with no endpoint is skipped
  with a warning and never erases another's. `otlp` and `jaeger` share one exporter
  (the first endpoint configured is used); `grafana` and `tempo` share another.
* Metrics are exported over OTLP to the first backend that has an endpoint, every
  60 s.
* `initialize_otel` while already running returns the running tracer and changes
  nothing. After `shutdown_otel()` it starts afresh with the new configuration.
* `shutdown_otel()` restores every patched method, disconnects the signal hooks,
  removes the trace propagation, flushes and shuts down the tracer and the meter
  provider, and resets the metric collectors. Nothing traces afterwards.

### Backends

| Backend | Configure | Notes |
| --- | --- | --- |
| `otlp` | `config["otlp"]["endpoint"]` | Any OTLP gRPC collector. |
| `jaeger` | `config["jaeger"]["endpoint"]` | Jaeger's OTLP receiver. |
| `tempo` / `grafana` | `config["tempo"]["endpoint"]` | Tempo or Grafana Agent. |
| `datadog` | `config["datadog"]["agent_url"]` | Datadog Agent with OTLP enabled. |

The trace exporter uses an insecure channel. Terminate TLS in a collector next to
the application (the deployment manifests under `deploy/` do).

## Spans

Attributes are bounded and never carry secrets (see [Troubleshooting](#troubleshooting)).
`ok` / `error` is the span status; an event that returns an error result is an error
span.

### The run

```
workflow.<Pipeline>                         SERVER   Pipeline.start
└─ engine.execute                                    DefaultWorkflowEngine.execute
   └─ context.dispatch                               ExecutionContext.dispatch (chained: the next context is a child of the previous)
      ├─ coordinator.execute_async                   ExecutionCoordinator._execute_async
      ├─ context.update_status_async, context.add_result, ...
      └─ flow.run.<Flow>                             SingleFlow / ParallelFlow / StreamFlow .run
         ├─ flow.initialize_event
         ├─ flow.map_events_to_executor
         └─ event.execute.<Event>                    ends when the event's future does
```

| Span | Attributes |
| --- | --- |
| `workflow.<P>` | `workflow.name`, `workflow.force_rerun`, `pipeline.id`, `pipeline.class`, `pipeline.input.<field>` (secret names redacted), and at the end `workflow.status`, `workflow.results_count`, `workflow.errors_count`, `workflow.context_count`, `workflow.total_duration_ms`. Events: `workflow_started`, `workflow_completed` / `_failed` / `_cancelled` / `_aborted`, `workflow_error`. |
| `engine.execute` | `engine.id`, `engine.class`, `engine.strict_mode`, `pipeline.class`, `engine.tasks_processed`, `engine.status`, `engine.error`. |
| `context.dispatch` | `context.id`, `context.is_multitask`, `context.pipeline_id`, `context.task_count`, `context.tasks`, `context.depth`, `context.previous_id`, `context.timeout`, `context.status`, `context.results_count`, `context.errors_count`, `context.duration_ms`. |
| `context.<update_status \| add_error \| add_result \| cancel \| abort \| failed \| save \| spawn_child>` (+ `_async`) | the context's attributes plus `context.current_status`, `context.results_count`, `context.errors_count` (`child.*` for `spawn_child`). |
| `coordinator.execute_async` | `coordinator.timeout`. A timeout reads "Execution timeout". |
| `flow.run.<Flow>` | `flow.type`, `flow.task_count`, `flow.tasks`, `flow.context_id`, `flow.parallel_tasks`. Ends when the future `run()` returned ends. |
| `event.execute.<Event>` | `event.name`, `event.task_id`, `executor.type`, `executor.max_workers`, `event.retry.max_attempts`, `context.id`. |
| `flow.initialize_event` | `event.name`, `task.*`, `event.retry_attempts`, `event.class`. |

Every signal Volnux emits is added as a `signal.<name>` event to the current span
(`pipeline_pre_init`, `pipeline_post_init`, `pipeline_execution_start`,
`pipeline_execution_end`, `pipeline_stop`, `pipeline_shutdown`, `event_init`,
`event_execution_start`, `event_execution_end`, `event_execution_retry`,
`event_execution_retry_done`, `event_execution_failed`, `event_execution_cancelled`,
`event_execution_aborted`, `batch_pipeline_started`, `batch_pipeline_finished`,
`pipeline_metrics_updated`).

### The framework around the run

| Span | Attributes |
| --- | --- |
| `trigger.activate` | `trigger.id`, `trigger.type`, `workflow.name` |
| `trigger.fire` | `workflow.name`, `trigger.type` |
| `trigger_engine.start` / `stop` / `force_stop` | `trigger_engine.triggers`, `trigger_engine.consumers` (`trigger_engine.dropped` for `force_stop`) |
| `workflow.run` | `workflow.name`, `executor.type`, `run.type`, `volnux.correlation_id`, `volnux.execution_id`. Every executor, through `WorkflowExecutor.__call__`. |
| `executor.<AsyncTask \| Celery \| Kubernetes>WorkflowExecutor.execute` | `workflow.name`, `executor.type` |
| `registry.load` | `registry.sources`, `registry.workflows`, `registry.failed_sources` |
| `registry.load_source` | `source.name`, `source.type`, `source.error` |
| `checkpoint.persist`, `checkpoint.build_event` | `checkpoint.snapshot` / `event.class` |
| `rehydrate.event`, `rehydrate.resume`, `rehydrate.node` | `event.class`, `event.phase` / `rehydrate.state_id` |
| `saga.execute`, `saga.resume` | `saga.id`, `saga.name`, `saga.steps`, `saga.status`, `saga.failed_step`, `saga.duration_s` |
| `saga.step`, `saga.compensate`, `saga.dead_letter` | `saga.id`, `saga.step.index`, `saga.step.name` / `saga.failed_step` |
| `saga.recovery.save` / `load` / `delete` | `saga.id` |
| `messaging.<op>` | `messaging.operation`, `messaging.model`, `messaging.timeout`. Ops: `publish`, `push`, `pop`, `pop_many`, `enqueue`, `dequeue`, `queue_delete`, `stream_append`, `stream_read`, `stream_ack`. A blocking pop's span includes its wait. `subscribe` is not traced (it runs until cancelled). |
| `remote.execute_task` | `remote.event`, `remote.task_id`, `remote.state_id`, `remote.mode`, `remote.final_status`, `remote.duration_s` |
| `remote.read_stream` | `remote.stream` |

## Metrics

Counters and histograms are exported over OTLP. All are recorded only when metrics
are enabled and an endpoint is configured.

| Metric | Type | Unit | Attributes |
| --- | --- | --- | --- |
| `workflow.executions`, `workflow.duration` | counter, histogram | 1, ms | `pipeline.name`, `workflow.status`, `environment` |
| `workflow.run.count`, `workflow.run.duration` | counter, histogram | 1, ms | `workflow`, `executor`, `outcome` |
| `trigger.activations` | counter | 1 | `trigger_type`, `outcome` |
| `setup.duration` | histogram | ms | `outcome` |
| `registry.load.duration` | histogram | ms | `outcome` |
| `checkpoint.operations`, `checkpoint.duration` | counter, histogram | 1, ms | `operation`, `outcome` |
| `saga.executions`, `saga.duration` | counter, histogram | 1, ms | `outcome` |
| `messaging.operations`, `messaging.duration` | counter, histogram | 1, ms | `operation`, `model`, `outcome` (duration: no outcome) |
| `flow.duration` | histogram | ms | `flow.type`, `flow.status` (`success` / `error` / `cancelled`) |
| `event.duration` | histogram | ms | `event.name`, `event.status`, `executor.type` |
| `task.count` | counter | 1 | `task.type`, `flow.type` (one per event submission) |
| `error.count` | counter | 1 | `error.type`, `flow.type`, `event.name` |
| `executor.usage` | counter | 1 | `executor.type`, `flow.type` |
| `parallel_tasks.active` | up-down counter | 1 | |
| `engine.queue_length`, `engine.target_workers`, `engine.actual_workers`, `engine.parallel_tasks` | up-down counter (levels) | 1 | `pipeline.name` |
| `resource.cpu_usage` | histogram | cores | `pipeline.name` |
| `resource.memory_usage` | histogram | MB | `pipeline.name` |

The engine numbers are **levels**: the collector remembers each series' last value
and adds the difference (`VolnuxMetricsCollector.record_level`), so recording the
same snapshot twice reads the same level, not double. `VolnuxMetricsRegistry` has
more definitions (`task.duration`, `context.duration`, `retry.count`,
`business.records_processed`, ...) that application code can record with
`get_metrics_collector().record_metric(definition, value, attributes)`.

Attribute values are bounded names (classes, workflows, executors, outcomes). Ids
(execution, saga, correlation) are span attributes only.

## Traces across processes

`volnux.context` puts the context of a run (workflow, execution and correlation
ids, the non-secret configuration) in a carrier that Celery and Kubernetes
executors send to the worker. `initialize_otel` installs `volnux.otel.propagation`,
which adds the active trace to that carrier as W3C `traceparent` / `tracestate`
(`ExecutionContextSnapshot.trace_context`):

```
dispatcher:  workflow.run ── executor.CeleryWorkflowExecutor.execute ── (inject_carrier)
                                                     │  traceparent in the task payload
worker:      volnux_context(snapshot=...)  ──►  the worker's spans are children of the dispatcher's
```

* A function you wrap with `@volnux.context.dispatch_worker_task` continues the
  trace of whoever dispatched it.
* The Kubernetes runner (`volnux-job-runner`) does the same from its payload.
* The worker process needs `initialize_otel` too (set `OTEL_ENABLED` for it), or its
  spans are not recorded; the trace id still travels.
* Without OpenTelemetry the carrier has no `trace_context` and `volnux.context` does
  not import OpenTelemetry (`register_trace_propagation(None)` is the off state).

## Application helpers

```python
from volnux.otel.utils import (
    get_current_trace_id, get_current_span_id, add_span_attribute, add_span_event,
)
```

All of them do nothing, and never raise, when there is no recording span.
`add_span_attribute` stores the value as text.

## How the instrumentation works

* **Patching.** Every part is patched through `patch_attr(owner, name, make_wrapper)`:
  once only (a second call returns `False`), `classmethod` / `staticmethod` kept,
  an inherited method patched on the subclass, and undone by `unpatch_all()`.
  `shutdown_otel()` calls it.
* **Kind is kept.** A coroutine function is wrapped by a coroutine function, so the
  span covers the awaited work. (`Pipeline.start` used to be wrapped by a plain
  function, which broke every start.)
* **No tracer, no cost.** Every wrapper starts by asking `get_tracer()`; before
  `initialize_otel` and after `shutdown_otel` it is `None` and the original runs.
* **Spans that outlive a call.** `run()` and `_submit_event_to_executor` return a
  future; their spans are started with `start_span`, made current only while the
  call runs (so what the flow submits nests under them), and ended by the future's
  done-callback with its outcome (success, error, error *result*, cancelled).
* **Cancellation** (`asyncio.CancelledError`, a `BaseException`) is marked on the span
  as an error; OpenTelemetry's own span handling only covers `Exception`.
* **Telemetry never breaks the traced code.** Attribute builders that fail are
  skipped, a value of an unsupported type becomes text, `None` is dropped,
  exceptions of the wrapped function propagate unchanged.
* **Nested contexts.** `OTelContextManager.set_async_context` / `clear_async_context`
  keep a stack, so finishing an inner dispatch restores the outer one.

## Adding instrumentation

A span around a method of your own component:

```python
from volnux.otel.instrumentations._common import traced
from volnux.otel.instrumentations._patching import patch_attr

wrapper = traced(
    "billing.charge",
    attributes=lambda self, order, *a, **k: {"order.currency": order.currency},
    result_attributes=lambda result: {"charge.ok": result.ok},
    on_finish=lambda args, kwargs, seconds, error: record_my_metric(seconds, error),
)

patch_attr(PaymentGateway, "charge", wrapper)   # once; undone by unpatch_all()
```

`traced(span_name, attributes=, result_attributes=, after=, kind=, on_finish=)` works
on sync and async functions. `attributes(*args, **kwargs)` gets the call's arguments;
`after(args, kwargs, result)` reads the state the call left behind; `on_finish(args,
kwargs, seconds, error)` is for metrics. To make the framework patch it, add a row to
`PATCH_TABLE` in `instrumentations/framework.py` (`(module, class name, attribute,
wrapper)`; `""` for a module-level function). A part whose module cannot be imported
is skipped.

Metrics: add a `MetricDefinition` to `VolnuxMetricsRegistry` and record it with
`get_metrics_collector().record_metric(...)` (a counter or histogram) or
`record_level(...)` (a level).

## Testing

The instrumentation is tested with real spans and metrics held in memory
(`tests/unittests/otel/`). The fixtures in `tests/unittests/otel/conftest.py`:

```python
def test_my_component(otel):                # a tracer + meter provider, in memory
    patch_attr(PaymentGateway, "charge", wrapper)
    gateway.charge(order)

    span = otel.span("billing.charge")      # exactly one span by that name
    assert span.attributes["order.currency"] == "EUR"
    assert otel.total("billing.charges", outcome="success") == 1
```

`otel.spans()`, `otel.names()`, `otel.spans_named(name)`, `otel.points(metric)`,
`otel.total(metric, **attributes)`; `no_otel` is the state before `initialize_otel`.
The fixtures undo every patch afterwards. `tests/unittests/otel/test_end_to_end.py`
runs a real pipeline and checks the hierarchy.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| No spans at all | `OTEL_ENABLED` / `initialize_otel` called? Endpoint reachable (the exporter retries in the background; `debug=True` logs it)? `OTEL_SAMPLE_RATE` not 0? |
| Spans but no metrics | A metrics endpoint is configured for a backend (`config[backend]["endpoint"]`); metrics use the first backend that has one. |
| Worker spans not linked to the API's | Worker has OTel enabled? The executor and worker run the same Volnux version (the payload carries `trace_context`). |
| `Overriding of current TracerProvider is not allowed` | Another provider was installed first; Volnux keeps using its own provider's tracer, so its spans still reach its exporters. |
| Trace ids missing from logs | `enable_logging_instrumentation=True` (the default) and `opentelemetry-instrumentation-logging` installed. |
| Secret in a span | Inputs are redacted by *name* (`password`, `passwd`, `secret`, `token`, `credential`, `authorization`, `api_key`, `apikey`, `private_key`); name a secret field accordingly, or add to `volnux.otel.context_manager.SENSITIVE_NAME_PARTS`. |
| Duplicate spans | Should not happen: patching is idempotent. If you wrapped a method yourself *and* it is in `PATCH_TABLE`, use one of the two. |

### Changes in behaviour

* `Pipeline.start` and `ExecutionContext.cancel_async(propagate=...)` work with the
  instrumentation on (both were broken by their wrappers).
* Concrete flows (`SingleFlow`, `ParallelFlow`, `StreamFlow`) are now instrumented
  (the old code imported classes that do not exist); `flow.run` and `event.execute`
  spans last as long as the work.
* `InstrumentedDefaultWorkflowEngine` / `instrument_engine` are gone (they could not
  work against the real engine); `engine.execute` is a normal patched method.
* Pipeline inputs with secret-looking names are redacted, and attribute values are
  cut to 256 characters.
* `memory_usage_gb` in an engine snapshot is converted to the instrument's MB;
  engine queue and worker numbers are levels; `flow.task_count` is no longer a
  metric attribute.
