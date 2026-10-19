# EventBase + Mixins — pytest Suite

This folder contains the pytest suite for `EventBase` and its five mixins
(`RetryMixin`, `ExecutorInitializerMixin`, `EventCheckpointingMixin`,
`EventCommandMixin`, `ExternalCommunicationMixin`).

## Layout

```
event/
├── conftest.py                    # Scoped patches: sys.path, VolnuxConfig stub, registry isolation, fixtures
├── pytest.ini                     # asyncio_mode = auto, testpaths
├── __init__.py
├── README.md                      # this file
└── tests/
    ├── __init__.py
    ├── test_enums.py              # EventType, EventCategory
    ├── test_event_meta.py         # EventMeta metaclass registration behaviour
    ├── test_event_base.py         # EventBase lifecycle (init, goto, evaluator, on_success/on_failure, __call__)
    ├── test_stop_condition_processor.py
    └── test_mixins/
        ├── __init__.py
        ├── test_retry_mixin.py
        ├── test_executor_initializer_mixin.py
        ├── test_checkpointing_mixin.py
        ├── test_command_mixin.py
        └── test_external_comm_mixin.py
```

## Setup — environment variables

The conftest reads two env vars. Set them before running:

```bash
# Required: path to the directory that CONTAINS the `volnux/` package
# (i.e. the repo root, not the package itself).
export VOLNUX_REPO_ROOT=/path/to/volnux/repo

# Optional: dotted module path where `EventBase` lives.
# Default: volnux.event.base
# Override if your repo puts EventBase in a different module
# (e.g. `volnux.event` if it's in the package __init__).
export VOLNUX_EVENT_BASE_MODULE=volnux.event.base
```

## Install test dependencies

```bash
pip install pytest pytest-asyncio
```

## Running the suite

From this folder:

```bash
# Run everything
pytest -v

# Run a single file
pytest tests/test_event_base.py -v

# Run a single test class
pytest tests/test_event_base.py::TestGoto -v

# Run a single test
pytest tests/test_event_base.py::TestGoto::test_invalid_descriptor_negative_raises_value_error -v

# Run with logging visible
pytest -v --log-cli-level=DEBUG

# Skip slow tests (sub_phase timeout tests, anything tagged `slow`)
pytest -v -m "not slow"
```

## What the conftest does (and why it's localised)

The `event/conftest.py` performs three patches that **only affect tests
collected under `event/`**:

1. **`sys.path` manipulation** — inserts `VOLNUX_REPO_ROOT` so `volnux.*`
   becomes importable.
2. **`VolnuxConfig.get_instance()` stub** — installed **before** any
   `volnux.*` module is imported, so the module-level `conf =
   VolnuxConfig.get_instance()` calls in `volnux.mixins.event` and
   `volnux.mixins._phase_decorator` (and the `RetryPolicy` dataclass
   defaults) all see a deterministic stub. The stub's `.get(key, default)`
   always returns `default`. Tests that need to flip a flag
   (e.g. `CHECKPOINT_REQUIRED`) can use the `volnux_config_stub` fixture.

3. **Event registry isolation** — `EventMeta` auto-registers every
   concrete `EventBase` subclass. The autouse fixture
   `_isolate_event_registry` snapshots the registry's `_classes` set and
   `_records` dict before each test and removes any new entries on
   teardown, so throw-away event classes defined inside tests don't leak.

Because these patches live in `event/conftest.py`, they only apply to tests
collected under the `event/` folder. Other test suites in the repo are
unaffected.

## Fixtures provided by the conftest

| Fixture | Description |
|---|---|
| `dummy_event_cls` | A minimal concrete `EventBase` subclass with a no-op `process` method. |
| `execution_context` | A `MagicMock` standing in for `ExecutionContext`. |
| `checkpoint_manager` | A `MagicMock` standing in for `VolnuxCheckPointManager`. |
| `command_channel` | A `MagicMock` standing in for `CommandChannelBase` with async `send_message` and `receive_command` stubs. |
| `make_event` | Factory that builds a fully-initialised `dummy_event_cls` instance. |
| `event` | A ready-to-use event instance (built via `make_event` with the `checkpoint_manager` fixture). |
| `volnux_config_stub` | The stub `VolnuxConfig` instance — call `.set("KEY", value)` to install overrides. |
| `make_step` | Factory that builds a step function decorated with a `_phase` attribute for `steps_runner` tests. |

## Important: import-path assumptions

I had to guess several module paths. If any of the following imports fail
in your env, edit the corresponding test file (single-line change at the
top):

| Test file | Guessed import path | What to edit |
|---|---|---|
| `tests/test_event_meta.py` | `volnux.event.base` (or `volnux.event`) | `_import_meta()` |
| `tests/test_event_base.py` | `volnux.event.base` (or `volnux.event`) | `_import_event_base()` |
| `tests/test_stop_condition_processor.py` | `volnux.mixins._checkpointing` | `_import_processor()` |
| `tests/test_mixins/test_checkpointing_mixin.py` | `volnux.mixins._checkpointing` | `_import_mixin()` |
| `tests/test_mixins/test_external_comm_mixin.py` | `volnux.mixins._external_communication` | `_import_mixin_and_types()` |
| All `volnux.mixins._checkpointing.*` patch targets in tests | `volnux.mixins._checkpointing` | `patch("volnux.mixins._checkpointing.X", ...)` |

The retry mixin (`volnux.mixins.event`) and the executor initializer mixin
(`volnux.mixins.event`) are confirmed by the base module's imports, so those
paths are correct. The external-comm and checkpointing mixins were pasted
as standalone files without a confirmed package path — `_checkpointing` and
`_external_communication` are best-guesses based on the file-naming pattern
in the repo.

To fix an import path: edit the relevant `_import_*` helper at the top of
the test file. The rest of the test file imports from that helper, so a
single-line change propagates everywhere.

For `patch("volnux.mixins._checkpointing.SnapshotBuilder", ...)` style
patches: if the module path differs, do a project-wide find/replace on the
patch target string (e.g. `volnux.mixins._checkpointing` →
`volnux.mixins.checkpointing`).

## What's covered

- **Enums** — `EventType` (3 members), `EventCategory` (18+ members, str mixin,
  frozenset membership).
- **`EventMeta`** — base-class skip rule, concrete-subclass auto-registration,
  bare-`EventCategory` normalisation, non-frozenset / non-`EventCategory`
  rejection via `ImproperlyConfigured`, `RuntimeError` swallow path.
- **`EventBase`** — `__init_subclass__` `__init__` override rejection,
  `__init__` delegation to `_setup_event` (all attributes verified),
  `__repr__`, `get_init_args`/`get_call_args`, `get_version_*`
  classmethods (handler mocked), `goto` (descriptor validation, success/
  failure/raw paths, `SwitchTask` payload), `evaluator` (returns
  `EventEvaluator`, raises on `None`/wrong type), `bypass`/`process`/
  `cleanup` defaults, `event_result` shape, `on_success`/`on_failure`
  (signal emission, `StopProcessingError` on stop-condition, `MaxRetryError`
  unwrapping), `__call__` (steps_runner invocation, command-listener task
  lifecycle, resource_monitor.stop() in finally, `_completed` fallback when
  phase != COMPLETED, exception swallowing in `_completed`).
- **`StopConditionProcessor`** — every `StopCondition` member,
  list-of-conditions any() semantics, `on_success`/`on_error` exception
  toggling, `reset`, `get_status`.
- **`RetryMixin`** — `RetryPolicy` defaults, `init_retry` dict→policy coercion,
  `config_retry_policy` (single/list/tuple exception handling),
  `get_backoff_time` (formula, cap), `_sleep_for_backoff` (sleep + return
  value), `is_retryable` (None policy, empty list, matching/non-matching,
  subclass), `is_exhausted`, `_retry` (no-policy call-once, first-attempt
  success, retryable-then-success, exhaustion→`MaxRetryError`,
  non-retryable propagate, `MaxRetryError` break-out, signal emissions,
  args/kwargs forwarding).
- **`ExecutorInitializerMixin`** — `get_task_executor` (instance passthrough
  vs registry lookup), `get_executor_initializer_config` (existing, dict
  coercion, default construction, caching), `get_executor_context` (None
  for non-class, mp_context inclusion, kwargs introspection, caller-ctx
  merge + override).
- **`EventCheckpointingMixin`** — `_process_result_tuple` validation,
  `_get_steps` ordering + `_phase` + `_is_step` attributes, `_process_wrapper`
  (async/sync dispatch, None→EmptyResultError, invalid→ValueError, EventResult
  unwrap, args/kwargs forwarding), `enqueue_checkpoint` (None manager,
  required-raises, enqueue with snapshot, error swallow + reraise),
  `create_snapshot` (SnapshotBuilder delegation), `_process` (no-retry path,
  retry path, MaxRetryError handling, generic exception, SwitchTask
  propagation), `_pre_process` (NotImplementedError silent, None→EmptyResult,
  valid tuple→SkipExecutionError, invalid→ValueError, coroutine awaiting,
  unhandled propagation), `_post_process` (success/failure routing),
  `_completed` (cleanup hook, swallow, resource iteration, provider cleanup
  failure swallow), `acquire_resource` (existing restore, init_func path,
  coroutine unwrap, provider-path restore + monitor start), `sub_phase`
  (cache hit, first execution, async init_func, suspend_on raising +
  caching, re-suspend on resume, deserialise transform, timeout, exception
  not cached), `_register_resource` (string path, class path, non-Provider
  TypeError, missing save_state/restore_state ValueError, non-dict save_state,
  async save_state), `_restore_resource` (missing provider_path, success
  path, bind=True attribute set, async restore, ImportError swallowed),
  `get_phase`.
- **`EventCommandMixin`** — `_send_update` (no-channel noop, payload shape,
  extra merge, send-error swallow), `_command_listener` (no-channel return,
  command dispatch, TimeoutError swallow, CancelledError propagate, generic
  exception swallow), `_handle_command` (PAUSE, RESUME, CANCEL,
  UPDATE_PRIORITY with/without payload, CHECKPOINT, QUERY_STATUS gate-
  dependent, unknown command warning, handler-exception swallow), `_run_step`
  (async awaited, sync to_thread offload, args/kwargs forwarding, exception
  propagate), `steps_runner` (happy path, step failure, pause gate blocking,
  phase skip, no-phase step execution, preemption→SuspendTask, hard
  cancel→CancelledError, no double-report).
- **`ExternalCommunicationMixin`** — `communicate` default no-op,
  `wait_for_condition`/`wait_for_event`/`request_human_input` (phase guard
  for non-COMMUNICATING, enqueue_checkpoint before suspension,
  `ExternalCommunicationSuspensionRequest` payload shape with correct
  `request_type`, event_type/event_filter propagation, payload/options
  propagation, defaults for optional args, unique UUID `request_id`).

## Known limitations / things not covered

1. **Real `VolnuxConfig`** is fully mocked — if your tests need to verify
   behaviour with specific config values, use the `volnux_config_stub`
   fixture to install overrides per-test.
2. **Real `VersionHandler`** is exercised end-to-end (not mocked) for the
   metaclass registration path. If `VersionHandler.from_class` has its
   own dependencies (e.g. needs the real `volnux.config` backend), the
   conftest stub will satisfy `.get(key, default)` calls but won't provide
   real version data. You may want to mock `VersionHandler.from_class`
   in `test_event_meta.py` if the real handler requires infrastructure.
3. **`sub_phase` timeout tests** use very short timeouts (0.05s) —
   considered fast enough to skip the `slow` marker.
4. **Thread offload in `_run_step`** — the test verifies the step runs in a
   non-main thread but does not assert the specific thread pool used.
5. **`steps_runner` pause-gate test** uses a 0.05s delay then sets the
   gate — if the test environment is heavily loaded, this could
   occasionally flake. Bump the timeout if needed.

## Troubleshooting

### "Could not import EventBase from any of [...]"

The conftest tried `volnux.event.base`, `volnux.event`, and `volnux.core.event`
and failed. Either:
- `VOLNUX_REPO_ROOT` is wrong (verify `ls $VOLNUX_REPO_ROOT/volnux/__init__.py`),
- Or `EventBase` lives at a different module path (set `VOLNUX_EVENT_BASE_MODULE`
  to the correct dotted path).

### "Could not import EventCheckpointingMixin" or similar

Each test file that imports a mixin has its own `_import_*` helper listing
candidate module paths. Edit the helper to include your actual module path.

### Tests hang on `steps_runner` pause-gate test

The `test_pause_gate_blocks_until_set` test schedules a `_resume_after_delay`
task that sets the gate after 0.05s. If the event loop is heavily loaded,
this can take longer than expected. The test has a 1.0s timeout — increase
if your env is slow.

### `ImportError: cannot import name 'X' from 'volnux.mixins.event'`

This means one of the mixins isn't exported from `volnux.mixins.event`'s
`__init__`. Either add it there, or edit the failing test file to import
from the correct submodule.

### Tests pass when run individually but fail when run together

This usually means the event registry isn't being properly isolated. The
conftest's `_isolate_event_registry` fixture relies on the registry having
`_classes` and `_records` attributes — if your `Registry` implementation
uses different attribute names, the isolation won't work and tests will
pollute each other. Inspect the real `Registry` class and update the
fixture.
