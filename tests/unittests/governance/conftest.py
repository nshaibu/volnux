"""Shared fixtures for the governance tests.

Nothing here needs a provisioned messaging backend, a live engine or Redis: the
reporter reads plain attributes off whatever the signals hand it, so lightweight
stand-ins are enough to exercise every translator, and the publisher's sink is
injectable so delivery can be observed without a transport.
"""

import os

# ``GovernanceEvent.project_id`` resolves PROJECT_ID when the model class is
# created — that is, at import of ``volnux.governance.events``. A real
# deployment supplies it from the project's settings; the tests supply it here,
# before any import below pulls the module in.
os.environ.setdefault("VOLNUX_PROJECT_ID", "test-project")

import pytest  # noqa: E402

from volnux.config import VolnuxConfig  # noqa: E402
from volnux.governance.events import EventType, GovernanceEvent  # noqa: E402
from volnux.governance.publisher import GovernanceEventPublisher  # noqa: E402

_config = VolnuxConfig.get_instance()
try:
    _config.get("PROJECT_ID")
except AttributeError:  # pragma: no cover - only if the singleton predates us
    _config.add("PROJECT_ID", os.environ["VOLNUX_PROJECT_ID"])


# --- Stand-ins for the live engine objects the signals carry ----------------


class FakePipeline:
    """What ``pipeline_execution_start`` hands the reporter."""

    def __init__(self, pid="run-1", workflow_id="wf-1", workflow_name="Customer ETL"):
        self.id = pid
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name


class FakeContext:
    """What every other wired signal hands the reporter."""

    def __init__(self, pipeline=None, workflow_id="wf-1", workflow_name="Customer ETL"):
        self.pipeline = pipeline if pipeline is not None else FakePipeline()
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name


class FakeTask:
    """A task/event node within a run."""

    def __init__(self, name="ExtractCustomerData", id=None):
        self.name = name
        self.id = id


class FakeSuspensionRequest:
    """The external-communication suspension request behind a HITL pause."""

    def __init__(self, **overrides):
        self.task_id = "ApproveTransfer"
        self.request_id = "req-77"
        self.title = "Approve $47,500 transfer?"
        self.description = "Needs a human"
        self.options = ["approve", "reject"]
        self.timeout_hours = 24
        for key, value in overrides.items():
            setattr(self, key, value)


# --- Test doubles -----------------------------------------------------------


class RecordingPublisher(GovernanceEventPublisher):
    """A publisher that records submissions instead of starting a thread."""

    def __init__(self):
        super().__init__(sink=None)
        self.sent = []

    def start(self):
        self._started = True

    def stop(self, *, timeout=5.0):
        self._started = False

    def submit(self, event):
        self.sent.append(event)


class ExplodingPublisher(RecordingPublisher):
    """A publisher whose submit always fails."""

    def submit(self, event):
        raise RuntimeError("publisher is down")


class FakeSignal:
    """Records connect/disconnect the way ``SoftSignal`` would be driven."""

    def __init__(self, name):
        self.name = name
        self.listeners = []

    def connect(self, sender, listener):
        self.listeners.append(listener)

    def disconnect(self, sender, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_installed_reporters():
    """Drop any reporter left installed, so the registry cannot leak sideways.

    ``install`` adds to a module-level strong-reference list that otherwise
    lives for the whole session.
    """
    yield
    from volnux.governance import reporter as reporter_module

    reporter_module._installed_reporters.clear()


@pytest.fixture
def pipeline():
    return FakePipeline()


@pytest.fixture
def context():
    return FakeContext()


@pytest.fixture
def task():
    return FakeTask()


@pytest.fixture
def publisher():
    return RecordingPublisher()


@pytest.fixture
def make_event():
    """Build a fully-populated event; override any field via keyword."""

    def _make(**overrides):
        fields = dict(
            event_type=EventType.TASK_COMPLETED,
            event_id="evt-1",
            occurred_at=1_700_000_000.5,
            execution_id="run-1",
            task_id="ExtractCustomerData",
            workflow_id="wf-1",
            workflow_name="Customer ETL",
            sequence=3,
            payload={"status": "completed"},
        )
        fields.update(overrides)
        return GovernanceEvent(**fields)

    return _make
