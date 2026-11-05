"""Governance events are scoped by PROJECT_ID, so an unset one is an error rather
than the text "None" in every key and event."""

import pytest

from volnux.conf import override_settings
from volnux.exceptions import ImproperlyConfigured
from volnux.governance.events import GovernanceEvent, required_project_id


def test_the_configured_value():
    with override_settings(PROJECT_ID="acme"):
        assert required_project_id() == "acme"


@pytest.mark.parametrize("value", [None, ""])
def test_an_unset_project_id_is_an_error_that_says_how_to_set_it(value):
    with override_settings(PROJECT_ID=value):
        with pytest.raises(ImproperlyConfigured, match="VOLNUX_PROJECT_ID"):
            required_project_id()


def test_a_variable_set_after_the_configuration_was_built_counts(monkeypatch):
    # (`volnux dev` and process managers export it after the config exists)
    monkeypatch.setenv("VOLNUX_PROJECT_ID", "set-late")
    assert required_project_id() == "set-late"


def test_a_number_is_text():
    with override_settings(PROJECT_ID=42):
        assert required_project_id() == "42"


def test_the_route_cannot_be_resolved_without_it():
    route = GovernanceEvent.get_storage_route()
    with override_settings(PROJECT_ID=None):
        with pytest.raises(ImproperlyConfigured):
            route.routing_keys()
    with override_settings(PROJECT_ID="acme"):
        assert route.routing_keys()["project_id"] == "acme"


def test_the_reporter_stamps_the_configured_project_on_each_event(context):
    from volnux.governance.reporter import translate_execution

    with override_settings(PROJECT_ID="acme"):
        event = translate_execution("execution.completed", execution_context=context)
    assert event.project_id == "acme"


def test_the_reporter_does_not_invent_a_project(context):
    # the model's own default is fixed when the class is created; the reporter
    # resolves the project for every event it builds
    from volnux.governance.reporter import translate_execution

    with override_settings(PROJECT_ID=None):
        with pytest.raises(ImproperlyConfigured, match="PROJECT_ID"):
            translate_execution("execution.completed", execution_context=context)
