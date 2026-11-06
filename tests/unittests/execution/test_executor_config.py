"""ExecutorInitializerConfig merging (event-level config <- pointy options)."""

import pytest

from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.typing import ConfigState


def test_update_keeps_values_the_options_did_not_set():
    """Option defaults (timeout=30, use_encryption=False...) used to overwrite
    what the event configured, because they count as 'configured'."""
    event_level = ExecutorInitializerConfig(
        timeout=120, use_encryption=True, client_cert_path="/certs/c.pem"
    )
    merged = event_level.update(ExecutorInitializerConfig(max_workers=8))

    assert merged.timeout == 120
    assert merged.use_encryption is True
    assert merged.client_cert_path == "/certs/c.pem"
    assert merged.max_workers == 8


def test_update_applies_values_set_only_on_the_options_side():
    """Fields the event left UNSET used to be dropped from the merge."""
    merged = ExecutorInitializerConfig().update(
        ExecutorInitializerConfig(host="worker.internal", port=9000)
    )
    assert (merged.host, merged.port) == ("worker.internal", 9000)


def test_options_override_the_event_when_they_set_a_value():
    merged = ExecutorInitializerConfig(max_workers=2, timeout=120).update(
        ExecutorInitializerConfig(max_workers=16, timeout=5)
    )
    assert (merged.max_workers, merged.timeout) == (16, 5)


def test_a_merged_config_serializes_like_any_other():
    """No hidden bookkeeping attributes: snapshots serialise the instance dict."""
    merged = ExecutorInitializerConfig(timeout=120).update(
        ExecutorInitializerConfig(max_workers=8)
    )
    assert set(vars(merged)) == set(vars(ExecutorInitializerConfig()))


def test_unset_fields_stay_unset_after_a_merge():
    merged = ExecutorInitializerConfig().update(ExecutorInitializerConfig())
    assert merged.thread_name_prefix is ConfigState.UNSET
    assert merged.max_tasks_per_child is ConfigState.UNSET


def test_update_does_not_modify_either_operand():
    a = ExecutorInitializerConfig(timeout=120)
    b = ExecutorInitializerConfig(max_workers=3)
    a.update(b)
    assert a.timeout == 120 and a.max_workers is ConfigState.UNSET
    assert b.max_workers == 3


def test_update_rejects_other_types():
    with pytest.raises(TypeError):
        ExecutorInitializerConfig().update({"timeout": 1})  # type: ignore[arg-type]
