class TestTimestampsAreStampedPerInstance:
    """Regression: a ``default_factory`` is evaluated once, so every response and
    entry used to carry the moment the module was imported."""

    def test_responses_get_their_own_time(self):
        import time

        from volnux.mixins.event.communication.datastructures import (
            ExternalCommunicationResponse,
        )

        first = ExternalCommunicationResponse(request_id="1", decision="x")
        time.sleep(0.01)
        second = ExternalCommunicationResponse(request_id="2", decision="x")
        assert first.responded_at < second.responded_at

    def test_timeout_response_is_stamped_now(self):
        import time
        from datetime import datetime, timezone

        from volnux.mixins.event.communication.datastructures import (
            ExternalCommunicationResponse,
        )

        time.sleep(0.01)
        stamp = datetime.fromisoformat(
            ExternalCommunicationResponse.timeout("r").responded_at
        )
        assert (datetime.now(timezone.utc) - stamp).total_seconds() < 5

    def test_explicit_value_is_kept(self):
        from volnux.mixins.event.communication.datastructures import (
            ExternalCommunicationResponse,
        )

        stamp = "2020-01-01T00:00:00+00:00"
        r = ExternalCommunicationResponse(
            request_id="1", decision="x", responded_at=stamp
        )
        assert r.responded_at == stamp

    def test_wire_round_trip_keeps_the_original_time(self):
        from volnux.mixins.event.communication.datastructures import (
            ExternalCommunicationResponse,
        )

        a = ExternalCommunicationResponse(request_id="1", decision="x")
        b = ExternalCommunicationResponse(request_id="2", decision="y")
        b.set_state(a.get_state())
        assert b.responded_at == a.responded_at

    def test_queue_entries_get_their_own_time(self):
        import time

        from volnux.mixins.event.communication.datastructures import (
            ExternalCommunicationQueueEntry,
        )

        fields = dict(
            request_id="1",
            workflow_name="w",
            workflow_id="w",
            task_id="t",
            request_type="hitl",
            checkpoint_key="c",
            context_id="c",
            request_title="t",
        )
        first = ExternalCommunicationQueueEntry(**fields)
        time.sleep(0.01)
        second = ExternalCommunicationQueueEntry(**fields)
        assert first.enqueued_at < second.enqueued_at
