import asyncio
from datetime import timedelta

import pytest

from tests.unittests.support import SerializingMemoryBackend
from volnux.asset import (
    AssetKey,
    AssetMaterialisation,
    AssetTriggerCondition,
    FreshnessPolicy,
    get_asset_catalog,
    set_asset_catalog,
)
from volnux.asset.catalog import AssetCatalog
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import AssetTrigger, TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerType

RAW = AssetKey("raw_orders")


@pytest.fixture
def catalog(monkeypatch):
    backend = SerializingMemoryBackend()
    monkeypatch.setattr(AssetMaterialisation, "_backend_store", backend, raising=False)
    monkeypatch.setattr(
        AssetMaterialisation, "_backend_initialized", True, raising=False
    )
    yield AssetCatalog()
    backend.close()


async def produce(catalog, key=RAW, version="1", **upstream):
    return await catalog.record_materialisation(
        key,
        asset_version=version,
        producing_event="E",
        producing_event_version="1",
        workflow_id="w",
        execution_id="x",
        task_id="t",
        upstream_versions=upstream,
    )


def make(catalog, **kw):
    kw.setdefault("watch_key", RAW)
    t = AssetTrigger("wf", catalog, **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


async def settle(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("not reached in time")
        await asyncio.sleep(0.005)


class TestConstruction:
    def test_type_and_defaults(self, catalog):
        t = AssetTrigger("wf", catalog, RAW)
        assert t.trigger_type is TriggerType.ASSET
        assert t.watch_key == RAW and t.trigger_on is AssetTriggerCondition.MATERIALISED
        assert t.poll_interval == 60.0

    def test_watch_key_is_required(self, catalog):
        with pytest.raises(ValueError, match="watch_key"):
            AssetTrigger("wf", catalog)

    def test_watch_key_may_be_a_string(self, catalog):
        t = AssetTrigger("wf", catalog, "finance/orders")
        assert t.watch_key == AssetKey("orders", "finance")

    @pytest.mark.parametrize("bad", [5, ["x"], object()])
    def test_watch_key_type_is_checked(self, catalog, bad):
        with pytest.raises(TypeError, match="watch_key"):
            AssetTrigger("wf", catalog, bad)

    def test_trigger_on_accepts_the_enum_or_its_value(self, catalog):
        assert (
            AssetTrigger("wf", catalog, RAW, trigger_on="stale").trigger_on
            is AssetTriggerCondition.STALE
        )
        assert (
            AssetTrigger(
                "wf", catalog, RAW, trigger_on=AssetTriggerCondition.STALE
            ).trigger_on
            is AssetTriggerCondition.STALE
        )

    def test_unknown_condition_is_refused(self, catalog):
        with pytest.raises(ValueError, match="trigger_on"):
            AssetTrigger("wf", catalog, RAW, trigger_on="exploded")

    @pytest.mark.parametrize("bad", [0, -1])
    def test_poll_interval_must_be_positive(self, catalog, bad):
        with pytest.raises(ValueError, match="poll_interval"):
            AssetTrigger("wf", catalog, RAW, poll_interval=bad)

    def test_defaults_to_the_global_catalog(self):
        shared = AssetCatalog()
        previous = get_asset_catalog()
        set_asset_catalog(shared)
        try:
            assert AssetTrigger("wf", watch_key=RAW)._catalog is shared
        finally:
            set_asset_catalog(previous)


@pytest.mark.asyncio
class TestMaterialised:
    async def test_fires_when_the_asset_is_produced(self, store, catalog):
        t, got = make(catalog)
        await t.start()
        mat = await produce(catalog, version="7", **{"upstream": "3"})
        await settle(lambda: got)
        params = got[0].workflow_params
        assert params["asset_key"] == "raw_orders"
        assert params["materialisation_id"] == mat.id
        assert params["materialised_at"] == mat.created_at
        assert params["upstream_versions"] == {"upstream": "3"}
        assert got[0].activation_source is TriggerType.ASSET
        await t.stop()

    async def test_only_the_watched_asset_matters(self, store, catalog):
        t, got = make(catalog)
        await t.start()
        await produce(catalog, AssetKey("other"))
        await produce(catalog, AssetKey("raw_orders", "finance"))
        assert got == []
        await t.stop()

    async def test_each_materialisation_fires_once(self, store, catalog):
        t, got = make(catalog)
        await t.start()
        for version in "123":
            await produce(catalog, version=version)
        assert len(got) == 3 and t.fire_count == 3
        await t.stop()

    async def test_the_same_materialisation_is_not_processed_twice(
        self, store, catalog
    ):
        t, got = make(catalog)
        mat = await produce(catalog)  # recorded before the trigger listened
        await t._on_materialised(mat)
        await t._on_materialised(mat)
        assert len(got) == 1

    async def test_dedup_forgets_the_oldest_ids_not_the_newest(self, store, catalog):
        t, got = make(catalog)
        t._MAX_PROCESSED = 3
        mats = [await produce(catalog, version=str(i)) for i in range(5)]
        for mat in mats:
            await t._on_materialised(mat)
        assert list(t._processed_materialisations) == [m.id for m in mats[-3:]]
        # the newest are still remembered, so a repeat of one is still ignored
        await t._on_materialised(mats[-1])
        assert len(got) == 5

    async def test_disabled_trigger_does_not_fire(self, store, catalog):
        t, got = make(catalog, enabled=False)
        await t.start()
        await produce(catalog)
        assert got == []
        await t.stop()

    async def test_activation_failure_does_not_break_the_catalog(self, store, catalog):
        t = AssetTrigger("wf", catalog, RAW)  # no callback: activate() raises
        await t.start()
        mat = await produce(catalog)  # the catalog logs the subscriber's error
        assert mat is not None and t.error_count == 1
        await t.stop()


@pytest.mark.asyncio
class TestSubscription:
    async def test_start_is_idempotent(self, store, catalog):
        t, got = make(catalog)
        await t.start()
        await t.start()
        assert len(catalog._subscribers[RAW]) == 1
        await produce(catalog)
        assert len(got) == 1
        await t.stop()

    async def test_stop_really_unsubscribes(self, store, catalog):
        # a bound method is a new object on every access: removal must not rely on
        # identity or the subscription would leak
        t, got = make(catalog)
        await t.start()
        await t.stop()
        assert RAW not in catalog._subscribers
        await produce(catalog)
        assert got == []

    async def test_stop_before_start_and_twice_is_safe(self, store, catalog):
        t, _ = make(catalog)
        await t.stop()
        await t.start()
        await t.stop()
        await t.stop()

    async def test_restart_does_not_double_fire(self, store, catalog):
        t, got = make(catalog)
        for _ in range(3):
            await t.start()
            await t.stop()
        await t.start()
        await produce(catalog)
        assert len(got) == 1
        await t.stop()

    async def test_two_triggers_on_one_asset_both_fire(self, store, catalog):
        (a, got_a), (b, got_b) = make(catalog), make(catalog)
        await a.start()
        await b.start()
        await produce(catalog)
        assert len(got_a) == 1 and len(got_b) == 1
        await a.stop()
        await produce(catalog, version="2")
        assert len(got_a) == 1 and len(got_b) == 2
        await b.stop()

    async def test_run_and_end_update_the_record(self, store, catalog):
        t, _ = make(catalog)
        await t.run()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await t.end()
        assert RAW not in catalog._subscribers
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )


@pytest.mark.asyncio
class TestCatalogSubscriptions:
    async def test_subscribe_is_idempotent_for_bound_methods(self, catalog):
        class Listener:
            async def on(self, mat):
                pass

        listener = Listener()
        await catalog.subscribe(RAW, listener.on)
        await catalog.subscribe(RAW, listener.on)
        assert len(catalog._subscribers[RAW]) == 1
        await catalog.unsubscribe(RAW, listener.on)
        assert RAW not in catalog._subscribers

    async def test_a_callback_may_unsubscribe_while_being_notified(
        self, store, catalog
    ):
        calls = []

        async def once(mat):
            calls.append("once")
            await catalog.unsubscribe(RAW, once)

        async def always(mat):
            calls.append("always")

        await catalog.subscribe(RAW, once)
        await catalog.subscribe(RAW, always)
        await produce(catalog)
        await produce(catalog, version="2")
        assert calls == ["once", "always", "always"]

    async def test_a_failing_subscriber_does_not_block_the_others(self, store, catalog):
        seen = []

        async def bad(mat):
            raise RuntimeError("boom")

        async def good(mat):
            seen.append(mat.id)

        await catalog.subscribe(RAW, bad)
        await catalog.subscribe(RAW, good)
        mat = await produce(catalog)
        assert seen == [mat.id]


async def declare(catalog, policy=None, upstream=None):
    await catalog.register(
        RAW,
        producing_event="E",
        freshness_policy=policy,
        upstream_keys=upstream,
    )


@pytest.mark.asyncio
class TestStale:
    def stale_trigger(self, catalog, **kw):
        return make(
            catalog, trigger_on=AssetTriggerCondition.STALE, poll_interval=0.02, **kw
        )

    async def test_never_materialised_counts_as_stale_and_fires_once(
        self, store, catalog
    ):
        await declare(catalog)
        t, got = self.stale_trigger(catalog)
        await t.start()
        await settle(lambda: got)
        await asyncio.sleep(0.15)  # several polls later: still just the one
        assert len(got) == 1
        assert got[0].workflow_params["reason"] == "freshness_policy_exceeded"
        assert got[0].workflow_params["asset_key"] == "raw_orders"
        await t.stop()

    async def test_a_fresh_asset_does_not_fire(self, store, catalog):
        await declare(catalog, FreshnessPolicy(maximum_lag=timedelta(hours=1)))
        await produce(catalog)
        t, got = self.stale_trigger(catalog)
        await t.start()
        await asyncio.sleep(0.15)
        assert got == []
        await t.stop()

    async def test_it_fires_again_only_after_the_asset_was_fresh_in_between(
        self, store, catalog
    ):
        await declare(catalog, FreshnessPolicy(maximum_lag=timedelta(hours=1)))
        mat = await produce(catalog)
        t, got = make(catalog, trigger_on="stale", poll_interval=1)

        assert await t.check_staleness() is False
        # age the asset past its policy
        catalog.get_declaration(RAW).freshness_policy = FreshnessPolicy(
            maximum_lag=timedelta(seconds=-1)
        )
        assert await t.check_staleness() is True
        assert await t.check_staleness() is True
        assert len(got) == 1  # one episode, one activation

        catalog.get_declaration(RAW).freshness_policy = FreshnessPolicy(
            maximum_lag=timedelta(hours=1)
        )
        assert await t.check_staleness() is False  # rematerialised: fresh again
        catalog.get_declaration(RAW).freshness_policy = FreshnessPolicy(
            maximum_lag=timedelta(seconds=-1)
        )
        assert await t.check_staleness() is True
        assert len(got) == 2

    async def test_stale_because_an_upstream_changed(self, store, catalog):
        upstream = AssetKey("upstream")
        await catalog.register(upstream, producing_event="U")
        await declare(catalog, upstream=[upstream])
        await produce(catalog)
        t, got = make(catalog, trigger_on="stale", poll_interval=1)
        assert await t.check_staleness() is False
        await asyncio.sleep(0.01)
        await produce(catalog, upstream)  # newer than the watched asset
        assert await t.check_staleness() is True
        assert len(got) == 1

    async def test_an_unregistered_asset_is_never_stale(self, store, catalog):
        t, got = make(catalog, trigger_on="stale", poll_interval=1)
        assert await t.check_staleness() is False
        assert got == []

    async def test_a_disabled_trigger_reports_the_episode_once_enabled(
        self, store, catalog
    ):
        await declare(catalog)
        t, got = make(catalog, trigger_on="stale", poll_interval=1, enabled=False)
        assert await t.check_staleness() is True
        assert got == [] and t._stale_reported is False
        t.state.enabled = True
        await t.check_staleness()
        assert len(got) == 1

    async def test_a_catalog_error_does_not_stop_the_polling(
        self, store, catalog, monkeypatch
    ):
        await declare(catalog)
        t, got = self.stale_trigger(catalog)
        calls = []
        real = catalog.is_stale

        async def flaky(key):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("store down")
            return await real(key)

        monkeypatch.setattr(catalog, "is_stale", flaky)
        await t.start()
        await settle(lambda: got)
        assert len(calls) >= 2
        await t.stop()

    async def test_start_is_idempotent_and_stop_ends_the_poll(self, store, catalog):
        await declare(catalog)
        t, got = self.stale_trigger(catalog)
        await t.start()
        task = t._stale_task
        await t.start()
        assert t._stale_task is task
        await t.stop()
        assert task.done() and t._stale_task is None
        await t.stop()

    async def test_restart_reports_a_still_stale_asset_again(self, store, catalog):
        await declare(catalog)
        t, got = self.stale_trigger(catalog)
        await t.start()
        await settle(lambda: len(got) == 1)
        await t.stop()
        await t.start()
        await settle(lambda: len(got) == 2)
        await t.stop()

    async def test_a_stale_trigger_does_not_subscribe_to_materialisations(
        self, store, catalog
    ):
        await declare(catalog)
        t, _ = self.stale_trigger(catalog)
        await t.start()
        assert catalog._subscribers == {}
        await t.stop()


@pytest.mark.asyncio
class TestMaterialisationRecords:
    """The records the trigger reacts to must persist and read back."""

    async def test_a_materialisation_is_saved_and_has_its_own_time(
        self, store, catalog
    ):
        first = await produce(catalog, version="1")
        await asyncio.sleep(0.01)
        second = await produce(catalog, version="2")
        assert first.id != second.id and first.created_at < second.created_at
        assert (await AssetMaterialisation.get(first.id)).asset_version == "1"

    async def test_get_materialisation_returns_the_latest(self, store, catalog):
        await produce(catalog, version="1")
        await asyncio.sleep(0.01)
        await produce(catalog, version="2")
        assert (await catalog.get_materialisation(RAW)).asset_version == "2"
        assert (
            await catalog.get_materialisation(RAW, version="1")
        ).asset_version == "1"
        assert await catalog.get_materialisation(RAW, version="9") is None
        assert await catalog.get_materialisation(AssetKey("never")) is None

    async def test_history_is_newest_first_and_limited(self, store, catalog):
        for version in "123":
            await produce(catalog, version=version)
            await asyncio.sleep(0.01)
        history = await catalog.get_materialisation_history(RAW, limit=2)
        assert [m.asset_version for m in history] == ["3", "2"]
