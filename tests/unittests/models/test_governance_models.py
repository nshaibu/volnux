"""The governance models: hashing, enums, uniqueness, defaults, versions, validation,
encryption at rest, the audit chain and deletion."""

import difflib
import json

import pytest

from formax.exceptions import ValidationError

from tests.unittests.models.conftest import run
from volnux.exceptions import ImproperlyConfigured, ObjectExistError
from volnux.models import (
    ApprovalChain,
    ApprovalStep,
    AuditEntry,
    Delegation,
    Event,
    Execution,
    HITLRequest,
    ExecutionTrace,
    MeshNode,
    NodeHeartbeat,
    NotificationConfig,
    Organization,
    Role,
    RoleAssignment,
    Team,
    TeamMember,
    User,
    Workflow,
    WorkflowDescriptor,
    WorkflowVariable,
    WorkflowVersion,
)
from volnux.models.enums import (
    ApproverType,
    AuditEventType,
    FailureBehavior,
    NodeStatus,
    SLABreachAction,
    TriggerType,
    WorkflowCategory,
    WorkflowMode,
    WorkflowStatus,
)
from volnux.models.workflows import apply_unified_diff


BAD = (ValueError, TypeError, ValidationError)


class TestHashing:
    def test_models_are_hashable_by_id(self, base):
        a = run(User.get(base.user.id))
        b = run(User.get(base.user.id))
        assert a == b and hash(a) == hash(b)
        assert len({a, b, base.user}) == 1
        assert {base.org: 1}[run(Organization.get(base.org.id))] == 1

    def test_every_model_is_hashable(self):
        import volnux.models as m

        for name in m.__all__:
            assert getattr(m, name).__hash__ is not None, name


class TestEnums:
    def test_enums_come_back_as_members(self, base):
        wf = run(Workflow.get(base.wf.id))
        assert wf.status is WorkflowStatus.DRAFT
        assert wf.category is WorkflowCategory.STANDARD
        assert wf.status.value == "draft"

    def test_optional_enums(self, base):
        async def go():
            ra = RoleAssignment(
                user=base.user,
                role=Role(name="r", slug="r"),
                granted_by=base.user,
                environment="staging",
            )
            await ra.role.save()
            await ra.save()
            return await RoleAssignment.get(ra.id)

        assert run(go()).environment.value == "staging"

    def test_new_enum_fields(self, base):
        chain = ApprovalChain(name="c", organization=base.org, created_by=base.user)
        assert chain.failure_behavior is FailureBehavior.RESET_CHAIN
        run(chain.save())
        assert (
            run(ApprovalChain.get(chain.id)).failure_behavior
            is FailureBehavior.RESET_CHAIN
        )
        with pytest.raises(BAD):
            ApprovalChain(
                name="c",
                organization=base.org,
                created_by=base.user,
                failure_behavior="bogus",
            )


class TestUniqueness:
    def test_organization_and_role_slugs(self, base):
        with pytest.raises(ObjectExistError):
            run(Organization(name="x", slug="o").save())
        run(Role(name="r", slug="r").save())
        with pytest.raises(ObjectExistError):
            run(Role(name="r2", slug="r").save())

    def test_resaving_the_same_record_is_fine(self, base):
        base.org.name = "renamed"
        run(base.org.save())
        run(base.user.save())

    def test_user_email_is_per_organization_and_names_may_repeat(self, base):
        run(
            User(name="u", email="other@o.io", organization=base.org).save()
        )  # same name
        run(
            User(name="x", email="u@o.io", organization=base.org2).save()
        )  # same email, other org
        with pytest.raises(ObjectExistError):
            run(User(name="y", email="u@o.io", organization=base.org).save())

    def test_team_slug_event_and_membership(self, base):
        with pytest.raises(ObjectExistError):
            run(Team(name="t2", slug="t", organization=base.org).save())
        run(Team(name="t", slug="t", organization=base.org2).save())
        run(TeamMember(team=base.team, user=base.user).save())
        with pytest.raises(ObjectExistError):
            run(TeamMember(team=base.team, user=base.user).save())

        def ev(version, org):
            return Event(
                name="e",
                version=version,
                organization=org,
                manifest={},
                publisher=base.user,
            )

        run(ev("1.0.0", base.org).save())
        run(ev("1.0.1", base.org).save())
        run(ev("1.0.0", base.org2).save())
        with pytest.raises(ObjectExistError):
            run(ev("1.0.0", base.org).save())

    def test_descriptor_labels_are_unique_per_workflow(self, base):
        wf2 = Workflow(
            name="w2", organization=base.org, team=base.team, created_by=base.user
        )
        run(wf2.save())
        run(WorkflowDescriptor(workflow=base.wf, descriptor_number=3, label="a").save())
        run(WorkflowDescriptor(workflow=wf2, descriptor_number=3, label="a").save())
        with pytest.raises(ObjectExistError):
            run(
                WorkflowDescriptor(
                    workflow=base.wf, descriptor_number=4, label="a"
                ).save()
            )

    def test_variable_names_are_unique_per_workflow(self, base):
        run(WorkflowVariable(workflow=base.wf, name="x", value=1).save())
        with pytest.raises(ObjectExistError):
            run(WorkflowVariable(workflow=base.wf, name="x", value=2).save())


class TestDefaults:
    def test_nullable_foreign_keys_can_be_left_out(self, base):
        wf = Workflow(
            name="w", organization=base.org, team=base.team, created_by=base.user
        )
        assert wf.updated_by is None and wf.approval_chain is None
        ra = RoleAssignment(
            user=base.user, role=Role(name="r", slug="r"), granted_by=base.user
        )
        assert ra.team is None and ra.revoked_at is None
        d = Delegation(
            organization=base.org,
            delegator=base.user,
            delegate=base.user,
            role=ra.role,
            start_time=1.0,
            end_time=2.0,
        )
        assert d.scope_team is None and d.revoked_by is None

    def test_required_foreign_keys_are_still_required(self, base):
        with pytest.raises(AttributeError):
            Workflow(name="w", organization=base.org, created_by=base.user)  # no team

    def test_triggered_by_is_nullable_and_set_null_on_user_delete(self, base):
        async def go():
            v = WorkflowVersion(
                workflow=base.wf,
                version="0.1.0",
                ast_hash="h",
                pty_source="A",
                published_by=base.user,
            )
            await v.save()
            ex = Execution(
                workflow=base.wf,
                workflow_version=v,
                organization=base.org,
                team=base.team,
                trigger_type=TriggerType.MANUAL,
            )
            assert ex.triggered_by is None
            runner = User(name="r", email="r@o.io", organization=base.org)
            await runner.save()
            ex.triggered_by = runner
            await ex.save()
            await runner.delete()
            return await Execution.get(ex.id)

        assert run(go()).triggered_by is None


class TestVersions:
    BASE = "".join(f"line {i}\n" for i in range(1, 21))

    def patch(self, new):
        return "".join(
            difflib.unified_diff(
                self.BASE.splitlines(True),
                new.splitlines(True),
                fromfile="a",
                tofile="b",
            )
        )

    @pytest.mark.parametrize(
        "edit",
        [
            lambda t: t.replace("line 2\n", "TWO\n").replace(
                "line 18\n", "EIGHTEEN\n"
            ),  # two hunks
            lambda t: "new first\n" + t,
            lambda t: t + "new last\n",
            lambda t: t.replace("line 10\n", ""),
            lambda t: t.replace("line 10\n", "a\nb\nc\n"),
            lambda t: "",
            lambda t: t,
        ],
    )
    def test_patches_round_trip(self, edit):
        new = edit(self.BASE)
        assert apply_unified_diff(self.BASE, self.patch(new)) == new

    def test_patch_into_an_empty_base(self):
        new = "a\nb\n"
        patch = "".join(difflib.unified_diff([], new.splitlines(True)))
        assert apply_unified_diff("", patch) == new

    def test_a_patch_for_other_text_is_refused(self):
        with pytest.raises(ValueError):
            apply_unified_diff("totally\ndifferent\n", self.patch(self.BASE.upper()))

    def test_text_without_a_final_newline_is_not_diffed(self):
        from volnux.models.workflows import make_delta

        assert make_delta("a\nb", "a\nc") is None  # difflib cannot mark the missing \n
        assert make_delta("a\nb\n", "a\nc") is None
        assert make_delta("", "a\n") is not None
        assert apply_unified_diff("a\nb\n", make_delta("a\nb\n", "a\nc\n")) == "a\nc\n"

    def test_create_delta_falls_back_to_the_whole_text(self, base):
        parent = WorkflowVersion(
            workflow=base.wf,
            version="0.1.0",
            ast_hash="h",
            pty_source="a\nb",
            published_by=base.user,
        )
        assert WorkflowVersion.create_delta(parent, "a\nc") == ("a\nc", None)
        assert WorkflowVersion.create_delta(None, "x\n") == ("x\n", None)
        assert WorkflowVersion.create_delta(parent, "a\nb") == (None, "")

    def test_delta_chain_is_rebuilt_from_the_store(self, base):
        v2_text = self.BASE.replace("line 3\n", "THREE\n")
        v3_text = v2_text.replace("line 15\n", "FIFTEEN\n")

        async def go():
            v1 = WorkflowVersion(
                workflow=base.wf,
                version="0.1.0",
                ast_hash="1",
                pty_source=self.BASE,
                published_by=base.user,
            )
            await v1.save()
            v2 = WorkflowVersion(
                workflow=base.wf,
                version="0.1.1",
                ast_hash="2",
                parent_version=v1,
                delta_patch=self.patch(v2_text),
                published_by=base.user,
            )
            await v2.save()
            v3 = WorkflowVersion(
                workflow=base.wf,
                version="0.1.2",
                ast_hash="3",
                parent_version=v2,
                delta_patch="".join(
                    difflib.unified_diff(
                        v2_text.splitlines(True), v3_text.splitlines(True)
                    )
                ),
                published_by=base.user,
            )
            await v3.save()
            loaded = await WorkflowVersion.get(v3.id)
            return (
                await loaded.get_full_source_async(),
                (await WorkflowVersion.get(v1.id)).get_full_source(),
                await (await WorkflowVersion.get(v2.id)).get_full_source_async(),
            )

        full3, full1, full2 = run(go())
        assert (full3, full1, full2) == (v3_text, self.BASE, v2_text)

    def test_mode_and_semver(self, base):
        v = WorkflowVersion(
            workflow=base.wf,
            version="1.2.3",
            ast_hash="h",
            pty_source="A",
            published_by=base.user,
        )
        assert v.mode is WorkflowMode.CFG
        with pytest.raises(BAD):
            WorkflowVersion(
                workflow=base.wf,
                version="v1",
                ast_hash="h",
                pty_source="A",
                published_by=base.user,
            )


class TestValidation:
    def step(self, base, **kw):
        chain = ApprovalChain(name="c", organization=base.org, created_by=base.user)
        return ApprovalStep(
            chain=chain,
            workflow=base.wf,
            step_order=1,
            approver_type=ApproverType.COMPLIANCE,
            **kw,
        )

    def test_approval_step_bounds(self, base):
        self.step(base, required_count=1, timeout_hours=1)
        for bad in ({"required_count": 0}, {"timeout_hours": 0}):
            with pytest.raises(BAD):
                self.step(base, **bad)

    def test_hitl_bounds_and_enum(self, base):
        def hitl(**kw):
            ex = Execution.__new__(Execution)
            return kw

        ver = WorkflowVersion(
            workflow=base.wf,
            version="0.1.0",
            ast_hash="h",
            pty_source="A",
            published_by=base.user,
        )
        ex = Execution(
            workflow=base.wf,
            workflow_version=ver,
            organization=base.org,
            team=base.team,
            trigger_type=TriggerType.MANUAL,
        )
        tr = ExecutionTrace(execution=ex, event_name="e", event_version="1")

        def make(**kw):
            return HITLRequest(
                execution=ex, trace=tr, workflow=base.wf, node_id="n", prompt="p", **kw
            )

        assert make().sla_breach_action is SLABreachAction.ESCALATE_TO_SUPER_ADMIN
        make(sla_warning_threshold=1.0)
        for bad in (
            {"sla_warning_threshold": 1.5},
            {"sla_warning_threshold": -0.1},
            {"escalation_count": -1},
        ):
            with pytest.raises(BAD):
                make(**bad)

    def test_delegation_window(self, base):
        role = Role(name="r", slug="r")
        run(role.save())

        def make(start, end):
            return Delegation(
                organization=base.org,
                delegator=base.user,
                delegate=base.user,
                role=role,
                start_time=start,
                end_time=end,
            )

        run(make(1.0, 2.0).save())
        for start, end in ((2.0, 2.0), (3.0, 2.0)):
            with pytest.raises(ValueError):
                run(make(start, end).save())

    def test_node_bounds(self, base):
        def node(**kw):
            return MeshNode(
                name="n", organization=base.org, registered_by=base.user, **kw
            )

        node(capacity_cpu_cores=4, capacity_memory_gb=8.0, current_load=0)
        for bad in (
            {"capacity_cpu_cores": 0},
            {"capacity_memory_gb": 0},
            {"current_load": -1},
        ):
            with pytest.raises(BAD):
                node(**bad)
        n = node()
        with pytest.raises(BAD):
            NodeHeartbeat(
                node=n, status=NodeStatus.HEALTHY, current_load=1, cpu_utilization=101.0
            )
        with pytest.raises(BAD):
            NodeHeartbeat(node=n, status=NodeStatus.HEALTHY, current_load=-1)

    def test_event_version_pattern(self, base):
        with pytest.raises(BAD):
            Event(
                name="e",
                version="one",
                organization=base.org,
                manifest={},
                publisher=base.user,
            )


class TestTimestamps:
    def test_epoch_fields_are_floats_after_a_reload(self, base):
        async def go():
            role = Role(name="r", slug="r")
            await role.save()
            d = Delegation(
                organization=base.org,
                delegator=base.user,
                delegate=base.user,
                role=role,
                start_time=1.0,
                end_time=2.0,
                revoked_at=5.5,
            )
            await d.save()
            node = MeshNode(name="n", organization=base.org, registered_by=base.user)
            await node.save()
            hb = NodeHeartbeat(node=node, status=NodeStatus.HEALTHY, current_load=1)
            await hb.save()
            return await Delegation.get(d.id), await NodeHeartbeat.get(hb.id)

        d, hb = run(go())
        assert d.revoked_at == 5.5 and isinstance(d.revoked_at, float)
        assert isinstance(hb.recorded_at, float)


class TestEncryptionAtRest:
    def raw(self, backend, obj):
        return json.loads(backend._serialize_record(obj))

    def test_notification_config_is_encrypted_in_the_store(self, backend, base):
        cfg = NotificationConfig(
            user=base.user,
            organization=base.org,
            channel="slack",
            config={"webhook": "https://hooks/secret"},
        )
        run(cfg.save())
        stored = self.raw(backend, cfg)
        assert "secret" not in str(stored)
        assert set(stored["config"]) == {"__enc__"}
        assert run(NotificationConfig.get(cfg.id)).config == {
            "webhook": "https://hooks/secret"
        }
        assert cfg.config == {
            "webhook": "https://hooks/secret"
        }  # in memory it stays plain

    def test_other_encrypted_fields(self, backend, base):
        var = WorkflowVariable(
            workflow=base.wf, name="token", value={"k": "s3cret"}, is_environment=True
        )
        run(var.save())
        assert "s3cret" not in str(self.raw(backend, var))
        assert run(WorkflowVariable.get(var.id)).value == {"k": "s3cret"}
        org = Organization(name="x", slug="x", sso_config={"client_secret": "abc"})
        run(org.save())
        assert "abc" not in str(self.raw(backend, org))
        assert run(Organization.get(org.id)).sso_config == {"client_secret": "abc"}
        plain = Organization(name="y", slug="y")
        run(plain.save())
        assert run(Organization.get(plain.id)).sso_config is None

    def test_saving_twice_does_not_double_encrypt(self, base):
        cfg = NotificationConfig(
            user=base.user, organization=base.org, channel="email", config={"a": 1}
        )
        run(cfg.save())
        run(cfg.save())
        assert run(NotificationConfig.get(cfg.id)).config == {"a": 1}

    def test_a_different_key_cannot_read_it(self, backend, base, monkeypatch):
        cfg = NotificationConfig(
            user=base.user, organization=base.org, channel="email", config={"a": 1}
        )
        run(cfg.save())
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "z" * 48)
        with pytest.raises(Exception, match="decrypt"):
            run(NotificationConfig.get(cfg.id))

    def test_the_encryption_key_setting_wins(self, backend, base, monkeypatch):
        monkeypatch.setenv("VOLNUX_ENCRYPTION_KEY", "e" * 48)
        cfg = NotificationConfig(
            user=base.user, organization=base.org, channel="email", config={"a": 1}
        )
        run(cfg.save())
        monkeypatch.delenv("VOLNUX_ENCRYPTION_KEY")
        with pytest.raises(Exception, match="decrypt"):
            run(NotificationConfig.get(cfg.id))

    def test_no_key_is_an_error_not_plaintext(self, backend, base, monkeypatch):
        monkeypatch.delenv("VOLNUX_SECRET_KEY")
        cfg = NotificationConfig(
            user=base.user, organization=base.org, channel="email", config={"a": 1}
        )
        with pytest.raises(Exception, match="SECRET_KEY"):
            run(cfg.save())


class TestAuditChain:
    def entry(self, base, n=0, **kw):
        return AuditEntry.append(
            organization=base.org,
            event_type=AuditEventType.WORKFLOW_CREATED,
            actor=base.user,
            actor_role="r",
            target_type="workflow",
            target_id=str(n),
            action="a",
            **kw,
        )

    def test_append_chains_and_verifies(self, base):
        first = run(self.entry(base, 1))
        second = run(self.entry(base, 2, metadata={"k": 1}))
        assert first.previous_hash is None and first.hash
        assert second.previous_hash == first.hash and second.hash != first.hash
        assert run(AuditEntry.verify_chain(base.org.id))

    def test_entries_cannot_be_created_directly(self, base):
        entry = AuditEntry(
            organization=base.org,
            event_type=AuditEventType.ROLE_ASSIGNED,
            actor=base.user,
            actor_role="r",
            target_type="t",
            target_id="1",
            action="a",
        )
        with pytest.raises(ValueError, match="append"):
            run(entry.save())

    def test_entries_are_immutable(self, base):
        e = run(self.entry(base, 1))
        e.action = "changed"
        for op in (e.save, e.update, e.delete):
            with pytest.raises(PermissionError):
                run(op())
        assert run(AuditEntry.get(e.id)).action == "a"

    def test_tampering_is_detected(self, base):
        from volnux.models.base import GovernanceModel

        for n in range(3):
            run(self.entry(base, n))
        entries = run(
            AuditEntry.filter_async(
                organization__object_id=base.org.id, order_by="created_at"
            )
        )
        entries[1].target_id = "forged"
        run(GovernanceModel.update(entries[1]))
        assert not run(AuditEntry.verify_chain(base.org.id))

    def test_chains_are_per_organization(self, base):
        run(self.entry(base, 1))
        other = run(
            AuditEntry.append(
                organization=base.org2,
                event_type=AuditEventType.ROLE_ASSIGNED,
                actor=base.user,
                actor_role="r",
                target_type="t",
                target_id="1",
                action="a",
            )
        )
        assert other.previous_hash is None
        assert run(AuditEntry.verify_chain(base.org2.id))


class TestDeletion:
    def test_deleting_a_workflow_removes_what_hangs_off_it(self, base):
        async def go():
            v = WorkflowVersion(
                workflow=base.wf,
                version="0.1.0",
                ast_hash="h",
                pty_source="A",
                published_by=base.user,
            )
            await v.save()
            ex = Execution(
                workflow=base.wf,
                workflow_version=v,
                organization=base.org,
                team=base.team,
                trigger_type=TriggerType.MANUAL,
            )
            await ex.save()
            tr = ExecutionTrace(execution=ex, event_name="e", event_version="1")
            await tr.save()
            h = HITLRequest(
                execution=ex, trace=tr, workflow=base.wf, node_id="n", prompt="p"
            )
            await h.save()
            chain = ApprovalChain(name="c", organization=base.org, created_by=base.user)
            await chain.save()
            step = ApprovalStep(
                chain=chain,
                workflow=base.wf,
                step_order=1,
                approver_type=ApproverType.COMPLIANCE,
            )
            await step.save()
            await base.wf.delete()
            return [
                len(await WorkflowVersion.filter_async(workflow__object_id=base.wf.id)),
                len(await Execution.filter_async(workflow__object_id=base.wf.id)),
                len(await ExecutionTrace.filter_async(execution__object_id=ex.id)),
                len(await HITLRequest.filter_async(workflow__object_id=base.wf.id)),
                len(await ApprovalStep.filter_async(workflow__object_id=base.wf.id)),
            ]

        assert run(go()) == [0, 0, 0, 0, 0]

    def test_an_organization_with_audit_entries_cannot_be_deleted(self, base):
        run(
            AuditEntry.append(
                organization=base.org2,
                event_type=AuditEventType.ROLE_ASSIGNED,
                actor=base.user,
                actor_role="r",
                target_type="t",
                target_id="1",
                action="a",
            )
        )
        from volnux.exceptions import ObjectProtectedError

        with pytest.raises(ObjectProtectedError):
            run(base.org2.delete())
