"""Workflow lifecycle: create -> submit -> approvals -> publish -> execute, plus the
audit trail, HITL, break-glass, executions and the smaller routers."""

import time
from unittest import mock

import pytest

from tests.unittests.app.conftest import run


def make(world, name, org, *roles):
    user = world.seed(name, org, roles)
    return user, world.headers(user)


class FakeExecutor:
    """What the API needs of an executor that can control runs."""

    supports_control = True

    def __init__(self):
        self.live = set()
        self.calls = []

    async def _act(self, name, execution_id):
        self.calls.append((name, execution_id))
        return execution_id in self.live

    async def pause_execution(self, execution_id):
        return await self._act("pause", execution_id)

    async def resume_execution(self, execution_id):
        return await self._act("resume", execution_id)

    async def stop_execution(self, execution_id):
        return await self._act("stop", execution_id)


@pytest.fixture
def live_engine():
    """The engine singleton replaced by one with a controllable executor."""
    holder = type("Holder", (), {})()
    holder.executor = FakeExecutor()
    holder.engine = mock.MagicMock()
    holder.engine.fire_trigger = mock.AsyncMock()
    holder.engine.workflow_executor = holder.executor
    with mock.patch(
        "volnux.engine.workflows.trigger.get_trigger_engine",
        return_value=holder.engine,
    ):
        yield holder


@pytest.fixture
def org(client, world):
    """acme: an author, two reviewers, a compliance approver, an operator, an
    auditor, a super-admin and an org-admin; globex: an outsider admin."""
    ctx = type("Ctx", (), {})()
    ctx.client, ctx.world = client, world
    ctx.author, ctx.ha = make(world, "author", "acme", "workflow-author")
    ctx.rev1, ctx.h1 = make(world, "rev1", "acme", "workflow-reviewer")
    ctx.rev2, ctx.h2 = make(world, "rev2", "acme", "workflow-reviewer")
    ctx.comp, ctx.hc = make(world, "comp", "acme", "compliance-approver")
    ctx.op, ctx.hop = make(
        world, "op", "acme", "workflow-operator", "workflow-consumer"
    )
    ctx.aud, ctx.haud = make(world, "aud", "acme", "governance-auditor")
    ctx.root, ctx.hroot = make(world, "root", "acme", "super-admin")
    ctx.adm, ctx.hadm = make(world, "adm", "acme", "org-admin", "workflow-author")
    ctx.out, ctx.hout = make(
        world,
        "out",
        "globex",
        "org-admin",
        "workflow-author",
        "workflow-reviewer",
        "workflow-operator",
        "super-admin",
    )
    from volnux.models import Team

    async def team():
        t = Team(name="t", slug="t", organization=ctx.author.organization)
        await t.save()
        return t

    ctx.team = run(team())
    return ctx


def make_chain(o, steps=None, **kw):
    reviewer = o.world.roles["workflow-reviewer"].id
    comp = o.world.roles["compliance-approver"].id
    steps = steps or [
        {
            "step_order": 1,
            "approver_type": "role_based",
            "role_id": reviewer,
            "required_count": 2,
        },
        {"step_order": 2, "approver_type": "compliance", "role_id": comp},
    ]
    body = {
        "name": "chain",
        "applicable_categories": ["standard"],
        "steps": steps,
        **kw,
    }
    r = o.client.post("/api/v1/approvals/chains", json=body, headers=o.hadm)
    return r


def new_workflow(o, chain_id, source="A >> B\n", headers=None):
    r = o.client.post(
        "/api/v1/workflows",
        json={
            "name": "wf",
            "team_id": o.team.id,
            "approval_chain_id": chain_id,
            "pty_source": source,
        },
        headers=headers or o.ha,
    )
    assert r.status_code == 201, r.text
    return r.json()["data"]["id"]


class TestLifecycle:
    def test_full_flow(self, org, live_engine):
        c = org.client
        # only the org-admin may manage chains (APPROVAL_MANAGE)
        chain = make_chain(org)
        assert chain.status_code == 201, chain.text
        chain_id = chain.json()["data"]["id"]
        assert (
            c.get("/api/v1/approvals/chains", headers=org.h1).json()["meta"]["total"]
            == 1
        )

        wid = new_workflow(org, chain_id)
        assert c.get("/api/v1/workflows", headers=org.ha).json()["meta"]["total"] == 1
        assert (
            c.get(f"/api/v1/workflows/{wid}/source", headers=org.ha).json()["data"][
                "pty_source"
            ]
            == "A >> B\n"
        )

        # can't publish a draft, can't execute it
        assert (
            c.post(f"/api/v1/workflows/{wid}/publish", headers=org.ha).status_code
            == 409
        )
        assert (
            c.post(
                f"/api/v1/workflows/{wid}/execute", json={}, headers=org.hop
            ).status_code
            == 409
        )
        # approve before submit
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h1
            ).status_code
            == 409
        )

        sub = c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        assert sub.status_code == 200, sub.text
        assert sub.json()["data"]["status"] == "pending_review"
        # no editing, no resubmitting while under review
        assert (
            c.put(
                f"/api/v1/workflows/{wid}", json={"name": "x"}, headers=org.ha
            ).status_code
            == 409
        )
        assert (
            c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha).status_code == 409
        )

        # pending lists: reviewers see it, the compliance approver does not yet,
        # the author never does
        assert (
            len(c.get("/api/v1/approvals/pending", headers=org.h1).json()["data"]) == 1
        )
        assert c.get("/api/v1/approvals/pending", headers=org.ha).status_code == 403
        assert c.get("/api/v1/approvals/pending", headers=org.hc).json()["data"] == []

        # order: compliance cannot jump ahead of the reviewers
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.hc
            ).status_code
            == 403
        )
        # quorum of two: one reviewer is not enough, and cannot approve twice
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h1
            ).status_code
            == 200
        )
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h1
            ).status_code
            == 409
        )
        assert (
            c.get(f"/api/v1/workflows/{wid}", headers=org.ha).json()["data"]["status"]
            == "pending_review"
        )
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h2
            ).status_code
            == 200
        )
        # now the compliance step
        assert (
            len(c.get("/api/v1/approvals/pending", headers=org.hc).json()["data"]) == 1
        )
        assert (
            c.get(f"/api/v1/workflows/{wid}", headers=org.ha).json()["data"]["status"]
            == "pending_review"
        )
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.hc
            ).status_code
            == 200
        )
        assert (
            c.get(f"/api/v1/workflows/{wid}", headers=org.ha).json()["data"]["status"]
            == "approved"
        )

        # publish, then run
        assert (
            c.post(f"/api/v1/workflows/{wid}/publish", headers=org.ha).json()["data"][
                "status"
            ]
            == "production"
        )
        engine = live_engine.engine
        ex = c.post(
            f"/api/v1/workflows/{wid}/execute",
            json={"inputs": {"a": 1}},
            headers=org.hop,
        )
        assert ex.status_code == 202, ex.text
        assert engine.fire_trigger.await_count == 1
        assert engine.fire_trigger.await_args.args[:2] == ("wf", {"a": 1})
        # the run is linked to the API's record
        assert (
            engine.fire_trigger.await_args.kwargs["execution_id"]
            == ex.json()["data"]["id"]
        )
        live_engine.executor.live.add(ex.json()["data"]["id"])
        eid = ex.json()["data"]["id"]

        # execution control
        assert (
            c.post(f"/api/v1/executions/{eid}/resume", headers=org.hop).status_code
            == 409
        )
        assert (
            c.post(f"/api/v1/executions/{eid}/pause", headers=org.hop).json()["data"][
                "status"
            ]
            == "paused"
        )
        assert (
            c.post(f"/api/v1/executions/{eid}/resume", headers=org.hop).json()["data"][
                "status"
            ]
            == "running"
        )
        assert (
            c.post(f"/api/v1/executions/{eid}/stop", headers=org.hop).json()["data"][
                "status"
            ]
            == "stopped"
        )
        assert (
            c.post(f"/api/v1/executions/{eid}/pause", headers=org.hop).status_code
            == 409
        )
        assert c.get("/api/v1/executions", headers=org.hop).json()["meta"]["total"] == 1
        assert (
            c.get(f"/api/v1/executions/{eid}/traces", headers=org.hop).json()["data"]
            == []
        )

        # everything above is in the audit trail, and the chain is intact
        entries = c.get("/api/v1/audit/entries?page_size=200", headers=org.haud).json()
        kinds = {e["event_type"] for e in entries["data"]}
        assert {
            "workflow_created",
            "workflow_submitted",
            "workflow_approved",
            "workflow_promoted",
            "execution_started",
            "execution_paused",
            "execution_stopped",
        } <= kinds
        assert (
            c.get("/api/v1/audit/verify", headers=org.haud).json()["data"]["intact"]
            is True
        )
        only = c.get(f"/api/v1/audit/entries?target_id={wid}", headers=org.haud).json()[
            "data"
        ]
        assert only and all(e["target_id"] == wid for e in only)
        # the trail is read-only
        assert c.delete("/api/v1/audit/entries", headers=org.haud).status_code == 405
        assert (
            c.put("/api/v1/audit/entries", json={}, headers=org.haud).status_code == 405
        )

    def test_engine_refusal_marks_the_execution_failed(self, org):
        c = org.client
        chain_id = make_chain(
            org,
            steps=[
                {
                    "step_order": 1,
                    "approver_type": "role_based",
                    "role_id": org.world.roles["workflow-reviewer"].id,
                }
            ],
        ).json()["data"]["id"]
        wid = new_workflow(org, chain_id)
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h1
            ).status_code
            == 200
        )
        c.post(f"/api/v1/workflows/{wid}/publish", headers=org.ha)
        engine = mock.MagicMock()
        engine.fire_trigger = mock.AsyncMock(
            side_effect=RuntimeError("Workflow executor is not ready.")
        )
        with mock.patch(
            "volnux.engine.workflows.trigger.get_trigger_engine", return_value=engine
        ):
            r = c.post(f"/api/v1/workflows/{wid}/execute", json={}, headers=org.hop)
        assert r.status_code == 503
        runs = c.get("/api/v1/executions", headers=org.hop).json()["data"]
        assert [e["status"] for e in runs] == ["failed"]

    def test_reject_resets_and_resubmit(self, org):
        c = org.client
        chain_id = make_chain(org).json()["data"]["id"]
        wid = new_workflow(org, chain_id)
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/reject", json={}, headers=org.h1
            ).status_code
            == 422
        )  # reason needed
        r = c.post(
            f"/api/v1/approvals/{wid}/reject",
            json={"reason": "not safe", "request_changes": True},
            headers=org.h1,
        )
        assert r.json()["data"]["status"] == "changes_requested"
        # chain reset: nothing pending, and the author can edit and resubmit
        assert c.get("/api/v1/approvals/pending", headers=org.h2).json()["data"] == []
        assert (
            c.put(
                f"/api/v1/workflows/{wid}",
                json={"pty_source": "A >> C\n"},
                headers=org.ha,
            ).status_code
            == 200
        )
        versions = c.get(f"/api/v1/workflows/{wid}/versions", headers=org.ha).json()[
            "data"
        ]
        assert [v["version"] for v in versions] == ["0.1.1", "0.1.0"]
        assert versions[0]["delta_patch"]
        assert (
            c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha).status_code == 200
        )
        assert (
            len(c.get("/api/v1/approvals/pending", headers=org.h2).json()["data"]) == 1
        )

    def test_hard_reject(self, org):
        c = org.client
        wid = new_workflow(org, make_chain(org).json()["data"]["id"])
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        r = c.post(
            f"/api/v1/approvals/{wid}/reject", json={"reason": "no"}, headers=org.h1
        )
        assert r.json()["data"]["status"] == "rejected"

    def test_cannot_approve_your_own_workflow(self, org):
        c = org.client
        # an org-admin who is also a reviewer authors a workflow
        from volnux.models import Role, RoleAssignment

        async def grant():
            await RoleAssignment(
                user=org.adm,
                role=org.world.roles["workflow-reviewer"],
                team=None,
                granted_by=org.adm,
                revoked_at=None,
            ).save()

        run(grant())
        wid = new_workflow(org, make_chain(org).json()["data"]["id"], headers=org.hadm)
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.hadm)
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.hadm
            ).status_code
            == 403
        )
        assert c.get("/api/v1/approvals/pending", headers=org.hadm).json()["data"] == []

    def test_submit_needs_a_chain_and_source(self, org):
        c = org.client
        r = c.post(
            "/api/v1/workflows",
            json={"name": "bare", "team_id": org.team.id},
            headers=org.ha,
        )
        wid = r.json()["data"]["id"]
        assert (
            c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha).status_code == 422
        )
        # an applicable org chain is found by category even if not set on the workflow
        make_chain(org)
        c.put(f"/api/v1/workflows/{wid}", json={"pty_source": "A\n"}, headers=org.ha)
        assert (
            c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha).status_code == 200
        )

    def test_delete_only_drafts(self, org):
        c = org.client
        wid = new_workflow(org, make_chain(org).json()["data"]["id"])
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        assert c.delete(f"/api/v1/workflows/{wid}", headers=org.ha).status_code == 409
        w2 = new_workflow(org, make_chain(org).json()["data"]["id"])
        assert c.delete(f"/api/v1/workflows/{w2}", headers=org.ha).status_code == 200
        assert c.get(f"/api/v1/workflows/{w2}", headers=org.ha).status_code == 404

    def test_timeout_blocks_a_late_approval(self, org):
        c = org.client
        steps = [
            {
                "step_order": 1,
                "approver_type": "role_based",
                "role_id": org.world.roles["workflow-reviewer"].id,
                "timeout_hours": 1,
            }
        ]
        wid = new_workflow(org, make_chain(org, steps=steps).json()["data"]["id"])
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        with mock.patch("volnux.app.v1.approvals.datetime") as dt:
            from datetime import datetime, timezone

            dt.now.return_value = datetime.fromtimestamp(
                time.time() + 7200, timezone.utc
            )
            r = c.post(f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h1)
        assert r.status_code == 409 and "timed out" in r.text

    def test_mass_assignment_and_status_are_rejected(self, org):
        c = org.client
        wid = new_workflow(org, make_chain(org).json()["data"]["id"])
        for body in (
            {"status": "production"},
            {"organization": "x"},
            {"created_by": "x"},
        ):
            assert (
                c.put(f"/api/v1/workflows/{wid}", json=body, headers=org.ha).status_code
                == 422
            )
        assert (
            c.post(
                "/api/v1/workflows",
                json={"name": "n", "team_id": org.team.id, "status": "production"},
                headers=org.ha,
            ).status_code
            == 422
        )


class TestTenancyAndPermissions:
    def test_other_organization_gets_404_everywhere(self, org):
        c = org.client
        wid = new_workflow(org, make_chain(org).json()["data"]["id"])
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        h = org.hout
        for method, path, body in [
            ("get", f"/api/v1/workflows/{wid}", None),
            ("get", f"/api/v1/workflows/{wid}/source", None),
            ("get", f"/api/v1/workflows/{wid}/versions", None),
            ("put", f"/api/v1/workflows/{wid}", {"name": "x"}),
            ("delete", f"/api/v1/workflows/{wid}", None),
            ("post", f"/api/v1/workflows/{wid}/submit", None),
            ("post", f"/api/v1/workflows/{wid}/publish", None),
            ("post", f"/api/v1/workflows/{wid}/execute", {}),
            ("post", f"/api/v1/approvals/{wid}/approve", {}),
            ("post", f"/api/v1/approvals/{wid}/reject", {"reason": "x"}),
        ]:
            r = getattr(c, method)(
                path, headers=h, **({"json": body} if body is not None else {})
            )
            assert r.status_code == 404, (method, path, r.status_code)
        assert c.get("/api/v1/workflows", headers=h).json()["data"] == []
        assert c.get("/api/v1/approvals/chains", headers=h).json()["data"] == []
        assert c.get("/api/v1/approvals/pending", headers=h).json()["data"] == []
        assert c.get("/api/v1/audit/entries", headers=h).json()["data"] == []

    def test_audit_is_scoped(self, org):
        c = org.client
        new_workflow(org, make_chain(org).json()["data"]["id"])
        mine = c.get("/api/v1/audit/entries", headers=org.haud).json()
        theirs = c.get("/api/v1/audit/entries", headers=org.hout).json()
        assert mine["meta"]["total"] >= 1
        assert theirs["data"] == []

    def test_permissions(self, org):
        c = org.client
        assert (
            c.post(
                "/api/v1/workflows",
                json={"name": "n", "team_id": org.team.id},
                headers=org.hop,
            ).status_code
            == 403
        )
        assert (
            c.post(
                "/api/v1/approvals/chains", json={"name": "n"}, headers=org.ha
            ).status_code
            == 403
        )
        assert c.get("/api/v1/audit/entries", headers=org.ha).status_code == 403
        assert c.post("/api/v1/executions/x/pause", headers=org.ha).status_code == 403

    def test_chain_validation(self, org):
        r = make_chain(
            org, steps=[{"step_order": 1, "approver_type": "named_individual"}]
        )
        assert r.status_code == 422
        r = make_chain(
            org,
            steps=[
                {
                    "step_order": 1,
                    "approver_type": "named_individual",
                    "individual_id": org.out.id,
                }
            ],
        )
        assert r.status_code == 404
        r = make_chain(
            org,
            steps=[
                {
                    "step_order": 1,
                    "approver_type": "named_individual",
                    "individual_id": org.rev1.id,
                }
            ],
        )
        assert r.status_code == 201

    def test_named_individual_and_team_steps(self, org):
        c = org.client
        steps = [
            {
                "step_order": 1,
                "approver_type": "named_individual",
                "individual_id": org.rev1.id,
            }
        ]
        wid = new_workflow(org, make_chain(org, steps=steps).json()["data"]["id"])
        c.post(f"/api/v1/workflows/{wid}/submit", headers=org.ha)
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h2
            ).status_code
            == 403
        )
        assert (
            c.post(
                f"/api/v1/approvals/{wid}/approve", json={}, headers=org.h1
            ).status_code
            == 200
        )
        # team quorum
        c.post(f"/api/v1/teams", json={"name": "T", "slug": "tt"}, headers=org.hadm)
        tid = [
            t
            for t in c.get("/api/v1/teams", headers=org.hadm).json()["data"]
            if t["slug"] == "tt"
        ][0]["id"]
        c.post(
            f"/api/v1/teams/{tid}/members",
            json={"user_id": org.rev2.id},
            headers=org.hadm,
        )
        steps = [{"step_order": 1, "approver_type": "team_quorum", "team_id": tid}]
        w2 = new_workflow(org, make_chain(org, steps=steps).json()["data"]["id"])
        c.post(f"/api/v1/workflows/{w2}/submit", headers=org.ha)
        assert (
            c.post(
                f"/api/v1/approvals/{w2}/approve", json={}, headers=org.h1
            ).status_code
            == 403
        )
        assert (
            c.post(
                f"/api/v1/approvals/{w2}/approve", json={}, headers=org.h2
            ).status_code
            == 200
        )
        assert (
            c.get(f"/api/v1/workflows/{w2}", headers=org.ha).json()["data"]["status"]
            == "approved"
        )


def seed_hitl(org, **kw):
    from volnux.models import (
        Execution,
        ExecutionTrace,
        HITLRequest,
        Workflow,
        WorkflowVersion,
    )
    from volnux.models.enums import TriggerType

    async def go():
        wf = Workflow(
            name="w",
            organization=org.author.organization,
            team=org.team,
            created_by=org.author,
            updated_by=None,
            approval_chain=None,
            published_at=None,
        )
        await wf.save()
        ver = WorkflowVersion(
            workflow=wf,
            version="0.1.0",
            ast_hash="h",
            parent_version=None,
            pty_source="A",
            published_by=org.author,
        )
        await ver.save()
        ex = Execution(
            workflow=wf,
            workflow_version=ver,
            organization=wf.organization,
            team=org.team,
            triggered_by=org.op,
            trigger_type=TriggerType.MANUAL,
            parent_execution=None,
        )
        await ex.save()
        tr = ExecutionTrace(
            execution=ex,
            event_name="e",
            event_version="1",
            node=None,
            parent_trace=None,
        )
        await tr.save()
        h = HITLRequest(
            execution=ex,
            trace=tr,
            workflow=wf,
            node_id="n",
            prompt="ok?",
            assigned_team=None,
            assigned_user=None,
            escalated_by=None,
            resolved_by=None,
            available_actions=["approve", "deny"],
            **kw,
        )
        await h.save()
        return h

    return run(go())


class TestHITL:
    def test_respond_rules(self, org):
        c = org.client
        h = seed_hitl(org, assigned_role="workflow-operator")
        assert (
            c.get("/api/v1/hitl/requests", headers=org.hop).json()["meta"]["total"] == 1
        )
        assert c.get("/api/v1/hitl/requests", headers=org.hout).json()["data"] == []
        assert (
            c.get(f"/api/v1/hitl/requests/{h.id}", headers=org.hout).status_code == 404
        )
        assert (
            c.post(
                f"/api/v1/hitl/requests/{h.id}/respond",
                json={"decision": "approve"},
                headers=org.hout,
            ).status_code
            == 404
        )
        assert (
            c.post(
                f"/api/v1/hitl/requests/{h.id}/respond",
                json={"decision": "maybe"},
                headers=org.hop,
            ).status_code
            == 422
        )
        r = c.post(
            f"/api/v1/hitl/requests/{h.id}/respond",
            json={"decision": "approve"},
            headers=org.hop,
        )
        assert r.status_code == 200, r.text
        assert r.json()["data"]["status"] == "responded"
        assert r.json()["data"]["resolved_by"] == org.op.id
        # a second answer cannot overwrite the first
        assert (
            c.post(
                f"/api/v1/hitl/requests/{h.id}/respond",
                json={"decision": "deny"},
                headers=org.hop,
            ).status_code
            == 409
        )

    def test_only_the_assignee_and_sla(self, org):
        c = org.client
        h = seed_hitl(org, assigned_role="some-other-role")
        assert (
            c.post(
                f"/api/v1/hitl/requests/{h.id}/respond",
                json={"decision": "approve"},
                headers=org.hop,
            ).status_code
            == 403
        )
        late = seed_hitl(org, sla_deadline=time.time() - 10)
        assert (
            c.post(
                f"/api/v1/hitl/requests/{late.id}/respond",
                json={"decision": "approve"},
                headers=org.hop,
            ).status_code
            == 409
        )
        assert (
            c.get(f"/api/v1/hitl/requests/{late.id}", headers=org.hop).json()["data"][
                "status"
            ]
            == "expired"
        )


class TestBreakGlass:
    def test_flow(self, org):
        c = org.client
        assert (
            c.post(
                "/api/v1/break-glass",
                json={"justification": "short"},
                headers=org.hroot,
            ).status_code
            == 422
        )
        assert (
            c.post(
                "/api/v1/break-glass",
                json={"justification": "production is down, need access"},
                headers=org.hadm,
            ).status_code
            == 403
        )
        r = c.post(
            "/api/v1/break-glass",
            json={"justification": "production is down, need access"},
            headers=org.hroot,
        )
        assert r.status_code == 201, r.text
        aid = r.json()["data"]["id"]
        # the initiator may not review it; an auditor may, once
        assert (
            c.post(
                f"/api/v1/break-glass/{aid}/review",
                json={"status": "compliant"},
                headers=org.hroot,
            ).status_code
            == 403
        )
        assert (
            c.post(
                f"/api/v1/break-glass/{aid}/review",
                json={"status": "pending"},
                headers=org.haud,
            ).status_code
            == 422
        )
        ok = c.post(
            f"/api/v1/break-glass/{aid}/review",
            json={"status": "non_compliant", "findings": "x"},
            headers=org.haud,
        )
        assert (
            ok.status_code == 200
            and ok.json()["data"]["review_status"] == "non_compliant"
        )
        assert (
            c.post(
                f"/api/v1/break-glass/{aid}/review",
                json={"status": "compliant"},
                headers=org.haud,
            ).status_code
            == 409
        )
        # other organizations cannot see it
        assert c.get(f"/api/v1/break-glass/{aid}", headers=org.hout).status_code == 404
        assert (
            c.get("/api/v1/break-glass", headers=org.haud).json()["meta"]["total"] == 1
        )
        kinds = {
            e["event_type"]
            for e in c.get("/api/v1/audit/entries", headers=org.haud).json()["data"]
        }
        assert {"break_glass_initiated", "break_glass_reviewed"} <= kinds


class TestSmallRouters:
    def test_notifications_are_private(self, org):
        c = org.client
        r = c.post(
            "/api/v1/notifications",
            json={
                "channel": "slack",
                "config": {"webhook_token": "sekret", "channel": "#x"},
            },
            headers=org.ha,
        )
        assert r.status_code == 201, r.text
        data = r.json()["data"]
        assert (
            data["config"]["webhook_token"] == "***"
            and data["config"]["channel"] == "#x"
        )
        nid = data["id"]
        assert [
            n["id"]
            for n in c.get("/api/v1/notifications", headers=org.ha).json()["data"]
        ] == [nid]
        assert c.get("/api/v1/notifications", headers=org.hop).json()["data"] == []
        assert (
            c.put(
                f"/api/v1/notifications/{nid}",
                json={"is_active": False},
                headers=org.hop,
            ).status_code
            == 404
        )
        assert (
            c.put(
                f"/api/v1/notifications/{nid}", json={"user": "x"}, headers=org.ha
            ).status_code
            == 422
        )
        assert (
            c.put(
                f"/api/v1/notifications/{nid}",
                json={"is_active": False},
                headers=org.ha,
            ).json()["data"]["is_active"]
            is False
        )
        assert (
            c.delete(f"/api/v1/notifications/{nid}", headers=org.hop).status_code == 404
        )
        assert (
            c.delete(f"/api/v1/notifications/{nid}", headers=org.ha).status_code == 200
        )

    def test_triggers(self, org):
        c = org.client
        wid = new_workflow(org, make_chain(org).json()["data"]["id"])
        body = {
            "workflow_id": wid,
            "trigger_type": "schedule",
            "config": {"cron": "* * * * *"},
        }
        assert (
            c.post("/api/v1/triggers", json=body, headers=org.hout).status_code == 404
        )
        r = c.post("/api/v1/triggers", json=body, headers=org.hadm)
        assert r.status_code == 201, r.text
        tid = r.json()["data"]["id"]
        assert (
            c.get(f"/api/v1/triggers?workflow_id={wid}", headers=org.ha).json()["meta"][
                "total"
            ]
            == 1
        )
        assert c.get("/api/v1/triggers", headers=org.hout).json()["data"] == []
        assert c.delete(f"/api/v1/triggers/{tid}", headers=org.hout).status_code == 404
        assert c.delete(f"/api/v1/triggers/{tid}", headers=org.hadm).status_code == 200

    def test_eventhub(self, org):
        c = org.client
        pub, hp = make(org.world, "pub", "acme", "eventhub-publisher")
        body = {"name": "ev", "version": "1.0.0", "manifest": {"k": 1}}
        r = c.post("/api/v1/eventhub/components", json=body, headers=hp)
        assert r.status_code == 201, r.text
        eid = r.json()["data"]["id"]
        assert (
            c.post("/api/v1/eventhub/components", json=body, headers=hp).status_code
            == 409
        )
        assert (
            c.post(
                "/api/v1/eventhub/components",
                json={**body, "status": "retired"},
                headers=hp,
            ).status_code
            == 422
        )
        assert (
            c.get("/api/v1/eventhub/components", headers=hp).json()["meta"]["total"]
            == 1
        )
        assert (
            c.get("/api/v1/eventhub/components", headers=org.hout).json()["data"] == []
        )
        assert (
            len(
                c.get(f"/api/v1/eventhub/components/{eid}/versions", headers=hp).json()[
                    "data"
                ]
            )
            == 1
        )
        hco = org.world.headers(org.out)
        assert (
            c.get(f"/api/v1/eventhub/components/{eid}", headers=org.ha).status_code
            == 200
        )
        assert c.get(
            f"/api/v1/eventhub/components/{eid}", headers=org.hout
        ).status_code in (403, 404)

    def test_mesh(self, org):
        c = org.client
        adm, ha = make(org.world, "mesh", "acme", "mesh-node-admin")
        r = c.post(
            "/api/v1/nodes", json={"name": "n1", "status": "healthy"}, headers=ha
        )
        assert r.status_code == 422  # a client cannot declare itself healthy
        r = c.post("/api/v1/nodes", json={"name": "n1"}, headers=ha)
        assert r.status_code == 201, r.text
        assert r.json()["data"]["status"] == "offline"
        nid = r.json()["data"]["id"]
        assert c.get("/api/v1/nodes", headers=ha).json()["meta"]["total"] == 1
        assert c.get(f"/api/v1/nodes/{nid}/health", headers=ha).json()["data"] == []
        assert c.get(f"/api/v1/nodes/{nid}", headers=org.hout).status_code == 404
        assert (
            c.post(f"/api/v1/nodes/{nid}/decommission", headers=org.hout).status_code
            == 404
        )
        first = c.post(f"/api/v1/nodes/{nid}/decommission", headers=ha).json()["data"]
        assert first["status"] == "decommissioned"
        again = c.post(f"/api/v1/nodes/{nid}/decommission", headers=ha).json()["data"]
        assert again["decommissioned_at"] == first["decommissioned_at"]
