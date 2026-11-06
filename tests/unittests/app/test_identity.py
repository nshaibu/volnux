"""Auth, users, organizations, teams, roles and delegations."""

import time

from tests.unittests.app.conftest import run, World


def make(world, name, org, *roles):
    user = world.seed(name, org, roles)
    return user, world.headers(user)


class TestAuth:
    def test_login_checks_the_password(self, client, world):
        user, _ = make(world, "ann", "acme", "workflow-author")
        bad = client.post(
            "/api/v1/auth/login", data={"email": user.email, "password": "nope"}
        )
        assert bad.status_code == 401
        ok = client.post(
            "/api/v1/auth/login",
            data={"email": user.email, "password": World.PASSWORD},
        )
        assert ok.status_code == 200, ok.text
        body = ok.json()["data"]
        assert body["roles"] == ["workflow-author"]
        assert "password_hash" not in body["user"]
        me = client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {body['access_token']}"},
        )
        assert me.status_code == 200
        assert me.json()["data"]["email"] == user.email

    def test_user_without_a_password_cannot_log_in(self, client, world):
        from volnux.models import Organization, User

        async def go():
            org = Organization(name="x", slug="x")
            await org.save()
            await User(name="nopw", email="nopw@x.io", organization=org).save()

        run(go())
        r = client.post(
            "/api/v1/auth/login", data={"email": "nopw@x.io", "password": "anything1"}
        )
        assert r.status_code == 401

    def test_no_token_is_401(self, client):
        assert client.get("/api/v1/users").status_code in (401, 403)

    def test_roles_come_from_the_database_not_the_token(self, client, world):
        admin, h = make(world, "adm", "acme", "org-admin")
        assert client.get("/api/v1/users", headers=h).status_code == 200
        # revoke the assignment: the same token stops working at once
        from volnux.models import RoleAssignment

        async def revoke():
            from datetime import datetime, timezone

            for a in await RoleAssignment.filter_async(user__object_id=admin.id):
                a.revoked_at = datetime.now(timezone.utc)
                await a.save()

        run(revoke())
        assert client.get("/api/v1/users", headers=h).status_code == 403


class TestUsers:
    def test_crud_and_isolation(self, client, world):
        admin, h = make(world, "adm", "acme", "org-admin")
        other, ho = make(world, "oth", "globex", "org-admin")
        r = client.post(
            "/api/v1/users",
            json={"name": "new", "email": "new@acme.io", "password": "longenough1"},
            headers=h,
        )
        assert r.status_code == 201, r.text
        uid = r.json()["data"]["id"]
        assert "password_hash" not in r.json()["data"]
        listed = client.get("/api/v1/users", headers=h).json()
        assert listed["meta"]["total"] == 2
        assert {u["id"] for u in listed["data"]} == {admin.id, uid}
        # other organization cannot see, change or delete it
        assert client.get(f"/api/v1/users/{uid}", headers=ho).status_code == 404
        assert (
            client.put(
                f"/api/v1/users/{uid}", json={"name": "x"}, headers=ho
            ).status_code
            == 404
        )
        assert client.delete(f"/api/v1/users/{uid}", headers=ho).status_code == 404
        # same organization can
        assert (
            client.put(
                f"/api/v1/users/{uid}", json={"name": "renamed"}, headers=h
            ).json()["data"]["name"]
            == "renamed"
        )
        assert client.delete(f"/api/v1/users/{uid}", headers=h).status_code == 200

    def test_mass_assignment_is_rejected(self, client, world):
        admin, h = make(world, "adm", "acme", "org-admin")
        r = client.put(
            f"/api/v1/users/{admin.id}",
            json={"organization": "elsewhere", "id": "x"},
            headers=h,
        )
        assert r.status_code == 422

    def test_cannot_delete_self_or_duplicate_email(self, client, world):
        admin, h = make(world, "adm", "acme", "org-admin")
        assert client.delete(f"/api/v1/users/{admin.id}", headers=h).status_code == 400
        dup = client.post(
            "/api/v1/users",
            json={"name": "d", "email": admin.email, "password": "longenough1"},
            headers=h,
        )
        assert dup.status_code == 409

    def test_needs_permission(self, client, world):
        _, h = make(world, "aut", "acme", "workflow-author")
        assert client.get("/api/v1/users", headers=h).status_code == 403


class TestTeamsRolesDelegations:
    def test_team_flow_and_isolation(self, client, world):
        admin, h = make(world, "adm", "acme", "org-admin")
        other, ho = make(world, "oth", "globex", "org-admin")
        t = client.post("/api/v1/teams", json={"name": "T", "slug": "t"}, headers=h)
        assert t.status_code == 201, t.text
        tid = t.json()["data"]["id"]
        assert (
            client.post(
                "/api/v1/teams", json={"name": "T", "slug": "t"}, headers=h
            ).status_code
            == 409
        )
        assert client.get(f"/api/v1/teams/{tid}", headers=ho).status_code == 404
        assert len(client.get("/api/v1/teams", headers=h).json()["data"]) == 1
        assert client.get("/api/v1/teams", headers=ho).json()["data"] == []
        # members: same org only, no duplicates
        assert (
            client.post(
                f"/api/v1/teams/{tid}/members", json={"user_id": other.id}, headers=h
            ).status_code
            == 404
        )
        assert (
            client.post(
                f"/api/v1/teams/{tid}/members", json={"user_id": admin.id}, headers=h
            ).status_code
            == 201
        )
        assert (
            client.post(
                f"/api/v1/teams/{tid}/members", json={"user_id": admin.id}, headers=h
            ).status_code
            == 409
        )
        members = client.get(f"/api/v1/teams/{tid}/members", headers=h).json()["data"]
        assert [m["id"] for m in members] == [admin.id]
        client.delete(f"/api/v1/teams/{tid}/members/{admin.id}", headers=h)
        assert (
            client.get(f"/api/v1/teams/{tid}/members", headers=h).json()["data"] == []
        )

    def test_roles_and_escalation(self, client, world):
        root, hr = make(world, "root", "acme", "super-admin")
        admin, h = make(world, "adm", "acme", "org-admin")
        author, _ = make(world, "aut", "acme")
        sa = client.post(
            "/api/v1/roles", json={"name": "SA", "slug": "super-admin"}, headers=hr
        )
        assert sa.status_code in (201, 409)
        role = client.post(
            "/api/v1/roles", json={"name": "A", "slug": "workflow-author"}, headers=hr
        )
        assert role.status_code in (201, 409)
        roles = {
            r["slug"]: r["id"]
            for r in client.get("/api/v1/roles", headers=h).json()["data"]
        }
        # org-admin may assign an ordinary role...
        ok = client.post(
            "/api/v1/roles/assign",
            json={
                "user_id": author.id,
                "role_id": roles["workflow-author"],
                "scope_type": "global",
            },
            headers=h,
        )
        assert ok.status_code == 201, ok.text
        # ...but not super-admin
        esc = client.post(
            "/api/v1/roles/assign",
            json={
                "user_id": admin.id,
                "role_id": roles["super-admin"],
                "scope_type": "global",
            },
            headers=h,
        )
        assert esc.status_code == 403
        # revoke
        assert (
            client.delete(
                f"/api/v1/roles/assign/{ok.json()['data']['id']}", headers=h
            ).status_code
            == 200
        )
        user_roles = client.get(f"/api/v1/users/{author.id}/roles", headers=h).json()[
            "data"
        ]
        assert user_roles == []

    def test_delegation_rules(self, client, world):
        op, h = make(world, "op", "acme", "workflow-operator")
        peer, _ = make(world, "peer", "acme")
        out, _ = make(world, "out", "globex")
        role_id = world.roles["workflow-operator"].id
        now = time.time()
        body = {
            "delegate_id": peer.id,
            "role_id": role_id,
            "start_time": now - 10,
            "end_time": now + 3600,
        }
        ok = client.post("/api/v1/delegations", json=body, headers=h)
        assert ok.status_code == 201, ok.text
        # a delegate gets the role while the delegation is active
        peer_h = world.headers(peer)
        assert client.get("/api/v1/auth/me", headers=peer_h).json()["data"][
            "roles"
        ] == ["workflow-operator"]
        assert (
            client.post(
                "/api/v1/delegations", json={**body, "delegate_id": out.id}, headers=h
            ).status_code
            == 404
        )
        assert (
            client.post(
                "/api/v1/delegations", json={**body, "end_time": now - 5}, headers=h
            ).status_code
            == 422
        )
        assert (
            client.post(
                "/api/v1/delegations", json={**body, "delegate_id": op.id}, headers=h
            ).status_code
            == 422
        )
        # revoked: gone from the delegate's roles
        assert (
            client.delete(
                f"/api/v1/delegations/{ok.json()['data']['id']}", headers=h
            ).status_code
            == 200
        )
        assert (
            client.get("/api/v1/auth/me", headers=peer_h).json()["data"]["roles"] == []
        )
        mine = client.get("/api/v1/delegations", headers=h).json()["data"]
        assert mine["made"] == []

    def test_organizations(self, client, world):
        root, hr = make(world, "root", "acme", "super-admin")
        admin, h = make(world, "adm", "acme", "org-admin")
        other, ho = make(world, "oth", "globex", "org-admin")
        assert (
            client.get(
                f"/api/v1/organizations/{admin.organization.id}", headers=h
            ).status_code
            == 200
        )
        assert (
            client.get(
                f"/api/v1/organizations/{admin.organization.id}", headers=ho
            ).status_code
            == 404
        )
        assert (
            client.post(
                "/api/v1/organizations", json={"name": "n", "slug": "n"}, headers=h
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/v1/organizations", json={"name": "n", "slug": "n"}, headers=hr
            ).status_code
            == 201
        )
        up = client.put(
            f"/api/v1/organizations/{admin.organization.id}",
            json={"name": "Renamed"},
            headers=h,
        )
        assert up.json()["data"]["name"] == "Renamed"


class TestCustomRoles:
    def test_role_permissions_take_effect(self, client, world):
        root, hr = make(world, "root", "acme", "super-admin")
        admin, ha = make(world, "adm", "acme", "org-admin")
        user, hu = make(world, "usr", "acme")
        # no role: no access
        assert client.get("/api/v1/teams", headers=hu).status_code == 403
        r = client.post(
            "/api/v1/roles",
            json={"name": "Lister", "slug": "lister", "permissions": ["team.manage"]},
            headers=hr,
        )
        assert r.status_code == 201, r.text
        assert r.json()["data"]["permissions"] == ["team.manage"]
        client.post("/api/v1/teams", json={"name": "T", "slug": "t"}, headers=ha)
        a = client.post(
            "/api/v1/roles/assign",
            json={
                "user_id": user.id,
                "role_id": r.json()["data"]["id"],
                "scope_type": "global",
            },
            headers=ha,
        )
        assert a.status_code == 201, a.text
        assert len(client.get("/api/v1/teams", headers=hu).json()["data"]) == 1
        # only what the role lists
        assert client.get("/api/v1/users", headers=hu).status_code == 403
        # revoking it removes the permissions with it
        client.delete(f"/api/v1/roles/assign/{a.json()['data']['id']}", headers=ha)
        assert client.get("/api/v1/teams", headers=hu).status_code == 403

    def test_a_custom_role_cannot_carry_super_admin_or_unknown_permissions(
        self, client, world
    ):
        root, hr = make(world, "root", "acme", "super-admin")
        for perms in (["super.admin"], ["no.such.permission"]):
            r = client.post(
                "/api/v1/roles",
                json={"name": "x", "slug": "x", "permissions": perms},
                headers=hr,
            )
            assert r.status_code == 422
