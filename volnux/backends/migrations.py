import hashlib
import logging
from typing import Any, List, Optional, Tuple, Type

from volnux.backends.db_utils import migrate_models
from volnux.engine.workflows.workflow import WorkflowConfig
from volnux.models import (
    Organization,
    Team,
    User,
    Workflow,
    WorkflowVersion,
)
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.engine.workflows.registry import WorkflowRegistry, get_workflow_registry

logger = logging.getLogger("volnux.cli.migrator")


class VolnuxProjectMigrator:
    """
    Handles database migrations in 3 phases:
    1. Schema DDL Migration (via migrate_models)
    2. System Bootstrap Seeding (Default Org, Team, User)
    3. Workflow AST Sync (from WorkflowRegistry)
    """

    # List of system governance models that require schema migration
    MIGRATION_MODELS: List[Type[KeyValueStoreIntegrationMixin]] = [
        Organization,
        Team,
        User,
        Workflow,
        WorkflowVersion,
    ]

    def __init__(
        self, config: Any, workflow_registry: Optional[WorkflowRegistry] = None
    ):
        self.config = config
        self.workflow_registry = workflow_registry or get_workflow_registry()

    async def execute_full_migration(self, skip_workflows: bool = False) -> None:
        """
        Executes the full 3-phase migration process.
        """
        logger.info("Starting Volnux database migration...")

        await self._migrate_schema()

        org, team, user = await self._seed_bootstrap_entities()

        if not skip_workflows:
            await self._sync_workflows_from_registry(org, team, user)

        logger.info("Volnux database migration completed successfully!")

    async def _migrate_schema(self) -> None:
        """
        Phase 1: Applies table and constraint DDL migrations across all registered models.
        """
        logger.info("[Phase 1/3] Applying schema migrations for governance models...")
        for model in self.MIGRATION_MODELS:
            try:
                await migrate_models(model)
                logger.debug("Migrated schema for model: %s", model.__name__)
            except Exception as e:
                logger.error(
                    "Failed executing schema migration for %s: %s", model.__name__, e
                )
                raise

    async def _seed_bootstrap_entities(self) -> Tuple[Organization, Team, User]:
        """
        Phase 2: Idempotently seeds default system Organization, Team, and User.
        """
        logger.info(
            "[Phase 2/3] Seeding default organization, team, and system user..."
        )

        org_slug = self.config.get("DEFAULT_ORG_SLUG", "default-org")
        org_qs = await Organization.filter(slug=org_slug)
        org = await org_qs.first()
        if not org:
            org = Organization(name="Default Organization", slug=org_slug)
            await org.save()
            logger.info("Created default Organization: '%s'", org_slug)

        team_slug = self.config.get("DEFAULT_TEAM_SLUG", "default-team")
        team_qs = await Team.filter(slug=team_slug, organization=org.id)
        team = await team_qs.first()
        if not team:
            team = Team(name="Default Team", slug=team_slug, organization=org)
            await team.save()
            logger.info("Created default Team: '%s'", team_slug)

        user_email = self.config.get("DEFAULT_USER_EMAIL", "cli@volnux.local")
        user_qs = await User.filter(email=user_email)
        user = await user_qs.first()
        if not user:
            user = User(
                name="cli_developer", email=user_email, is_active=True, organization=org
            )
            await user.save()
            logger.info("Created default CLI User: '%s'", user_email)

        return org, team, user

    async def _sync_workflows_from_registry(
        self, org: Organization, team: Team, user: User
    ) -> None:
        """
        Phase 3: Pulls instantiated WorkflowConfig objects from the WorkflowRegistry,
        extracts AST and Pointy DSL text via get_pointy_ast(), and registers/updates versions in DB.
        """
        logger.info("[Phase 3/3] Syncing workflows from WorkflowRegistry...")

        # Ensure registry is populated if not loaded already
        if not self.workflow_registry.is_ready():
            raise RuntimeError("WorkflowRegistry is not ready. Please load it first.")

        workflows = self.workflow_registry.get_workflow_configs()

        if not workflows:
            logger.warning(
                "No workflows found in WorkflowRegistry. Skipping workflow sync."
            )
            return

        for wfconfig in workflows:
            try:
                ast_obj, pty_text = wfconfig.get_pointy_ast()
                ast_hash = hashlib.sha256(pty_text.encode("utf-8")).hexdigest()

                workflow_qs = await Workflow.filter(
                    name=wfconfig.name, organization_object_id=org.id
                )
                workflow = await workflow_qs.first()

                if not workflow:
                    workflow = Workflow(
                        name=wfconfig.name,
                        organization=org,
                        team=team,
                        created_by=user,
                        is_active=True,
                    )
                    logger.info("Registered new Workflow: '%s'", wfconfig.name)

                version_record_qs = await WorkflowVersion.filter(
                    workflow=workflow.id, ast_hash=ast_hash
                )
                version_record = await version_record_qs.first()

                if not version_record:
                    latest_version_qs = await WorkflowVersion.filter(
                        workflow=workflow.id
                    )
                    latest_version = await latest_version_qs.order_by(
                        "-created_at"
                    ).first()

                    version_str = self._calculate_next_version_str(latest_version)

                    await WorkflowVersion.create(
                        workflow=workflow,
                        version=version_str,
                        ast_hash=ast_hash,
                        pty_source=pty_text,
                        mode=getattr(ast_obj, "mode", "CFG"),
                        is_active=True,
                    )
                    logger.info(
                        "Registered new version '%s' for '%s' [%s]",
                        version_str,
                        wf_name,
                        ast_hash[:8],
                    )
                else:
                    logger.debug(
                        "Workflow '%s' is up to date [%s]", wf_name, ast_hash[:8]
                    )

            except Exception as e:
                logger.error(
                    "Failed to register workflow '%s' from registry: %s", wf_name, e
                )
                raise

    def _calculate_next_version_str(self, latest_version: Any) -> str:
        """Helper to increment semantic version strings (e.g., 1.0.0 -> 1.0.1)."""
        if not latest_version or not getattr(latest_version, "version", None):
            return "1.0.0"
        try:
            parts = [int(p) for p in latest_version.version.split(".")]
            parts[-1] += 1
            return ".".join(map(str, parts))
        except Exception:
            return "1.0.0"
