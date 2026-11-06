from typing import List, Tuple, Optional, TYPE_CHECKING, Type
from collections import OrderedDict

from volnux.import_utils import import_string as import_model

if TYPE_CHECKING:
    from volnux.mixins import KeyValueStoreIntegrationMixin

# Model registry
# Models must be listed in dependency order.
# A model that has a ForeignKeyField to another model must come AFTER it.
# If a model cannot be imported (optional feature not installed), it is skipped
# with a warning rather than aborting the entire migration.
_ORDERED_MODELS: OrderedDict[str, str] = OrderedDict(
    [
        # Group 1: IAM — no foreign key dependencies
        (
            "Organization",
            "volnux.models.iam",
        ),
        (
            "Team",
            "volnux.models.iam",
        ),
        (
            "Role",
            "volnux.models.iam",
        ),
        (
            "User",
            "volnux.models.iam",
        ),
        (
            "RoleAssignment",
            "volnux.models.iam",
        ),
        (
            "TeamMember",
            "volnux.models.iam",
        ),
        # Group 2: Workflow — references User (created_by)
        (
            "Workflow",
            "volnux.models.workflow",
        ),
        (
            "WorkflowVersion",
            "volnux.models.workflow",
        ),
        # Group 3: Audit — references Workflow and User
        (
            "AuditEntry",
            "volnux.models.audit",
        ),
        # Group 4: Approval — references Workflow and User
        (
            "ApprovalChain",
            "volnux.models.approval",
        ),
        ("ApprovalStep", "volnux.models.approval"),
        # Group 5: Delegation — references User
        ("Delegation", "volnux.models.delegation"),
        ("DelegationAction", "volnux.models.delegation"),
        # Group 6: BreakGlass — references User and Workflow
        ("BreakGlassAccess", "volnux.models.break_glass"),
        ("BreakGlassAction", "volnux.models.break_glass"),
        # Group 7: Assets — no FK dependencies on governance models
        ("AssetMaterialisation", "volnux.assets.models"),
        # Group 8: HITL — references Workflow and User
        ("HITLRequest", "volnux.models.hitl"),
        # Group 9: Execution — references Workflow
        ("Execution", "volnux.models.execution"),
        ("ExecutionTrace", "volnux.models.execution"),
        # Group 10: Triggers — references Workflow
        ("TriggerConfig", "volnux.models.triggers"),
        ("TriggerStateRecord", "volnux.triggers.models"),
        # Group 11: Checkpoints — no FK constraints (keyed by task_id only)
        ("EventCheckpointSnapshot", "volnux.checkpointing.snapshot"),
    ]
)


def resolve_models(
    model_names: Optional[Tuple[str]] = None,
) -> Tuple[List[Type["KeyValueStoreIntegrationMixin"]], List[str]]:
    """
    Import all models in dependency order.

    :param model_names: A tuple of specific model names to import, or None to load
        all available models.
    :type model_names: Optional[Tuple[str]]
    :return: A tuple containing two lists:
        - A list of successfully imported model classes.
        - A list of human-readable names of models that could not be imported, along
          with the reason they failed.
    :rtype: Tuple[List[Type[KeyValueStoreIntegrationMixin]], List[str]]
    """

    models = []
    skipped = []

    models_dict = {}

    if not model_names:
        models_dict = _ORDERED_MODELS
    else:
        for model in model_names:
            try:
                models_dict[model] = _ORDERED_MODELS[model]
            except KeyError:
                skipped.append(f"Model {model} not found in registry")

        if not models_dict:
            return models, skipped

    for module_path, class_name in models_dict.items():
        try:
            cls = import_model(f"{module_path}.{class_name}")
            models.append(cls)
        except ImportError:
            skipped.append(f"{module_path}.{class_name}  [module not installed]")
        except AttributeError:
            skipped.append(f"{module_path}.{class_name}  [class not found in module]")
        except Exception as exc:
            skipped.append(f"{module_path}.{class_name}  [{exc}]")

    return models, skipped
