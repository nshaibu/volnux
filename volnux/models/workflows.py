import difflib
from typing import Any, Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
    DateTimeField,
    DTConfig,
)
from volnux.models.enums import WorkflowCategory, WorkflowStatus

from .base import GovernanceModel
from .users import User, Team, Organization


def apply_unified_diff(base_text: str, patch_text: str) -> str:
    """Helper to apply a unified diff string to a base string."""
    base_lines = base_text.splitlines(keepends=True)
    patch_lines = patch_text.splitlines(keepends=True)

    result = []
    i = 0
    # Process unified diff lines
    for line in patch_lines:
        if line.startswith("---") or line.startswith("+++") or line.startswith("@@"):
            continue
        elif line.startswith("-"):
            i += 1  # Skip removed line
        elif line.startswith("+"):
            result.append(line[1:])  # Add inserted line
        elif line.startswith(" "):
            if i < len(base_lines):
                result.append(base_lines[i])
                i += 1

    return "".join(result)


class Workflow(GovernanceModel):
    """The governed workflow artifact — the system of record.

    Reverse Relations:
        versions        — Version history
        variables — Declared variables
        descriptors — Descriptor labels
        executions — Execution history
        approval_steps — Approval chain steps
        hitl_requests — HITL requests
        trigger_configs — Trigger configurations
        break_glass_accesses — Break-glass access records
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    description: MiniAnnotated[Optional[str], Attrib(default=None)]
    category: MiniAnnotated[WorkflowCategory, Attrib(default=WorkflowCategory.STANDARD)]
    status: MiniAnnotated[WorkflowStatus, Attrib(default=WorkflowStatus.DRAFT)]
    # pointy_lang_source: str
    # compiled_graph: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    # version: MiniAnnotated[str, Attrib(default="0.1.0")]
    # mode: MiniAnnotated[str, Attrib(default="cfg")]

    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="workflows", on_delete=OnDelete.CASCADE),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(reverse_name="workflows", on_delete=OnDelete.PROTECT),
    ]
    created_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="created_workflows", on_delete=OnDelete.PROTECT),
    ]
    updated_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True, reverse_name="updated_workflows", on_delete=OnDelete.SET_NULL
        ),
    ]
    approval_chain: ForeignKeyField[
        "volnux.models.ApprovalChain",
        FKConfig(nullable=True, reverse_name="workflows", on_delete=OnDelete.SET_NULL),
    ]

    published_at: DateTimeField[DTConfig(nullable=True)]


class WorkflowVersion(GovernanceModel):
    """Immutable record of a published workflow version."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="versions", on_delete=OnDelete.CASCADE),
    ]

    version: str  # Semantic version e.g. "1.0.1"
    ast_hash: str  # SHA-256 of compiled AST (for cache/dedup)
    mode: MiniAnnotated[str, Attrib(default="CFG")]  # "CFG" or "DAG"

    parent_version: ForeignKeyField[
        "volnux.models.WorkflowVersion",
        FKConfig(
            nullable=True, reverse_name="child_versions", on_delete=OnDelete.SET_NULL
        ),
    ]

    # If delta_patch is None, this record is a Base Keyframe and stores full text in pty_source
    pty_source: MiniAnnotated[Optional[str], Attrib(default=None)]
    delta_patch: MiniAnnotated[Optional[str], Attrib(default=None)]

    is_active: MiniAnnotated[bool, Attrib(default=True)]
    changelog: MiniAnnotated[Optional[str], Attrib(default=None)]

    published_by: ForeignKeyField[
        User,
        FKConfig(
            reverse_name="published_workflow_versions", on_delete=OnDelete.PROTECT
        ),
    ]
    published_at: DateTimeField[DTConfig(auto_now_add=True)]

    @classmethod
    def create_delta(
        cls, parent_version: Optional["WorkflowVersion"], new_pty_source: str
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Calculates unified diff between the parent version source and new pty source.
        Returns tuple of (pty_source, delta_patch).
        """
        if not parent_version:
            # First version (Base Keyframe) — store full source
            return new_pty_source, None

        parent_full_text = parent_version.get_full_source()
        if parent_full_text == new_pty_source:
            # No changes — return empty delta
            return None, ""

        # Generate Unified Diff patch
        parent_lines = parent_full_text.splitlines(keepends=True)
        new_lines = new_pty_source.splitlines(keepends=True)

        diff = difflib.unified_diff(
            parent_lines,
            new_lines,
            fromfile=f"v{parent_version.version}",
            tofile="new_version",
        )
        patch_text = "".join(diff)

        # Return None for pty_source, and the delta_patch string
        return None, patch_text

    def get_full_source(self) -> str:
        """
        Reconstructs the full .pty source text by walking up the
        parent_version chain and applying unified diff patches.
        """
        if self.pty_source is not None:
            # Keyframe record containing full text
            return self.pty_source

        if not self.parent_version or not self.delta_patch:
            # Fallback if no parent or empty patch
            return ""

        # Fetch base text from parent recursively
        base_text = self.parent_version.get_full_source()

        # Apply patch
        return apply_unified_diff(base_text, self.delta_patch)


class WorkflowVariable(GovernanceModel):
    """Declared variables in a workflow definition."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="variables", on_delete=OnDelete.CASCADE),
    ]
    name: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    value: Any
    is_environment: bool = False


class WorkflowDescriptor(GovernanceModel):
    """User-defined descriptor labels for conditional branching (3-9)."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="descriptors", on_delete=OnDelete.CASCADE),
    ]
    descriptor_number: MiniAnnotated[int, Attrib(ge=3, le=9)]
    label: MiniAnnotated[
        str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$", metadata={"unique": True})
    ]
