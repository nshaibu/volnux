import difflib
import re
from typing import Any, Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
    DateTimeField,
    DTConfig,
)
from volnux.models.enums import WorkflowCategory, WorkflowMode, WorkflowStatus
from .utils import SEMVER

from .base import GovernanceModel
from .users import User, Team, Organization


_HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def apply_unified_diff(base_text: str, patch_text: str) -> str:
    """Apply a unified diff (as made by ``difflib.unified_diff``) to ``base_text``.

    Every hunk is placed by its ``@@ -start,count +start,count @@`` header, so the
    lines between hunks are kept and a patch with several changes applies correctly.

    :raises ValueError: a hunk does not match ``base_text``.
    """
    base = base_text.splitlines(keepends=True)
    out: list = []
    pos = 0  # next unread line of ``base``
    in_hunk = False
    for line in patch_text.splitlines(keepends=True):
        header = _HUNK.match(line)
        if header:
            start = int(header.group(1))
            count = int(header.group(2) if header.group(2) is not None else 1)
            # a hunk with no old lines is positioned *after* line ``start``
            target = start if count == 0 else start - 1
            if target < pos:
                raise ValueError("Overlapping hunks in the patch")
            out.extend(base[pos:target])
            pos = target
            in_hunk = True
        elif not in_hunk:
            continue  # the ---/+++ file header
        elif line.startswith("\\"):
            continue  # "\ No newline at end of file"
        elif line.startswith("-"):
            if pos >= len(base) or base[pos].rstrip("\r\n") != line[1:].rstrip("\r\n"):
                raise ValueError("The patch does not match the base text")
            pos += 1
        elif line.startswith("+"):
            out.append(line[1:])
        else:  # context line
            if pos >= len(base) or base[pos].rstrip("\r\n") != line[1:].rstrip("\r\n"):
                raise ValueError("The patch does not match the base text")
            out.append(base[pos])
            pos += 1
    out.extend(base[pos:])
    return "".join(out)


def make_delta(old_text: str, new_text: str) -> Optional[str]:
    """A unified diff turning ``old_text`` into ``new_text``, or ``None`` when a diff
    cannot be trusted to rebuild it (difflib does not mark a missing final newline,
    so such texts are stored whole instead)."""
    if (old_text and not old_text.endswith("\n")) or (
        new_text and not new_text.endswith("\n")
    ):
        return None
    return "".join(
        difflib.unified_diff(
            old_text.splitlines(keepends=True),
            new_text.splitlines(keepends=True),
            fromfile="old",
            tofile="new",
        )
    )


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
            nullable=True,
            default=None,
            reverse_name="updated_workflows",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    approval_chain: ForeignKeyField[
        "volnux.models.ApprovalChain",
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="workflows",
            on_delete=OnDelete.SET_NULL,
        ),
    ]

    published_at: DateTimeField[DTConfig(nullable=True)]


class WorkflowVersion(GovernanceModel):
    """Immutable record of a published workflow version."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="versions", on_delete=OnDelete.CASCADE),
    ]

    version: MiniAnnotated[str, Attrib(pattern=SEMVER)]  # e.g. "1.0.1"
    ast_hash: str  # SHA-256 of compiled AST (for cache/dedup)
    mode: MiniAnnotated[WorkflowMode, Attrib(default=WorkflowMode.CFG)]

    parent_version: ForeignKeyField[
        "volnux.models.WorkflowVersion",
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="child_versions",
            on_delete=OnDelete.SET_NULL,
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
        patch = make_delta(parent_full_text, new_pty_source)
        if patch is None:
            return new_pty_source, None  # not safely diffable: store it whole
        # Return None for pty_source, and the delta_patch string
        return None, patch

    def get_full_source(self) -> str:
        """The full source of this version, without reading the store.

        A keyframe (``pty_source`` set) returns its text; a delta needs its parent,
        which must already be resolved (``await version.parent_version.resolve_async()``)
        or use :meth:`get_full_source_async`.
        """
        if self.pty_source is not None:
            return self.pty_source
        if self.parent_version is None or self.delta_patch is None:
            return ""
        parent = self.parent_version
        if not isinstance(parent, WorkflowVersion):
            parent = parent.instance  # a resolved ForeignKey
        return apply_unified_diff(parent.get_full_source(), self.delta_patch)

    async def get_full_source_async(self) -> str:
        """The full source of this version, loading its parents from the store."""
        if self.pty_source is not None:
            return self.pty_source
        if self.parent_version is None or self.delta_patch is None:
            return ""
        parent = await WorkflowVersion.get(self.parent_version.id)
        return apply_unified_diff(
            await parent.get_full_source_async(), self.delta_patch
        )


class WorkflowVariable(GovernanceModel):
    """Declared variables in a workflow definition."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="variables", on_delete=OnDelete.CASCADE),
    ]
    name: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    value: Any
    is_environment: bool = False

    unique_together = (("workflow", "name"),)
    encrypted_fields = ("value",)


class WorkflowDescriptor(GovernanceModel):
    """User-defined descriptor labels for conditional branching (3-9)."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="descriptors", on_delete=OnDelete.CASCADE),
    ]
    descriptor_number: MiniAnnotated[int, Attrib(ge=3, le=9)]
    label: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]

    # A label is unique within its workflow, not across all workflows.
    unique_together = (("workflow", "label"),)
