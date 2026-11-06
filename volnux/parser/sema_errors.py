"""
Diagnostic types for Pointy semantic analysis.
"""

from __future__ import annotations

import typing
from dataclasses import dataclass, field
from enum import Enum

if typing.TYPE_CHECKING:
    from .ast import ASTNode


class SemanticLevel(Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass
class SemanticDiagnostic:
    """A single diagnostic produced by the semantic analyser."""

    level: SemanticLevel
    message: str
    node: typing.Optional["ASTNode"] = field(default=None, repr=False)

    def __str__(self) -> str:
        return f"[{self.level.value.upper()}] {self.message}"

    def is_error(self) -> bool:
        return self.level == SemanticLevel.ERROR

    def is_warning(self) -> bool:
        return self.level == SemanticLevel.WARNING


@dataclass
class SemanticResult:
    """Accumulates all diagnostics from a single semantic analysis pass."""

    errors: typing.List[SemanticDiagnostic] = field(default_factory=list)
    warnings: typing.List[SemanticDiagnostic] = field(default_factory=list)

    def add(self, diag: SemanticDiagnostic) -> None:
        if diag.level == SemanticLevel.ERROR:
            self.errors.append(diag)
        else:
            self.warnings.append(diag)

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    @property
    def all(self) -> typing.List[SemanticDiagnostic]:
        return self.errors + self.warnings

    def __bool__(self) -> bool:
        """True when the result is clean (no errors, no warnings)."""
        return not self.errors and not self.warnings

    def __repr__(self) -> str:
        return (
            f"SemanticResult(errors={len(self.errors)}, "
            f"warnings={len(self.warnings)})"
        )
