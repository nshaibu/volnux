"""
What a ``@source:`` declaration may say.

``@source: hub("trading_events", "trading-events", version="2.1.0")`` names a
namespace (``trading_events``, the prefix tasks use: ``trading_events::Validate``),
says where its events come from (a hub package, ``trading-events``) and pins a
version. The engine turns it into the same ``WorkflowSource`` that ``ready()``
builds by hand.

Pure data and functions, shared by semantic analysis (parse time) and the engine
(run time); nothing here imports the engine.
"""

import re
import typing

__all__ = [
    "KIND_ALIASES",
    "LOOKS_LIKE_VERSION",
    "RESERVED_NAMESPACES",
    "SOURCE_KEYWORDS",
    "SOURCE_KINDS",
    "VERSION_REQUIRED",
    "canonical_kind",
]

#: kind -> the ``RegistrySource`` value it stands for.
SOURCE_KINDS: typing.Dict[str, str] = {
    "hub": "hub",
    "git": "git",
    "github": "git",
    "pypi": "pypi",
    "local": "local",
}
KIND_ALIASES = {"github": "git"}

#: Namespaces a script cannot declare: tasks without a prefix live in ``local``.
RESERVED_NAMESPACES = frozenset({"local"})

#: Kinds that install a pinned release and so refuse to load without a version.
VERSION_REQUIRED = frozenset({"hub", "pypi"})

# keyword -> accepted Python types of its literal value (``location`` may be given
# as a keyword instead of the second positional argument).
_COMMON: typing.Dict[str, typing.Tuple[type, ...]] = {
    "location": (str,),
    "timeout": (int,),  # milliseconds
    "retries": (int,),
    "priority": (int,),
    "events": (list,),
}

#: kind -> keyword -> accepted types.
SOURCE_KEYWORDS: typing.Dict[str, typing.Dict[str, typing.Tuple[type, ...]]] = {
    "hub": {
        **_COMMON,
        "version": (str,),
        "token": (str,),
        "eventhub_url": (str,),
    },
    "pypi": {
        **_COMMON,
        "version": (str,),
        "token": (str,),
        "index_url": (str,),
    },
    "git": {
        **_COMMON,
        "version": (str,),  # a branch, tag or commit
        "token": (str,),
        "branch": (str,),
        "workflow_name": (str,),
        "as_package": (bool,),
    },
    "local": dict(_COMMON),
}

LOOKS_LIKE_VERSION = re.compile(r"^[vV]?\d+(\.\d+)*([-+.][0-9A-Za-z.\-+]+)?$")


def canonical_kind(kind: str) -> str:
    """``github`` -> ``git``; anything else unchanged."""
    return KIND_ALIASES.get(kind, kind)
