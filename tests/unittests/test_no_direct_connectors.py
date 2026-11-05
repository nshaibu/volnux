"""Nothing outside ``volnux/backends`` opens a connection of its own.

``volnux/backends`` is the middleware between the connectors (Redis, Kafka,
databases) and the rest of the framework. Everything else reaches a store through
the models (``MessagingBackendIntegrationMixin``, ``KeyValueStoreIntegrationMixin``),
so the backend is a ``DATABASES`` setting and can be changed without touching code.
"""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2] / "volnux"

# client libraries that are connectors
FORBIDDEN = {
    "redis",
    "aioredis",
    "kafka",
    "aiokafka",
    "confluent_kafka",
    "pika",
    "aio_pika",
    "nats",
    "boto3",
    "botocore",
    "asyncpg",
    "psycopg",
    "pymongo",
    "motor",
    "memcache",
    "pymemcache",
}

# {file relative to volnux/: reason}
ALLOWED = {
    "execution/rehydrator/event/resources/builtins.py": (
        "snapshots a live psycopg2 cursor handle for rehydration: not a store"
    ),
}


def violations(source: str):
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            names = [node.module or ""]
        else:
            continue
        for name in names:
            if name.split(".")[0] in FORBIDDEN | {"psycopg2"}:
                found.append((node.lineno, name))
    return found


def test_no_connector_is_imported_outside_the_backends():
    problems = []
    for path in sorted(ROOT.rglob("*.py")):
        relative = path.relative_to(ROOT).as_posix()
        if relative.startswith("backends/") or relative in ALLOWED:
            continue
        for lineno, name in violations(path.read_text()):
            problems.append(f"volnux/{relative}:{lineno} imports {name}")
    assert not problems, (
        "Reach a store through a model (MessagingBackendIntegrationMixin / "
        "KeyValueStoreIntegrationMixin), not a client library:\n" + "\n".join(problems)
    )


def test_the_allowed_files_still_exist():
    for relative in ALLOWED:
        assert (ROOT / relative).exists(), f"stale allowlist entry: {relative}"


@pytest.mark.parametrize(
    "source",
    [
        "import redis",
        "import redis.asyncio as aioredis",
        "from redis.asyncio import Redis",
        "from kafka import KafkaProducer",
        "import psycopg2",
        "def f():\n    import confluent_kafka",
    ],
)
def test_the_scan_sees_the_imports_it_forbids(source):
    assert violations(source)


@pytest.mark.parametrize(
    "source",
    ["import redisx_not_a_client", "from . import redis", "import json"],
)
def test_the_scan_leaves_other_imports_alone(source):
    assert not violations(source)
