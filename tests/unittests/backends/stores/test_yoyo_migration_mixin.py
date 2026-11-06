from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from volnux.backends.stores.sqlite import SqliteStoreBackend


@pytest.fixture
def sqlite_store(tmp_path):
    db_path = tmp_path / "store.db"
    store = SqliteStoreBackend(database=db_path)
    try:
        yield store
    finally:
        store.close()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    d = tmp_path / "migrations"
    d.mkdir()
    return d


@pytest.fixture
def backend():
    backend = MagicMock()
    backend.lock.return_value.__enter__.return_value = None
    backend.lock.return_value.__exit__.return_value = None
    return backend


def test_run_migrations_applies_pending_migrations(
    sqlite_store, migrations_dir, backend
):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_apply.return_value = ["migration_001", "migration_002"]

    with patch(
        "volnux.backends.store.read_migrations", return_value=["raw_001", "raw_002"]
    ):
        count = sqlite_store.run_migrations(str(migrations_dir))

    assert count == 2
    backend.apply_migrations.assert_called_once_with(["migration_001", "migration_002"])
    backend.lock.assert_called_once()


def test_run_migrations_dry_run_does_not_apply(sqlite_store, migrations_dir, backend):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_apply.return_value = ["migration_001", "migration_002"]

    with patch(
        "volnux.backends.store.read_migrations", return_value=["raw_001", "raw_002"]
    ):
        count = sqlite_store.run_migrations(str(migrations_dir), dry_run=True)

    assert count == 2
    backend.apply_migrations.assert_not_called()
    backend.lock.assert_not_called()


def test_run_migrations_returns_zero_when_nothing_to_apply(
    sqlite_store, migrations_dir, backend
):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_apply.return_value = []

    with patch("volnux.backends.store.read_migrations", return_value=[]):
        count = sqlite_store.run_migrations(str(migrations_dir))

    assert count == 0
    backend.apply_migrations.assert_not_called()
    backend.lock.assert_not_called()


def test_rollback_migrations_rolls_back_limited_count(
    sqlite_store, migrations_dir, backend
):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_rollback.return_value = [
        "migration_003",
        "migration_002",
        "migration_001",
    ]

    with patch(
        "volnux.backends.store.read_migrations", return_value=["raw_001", "raw_002"]
    ):
        count = sqlite_store.rollback_migrations(str(migrations_dir), count=2)

    assert count == 2
    backend.rollback_migrations.assert_called_once_with(
        ["migration_003", "migration_002"]
    )
    backend.lock.assert_called_once()


def test_rollback_migrations_dry_run_does_not_execute(
    sqlite_store, migrations_dir, backend
):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_rollback.return_value = [
        "migration_003",
        "migration_002",
        "migration_001",
    ]

    with patch(
        "volnux.backends.store.read_migrations", return_value=["raw_001", "raw_002"]
    ):
        count = sqlite_store.rollback_migrations(
            str(migrations_dir),
            count=2,
            dry_run=True,
        )

    assert count == 2
    backend.rollback_migrations.assert_not_called()
    backend.rollback.assert_not_called()
    backend.lock.assert_not_called()


def test_rollback_all_migrations_rolls_back_everything(
    sqlite_store, migrations_dir, backend
):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_rollback.return_value = [
        "migration_003",
        "migration_002",
        "migration_001",
    ]

    with patch(
        "volnux.backends.store.read_migrations", return_value=["raw_001", "raw_002"]
    ):
        count = sqlite_store.rollback_all_migrations(str(migrations_dir))

    assert count == 3
    backend.rollback_migrations.assert_called_once_with(
        ["migration_003", "migration_002", "migration_001"]
    )
    backend.lock.assert_called_once()


def test_rollback_all_migrations_dry_run(sqlite_store, migrations_dir, backend):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_rollback.return_value = [
        "migration_003",
        "migration_002",
        "migration_001",
    ]

    with patch(
        "volnux.backends.store.read_migrations", return_value=["raw_001", "raw_002"]
    ):
        count = sqlite_store.rollback_all_migrations(str(migrations_dir), dry_run=True)

    assert count == 3
    backend.rollback_migrations.assert_not_called()
    backend.lock.assert_not_called()


def test_invalid_migrations_dir_raises_file_not_found(sqlite_store):
    with pytest.raises(FileNotFoundError):
        sqlite_store.run_migrations("/path/that/does/not/exist")


def test_rollback_raises_not_implemented_if_backend_does_not_support_selection(
    sqlite_store,
    migrations_dir,
):
    backend = MagicMock()
    del backend.to_rollback
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)

    with patch("volnux.backends.store.read_migrations", return_value=["raw_001"]):
        with pytest.raises(NotImplementedError):
            sqlite_store.rollback_migrations(str(migrations_dir))


def test_run_migrations_propagates_backend_errors(
    sqlite_store, migrations_dir, backend
):
    sqlite_store._get_migration_backend = MagicMock(return_value=backend)
    backend.to_apply.return_value = ["migration_001"]
    backend.apply_migrations.side_effect = RuntimeError("boom")

    with patch("volnux.backends.store.read_migrations", return_value=["raw_001"]):
        with pytest.raises(RuntimeError):
            sqlite_store.run_migrations(str(migrations_dir))
