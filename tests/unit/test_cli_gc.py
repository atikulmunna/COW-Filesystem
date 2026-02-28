"""Unit tests for `cowfs gc` CLI command."""

import json
from datetime import datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from cowfs.cli import app
from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore


def _init_storage(storage: Path) -> None:
    storage.mkdir(parents=True, exist_ok=True)
    (storage / ".cowfs").write_text(json.dumps({"version": 1, "hash_algo": "sha256"}))


def _make_orphan(storage: Path) -> str:
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")

    obj_hash = store.store_sync(b"hello gc")
    assert db.db is not None
    db.db.execute(
        "INSERT INTO objects (hash, size_bytes, ref_count) VALUES (?, ?, 0)",
        (obj_hash, 8),
    )
    db.db.commit()
    db.close()
    return obj_hash


def test_gc_dry_run_does_not_delete(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    obj_hash = _make_orphan(storage)

    runner = CliRunner()
    result = runner.invoke(app, ["gc", "--storage", str(storage), "--dry-run", "--json"])
    assert result.exit_code == 0

    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["orphaned_objects"] == 1
    assert payload["processed_objects"] == 1
    assert payload["reclaimed_bytes"] == 8

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert db.get_object(obj_hash) is not None
    db.close()
    assert ObjectStore(storage / "objects").exists(obj_hash)


def test_gc_deletes_orphan_object_and_record(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    obj_hash = _make_orphan(storage)

    runner = CliRunner()
    result = runner.invoke(app, ["gc", "--storage", str(storage), "--json"])
    assert result.exit_code == 0

    payload = json.loads(result.stdout)
    assert payload["dry_run"] is False
    assert payload["processed_objects"] == 1
    assert payload["reclaimed_bytes"] == 8

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert db.get_object(obj_hash) is None
    db.close()
    assert not ObjectStore(storage / "objects").exists(obj_hash)


def test_gc_skips_objects_still_referenced_by_versions(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode = db.create_file(parent_id=1, name="b.txt", path="/b.txt")
    obj_hash = store.store_sync(b"referenced")
    db.create_version(inode, obj_hash, 10)
    db.decrement_ref_count(obj_hash)
    db.close()

    runner = CliRunner()
    result = runner.invoke(app, ["gc", "--storage", str(storage), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["processed_objects"] == 0
    assert payload["skipped_referenced"] == 1

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert db.get_object(obj_hash) is not None
    db.close()
    assert ObjectStore(storage / "objects").exists(obj_hash)


def _make_version_chain(storage: Path) -> tuple[int, list[str]]:
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode = db.create_file(parent_id=1, name="v.txt", path="/v.txt")

    hashes = []
    for payload in [b"v1", b"v2", b"v3"]:
        obj_hash = store.store_sync(payload)
        db.create_version(inode, obj_hash, len(payload))
        hashes.append(obj_hash)

    db.close()
    return inode, hashes


def test_gc_keep_last_dry_run_does_not_prune_versions(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _make_version_chain(storage)

    runner = CliRunner()
    result = runner.invoke(
        app, ["gc", "--storage", str(storage), "--keep-last", "1", "--dry-run", "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["keep_last"] == 1
    assert payload["versions_pruned"] == 2

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert len(db.list_versions(inode)) == 3
    db.close()
    store = ObjectStore(storage / "objects")
    for obj_hash in hashes:
        assert store.exists(obj_hash)


def test_gc_keep_last_prunes_old_versions_and_collects_orphans(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _make_version_chain(storage)

    runner = CliRunner()
    result = runner.invoke(app, ["gc", "--storage", str(storage), "--keep-last", "1", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["keep_last"] == 1
    assert payload["versions_pruned"] == 2
    assert payload["processed_objects"] == 2

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    remaining = db.list_versions(inode)
    assert len(remaining) == 1
    assert remaining[0]["object_hash"] == hashes[-1]
    db.close()

    store = ObjectStore(storage / "objects")
    assert not store.exists(hashes[0])
    assert not store.exists(hashes[1])
    assert store.exists(hashes[2])


def _set_version_created_at(storage: Path, version_id: int, created_at: str) -> None:
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert db.db is not None
    db.db.execute("UPDATE versions SET created_at = ? WHERE id = ?", (created_at, version_id))
    db.db.commit()
    db.close()


def test_gc_before_dry_run_projects_prune_without_changes(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _make_version_chain(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    versions = db.list_versions(inode)
    db.close()
    old = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    _set_version_created_at(storage, versions[0]["id"], old)
    _set_version_created_at(storage, versions[1]["id"], old)

    cutoff = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    runner = CliRunner()
    result = runner.invoke(
        app, ["gc", "--storage", str(storage), "--before", cutoff, "--dry-run", "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["before"] == cutoff
    assert payload["versions_pruned"] == 2
    assert payload["processed_objects"] == 2

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert len(db.list_versions(inode)) == 3
    db.close()
    store = ObjectStore(storage / "objects")
    for obj_hash in hashes:
        assert store.exists(obj_hash)


def test_gc_before_prunes_old_versions_but_keeps_current(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _make_version_chain(storage)

    future = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    runner = CliRunner()
    result = runner.invoke(app, ["gc", "--storage", str(storage), "--before", future, "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["versions_pruned"] == 2
    assert payload["processed_objects"] == 2

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    remaining = db.list_versions(inode)
    assert len(remaining) == 1
    assert remaining[0]["object_hash"] == hashes[-1]
    db.close()

    store = ObjectStore(storage / "objects")
    assert not store.exists(hashes[0])
    assert not store.exists(hashes[1])
    assert store.exists(hashes[2])


def test_gc_rejects_before_and_keep_last_together(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    _make_version_chain(storage)

    runner = CliRunner()
    cutoff = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = runner.invoke(
        app,
        ["gc", "--storage", str(storage), "--keep-last", "1", "--before", cutoff, "--json"],
    )
    assert result.exit_code == 1


def test_gc_rolls_back_metadata_when_object_delete_fails(tmp_path: Path, monkeypatch) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    obj_hash = _make_orphan(storage)

    def _boom(self, _hash: str) -> int:
        raise RuntimeError("disk failure")

    monkeypatch.setattr(ObjectStore, "delete_sync", _boom)

    runner = CliRunner()
    result = runner.invoke(app, ["gc", "--storage", str(storage), "--json"])
    assert result.exit_code == 1

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    # Row should still exist because transaction is rolled back.
    assert db.get_object(obj_hash) is not None
    db.close()
