"""Integration tests for restore + GC workflows via CLI."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cowfs.cli import app
from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore


def _init_storage(storage: Path) -> None:
    storage.mkdir(parents=True, exist_ok=True)
    (storage / ".cowfs").write_text(json.dumps({"version": 1, "hash_algo": "sha256"}))


def _seed_file_versions(storage: Path) -> tuple[int, list[str]]:
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")

    inode = db.create_file(parent_id=1, name="doc.txt", path="/doc.txt")
    hashes = []
    for payload in [b"alpha", b"beta", b"gamma"]:
        obj_hash = store.store_sync(payload)
        db.create_version(inode, obj_hash, len(payload))
        hashes.append(obj_hash)

    db.close()
    return inode, hashes


def test_restore_deleted_file_then_gc_keep_last(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _seed_file_versions(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.soft_delete_file(inode)
    db.close()

    runner = CliRunner()
    restore = runner.invoke(
        app, ["restore", "/doc.txt", "--version", "2", "--storage", str(storage), "--json"]
    )
    assert restore.exit_code == 0
    restore_payload = json.loads(restore.stdout)
    assert restore_payload["restored_from_version"] == 2
    assert restore_payload["target_hash"] == hashes[1]

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    file_row = db.get_file_by_path("/doc.txt")
    assert file_row is not None
    versions_after_restore = db.list_versions(inode)
    assert len(versions_after_restore) == 4
    assert versions_after_restore[-1]["object_hash"] == hashes[1]
    db.close()

    gc = runner.invoke(app, ["gc", "--storage", str(storage), "--keep-last", "1", "--json"])
    assert gc.exit_code == 0
    gc_payload = json.loads(gc.stdout)
    assert gc_payload["versions_pruned"] == 3
    assert gc_payload["processed_objects"] == 2

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    versions_final = db.list_versions(inode)
    assert len(versions_final) == 1
    assert versions_final[0]["object_hash"] == hashes[1]
    stats = db.get_stats()
    assert stats["total_objects"] == 1
    db.close()

    store = ObjectStore(storage / "objects")
    assert not store.exists(hashes[0])
    assert store.exists(hashes[1])
    assert not store.exists(hashes[2])


def test_restore_dry_run_does_not_change_versions(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _seed_file_versions(storage)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "restore",
            "/doc.txt",
            "--version",
            "1",
            "--storage",
            str(storage),
            "--dry-run",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["target_hash"] == hashes[0]

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    versions = db.list_versions(inode)
    assert len(versions) == 3
    assert versions[-1]["object_hash"] == hashes[2]
    db.close()


def test_snapshot_restore_flow_keep_new_toggle(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode, hashes = _seed_file_versions(storage)

    runner = CliRunner()
    create = runner.invoke(app, ["snapshot", "create", "base", "--storage", str(storage), "--json"])
    assert create.exit_code == 0

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    store = ObjectStore(storage / "objects")
    hash_delta = store.store_sync(b"delta")
    db.create_version(inode, hash_delta, 5)
    inode_new = db.create_file(parent_id=1, name="new.txt", path="/new.txt")
    hash_new = store.store_sync(b"new")
    db.create_version(inode_new, hash_new, 3)
    db.close()

    restore = runner.invoke(
        app, ["snapshot", "restore", "base", "--storage", str(storage), "--json"]
    )
    assert restore.exit_code == 0
    payload = json.loads(restore.stdout)
    assert payload["files_soft_deleted"] == 1

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    versions = db.list_versions(inode)
    assert versions[-1]["object_hash"] == hashes[-1]
    assert db.get_file_by_path("/new.txt") is None
    db.close()

    restore_keep = runner.invoke(
        app,
        ["snapshot", "restore", "base", "--keep-new", "--storage", str(storage), "--json"],
    )
    assert restore_keep.exit_code == 0
    payload_keep = json.loads(restore_keep.stdout)
    assert payload_keep["files_soft_deleted"] == 0
