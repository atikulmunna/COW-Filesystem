"""Unit tests for `cowfs snapshot` CLI commands."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cowfs.cli import app
from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore


def _init_storage(storage: Path) -> None:
    storage.mkdir(parents=True, exist_ok=True)
    (storage / ".cowfs").write_text(json.dumps({"version": 1, "hash_algo": "sha256"}))


def _seed_file(db: MetadataDB, store: ObjectStore, name: str, content: bytes) -> tuple[int, str]:
    inode = db.create_file(parent_id=1, name=name, path=f"/{name}")
    obj_hash = store.store_sync(content)
    db.create_version(inode, obj_hash, len(content))
    return inode, obj_hash


def test_snapshot_create_list_delete(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    _seed_file(db, store, "a.txt", b"a1")
    db.close()

    runner = CliRunner()
    create = runner.invoke(
        app, ["snapshot", "create", "baseline", "--storage", str(storage), "--json"]
    )
    assert create.exit_code == 0
    created = json.loads(create.stdout)
    assert created["name"] == "baseline"
    assert created["file_count"] == 1

    listing = runner.invoke(app, ["snapshot", "list", "--storage", str(storage), "--json"])
    assert listing.exit_code == 0
    snapshots = json.loads(listing.stdout)
    assert len(snapshots) == 1
    assert snapshots[0]["name"] == "baseline"

    deletion = runner.invoke(
        app, ["snapshot", "delete", "baseline", "--storage", str(storage), "--json"]
    )
    assert deletion.exit_code == 0
    deleted = json.loads(deletion.stdout)
    assert deleted["deleted"] is True

    listing = runner.invoke(app, ["snapshot", "list", "--storage", str(storage), "--json"])
    assert listing.exit_code == 0
    snapshots = json.loads(listing.stdout)
    assert snapshots == []


def test_snapshot_restore_default_soft_deletes_new_files(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode_a, hash_a1 = _seed_file(db, store, "a.txt", b"a1")
    db.close()

    runner = CliRunner()
    snap = runner.invoke(app, ["snapshot", "create", "s1", "--storage", str(storage), "--json"])
    assert snap.exit_code == 0

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    hash_a2 = store.store_sync(b"a2")
    db.create_version(inode_a, hash_a2, 2)
    _seed_file(db, store, "b.txt", b"b1")
    db.close()

    restore = runner.invoke(app, ["snapshot", "restore", "s1", "--storage", str(storage), "--json"])
    assert restore.exit_code == 0
    payload = json.loads(restore.stdout)
    assert payload["files_restored"] == 1
    assert payload["files_soft_deleted"] == 1

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    versions_a = db.list_versions(inode_a)
    assert versions_a[-1]["object_hash"] == hash_a1
    b_live = db.get_file_by_path("/b.txt")
    b_any = db.get_file_by_path("/b.txt", include_deleted=True)
    assert b_live is None
    assert b_any is not None
    assert b_any["is_deleted"] == 1
    db.close()


def test_snapshot_restore_keep_new_preserves_post_snapshot_files(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode_a, hash_a1 = _seed_file(db, store, "a.txt", b"a1")
    db.close()

    runner = CliRunner()
    snap = runner.invoke(app, ["snapshot", "create", "s1", "--storage", str(storage), "--json"])
    assert snap.exit_code == 0

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    hash_a2 = store.store_sync(b"a2")
    db.create_version(inode_a, hash_a2, 2)
    _seed_file(db, store, "b.txt", b"b1")
    db.close()

    restore = runner.invoke(
        app, ["snapshot", "restore", "s1", "--keep-new", "--storage", str(storage), "--json"]
    )
    assert restore.exit_code == 0
    payload = json.loads(restore.stdout)
    assert payload["files_soft_deleted"] == 0

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    b_live = db.get_file_by_path("/b.txt")
    assert b_live is not None
    versions_a = db.list_versions(inode_a)
    assert versions_a[-1]["object_hash"] == hash_a1
    db.close()


def test_snapshot_show_json_lists_snapshot_entries(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    _, hash_a = _seed_file(db, store, "a.txt", b"a1")
    _, hash_b = _seed_file(db, store, "b.txt", b"bee")
    db.close()

    runner = CliRunner()
    create = runner.invoke(app, ["snapshot", "create", "s1", "--storage", str(storage), "--json"])
    assert create.exit_code == 0

    show = runner.invoke(app, ["snapshot", "show", "s1", "--storage", str(storage), "--json"])
    assert show.exit_code == 0
    rows = json.loads(show.stdout)
    assert len(rows) == 2
    paths = {r["path"] for r in rows}
    hashes = {r["hash"] for r in rows}
    assert paths == {"/a.txt", "/b.txt"}
    assert hashes == {hash_a, hash_b}


def test_snapshot_restore_rolls_back_on_failure(tmp_path: Path, monkeypatch) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode_a, _ = _seed_file(db, store, "a.txt", b"a1")
    db.close()

    runner = CliRunner()
    snap = runner.invoke(app, ["snapshot", "create", "s1", "--storage", str(storage), "--json"])
    assert snap.exit_code == 0

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    hash_a2 = store.store_sync(b"a2")
    db.create_version(inode_a, hash_a2, 2)
    _seed_file(db, store, "b.txt", b"b1")
    before_versions = len(db.list_versions(inode_a))
    db.close()

    original_create_version = MetadataDB.create_version

    def _boom(self, file_id, object_hash, size_bytes, *, commit=True):
        raise RuntimeError("injected failure")

    monkeypatch.setattr(MetadataDB, "create_version", _boom)
    restore = runner.invoke(app, ["snapshot", "restore", "s1", "--storage", str(storage), "--json"])
    assert restore.exit_code == 1
    monkeypatch.setattr(MetadataDB, "create_version", original_create_version)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    # New file should not be soft-deleted because transaction rolls back.
    assert db.get_file_by_path("/b.txt") is not None
    # Original file should still point to latest pre-restore version.
    versions_a = db.list_versions(inode_a)
    assert len(versions_a) == before_versions
    assert versions_a[-1]["object_hash"] == hash_a2
    db.close()
