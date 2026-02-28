"""Unit tests for `cowfs diff` CLI command."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cowfs.cli import app
from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore


def _init_storage(storage: Path) -> None:
    storage.mkdir(parents=True, exist_ok=True)
    (storage / ".cowfs").write_text(json.dumps({"version": 1, "hash_algo": "sha256"}))


def _seed_versions(storage: Path, versions: list[bytes]) -> tuple[int, list[str]]:
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode = db.create_file(parent_id=1, name="a.txt", path="/a.txt")
    hashes = []
    for content in versions:
        h = store.store_sync(content)
        db.create_version(inode, h, len(content))
        hashes.append(h)
    db.close()
    return inode, hashes


def test_diff_text_versions_json(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    _seed_versions(storage, [b"hello\nworld\n", b"hello\ncowfs\n"])

    runner = CliRunner()
    result = runner.invoke(
        app, ["diff", "/a.txt", "--v1", "1", "--v2", "2", "--storage", str(storage), "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "text"
    diff_blob = "\n".join(payload["diff"])
    assert "-world" in diff_blob
    assert "+cowfs" in diff_blob


def test_diff_current_vs_version_json(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    _seed_versions(storage, [b"v1\n", b"v2\n", b"v3\n"])

    runner = CliRunner()
    result = runner.invoke(
        app, ["diff", "/a.txt", "--version", "1", "--storage", str(storage), "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["left_version"] == 3
    assert payload["right_version"] == 1
    assert payload["mode"] == "text"


def test_diff_binary_versions_json(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    _seed_versions(storage, [b"\x00\x01\x02", b"\x00\x01\x02\x03"])

    runner = CliRunner()
    result = runner.invoke(
        app, ["diff", "/a.txt", "--v1", "1", "--v2", "2", "--storage", str(storage), "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "binary"
    assert payload["left_size"] == 3
    assert payload["right_size"] == 4
    assert payload["size_delta"] == 1


def test_diff_rejects_invalid_selector_combination(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    _seed_versions(storage, [b"a", b"b"])

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["diff", "/a.txt", "--v1", "1", "--v2", "2", "--version", "1", "--storage", str(storage)],
    )
    assert result.exit_code == 1
