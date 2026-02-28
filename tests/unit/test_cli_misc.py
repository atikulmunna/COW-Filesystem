"""Additional CLI tests to improve command/path coverage."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cowfs import cli
from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore


def _init_storage(storage: Path) -> None:
    storage.mkdir(parents=True, exist_ok=True)
    (storage / ".cowfs").write_text(json.dumps({"version": 1, "hash_algo": "sha256"}))


def _seed_versions(storage: Path, name: str = "a.txt", payloads: list[bytes] | None = None) -> int:
    payloads = payloads or [b"one\n", b"two\n"]
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")
    inode = db.create_file(parent_id=1, name=name, path=f"/{name}")
    for data in payloads:
        h = store.store_sync(data)
        db.create_version(inode, h, len(data))
    db.close()
    return inode


def test_read_format_marker_invalid_or_non_dict(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    storage.mkdir()
    marker = storage / cli.FORMAT_MARKER_FILE
    marker.write_text("not-json")
    assert cli._read_format_marker(storage) is None
    marker.write_text(json.dumps(["not", "dict"]))
    assert cli._read_format_marker(storage) is None


def test_lock_helpers_error_path(monkeypatch, tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    storage.mkdir()

    def bad_flock(fd: int, op: int) -> None:  # type: ignore[no-untyped-def]
        raise OSError("busy")

    monkeypatch.setattr(cli.fcntl, "flock", bad_flock)
    runner = CliRunner()
    # Trigger _acquire_lock via mount with empty mount path and valid storage marker.
    (storage / cli.FORMAT_MARKER_FILE).write_text(json.dumps({"version": 1, "hash_algo": "sha256"}))
    mount = tmp_path / "mnt"
    mount.mkdir()
    result = runner.invoke(cli.app, ["mount", str(storage), str(mount)])
    assert result.exit_code == 1


def test_history_stats_diff_and_restore_error_branches(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode = _seed_versions(storage)

    runner = CliRunner()

    hist = runner.invoke(cli.app, ["history", "/a.txt", "--storage", str(storage)])
    assert hist.exit_code == 0

    stats = runner.invoke(cli.app, ["stats", "--storage", str(storage), "--json"])
    assert stats.exit_code == 0
    payload = json.loads(stats.stdout)
    assert payload["total_versions"] >= 2

    same = runner.invoke(
        cli.app, ["diff", "/a.txt", "--v1", "1", "--v2", "1", "--storage", str(storage)]
    )
    assert same.exit_code == 0

    missing = runner.invoke(cli.app, ["history", "/missing.txt", "--storage", str(storage)])
    assert missing.exit_code == 1

    bad_restore = runner.invoke(cli.app, ["restore", "/a.txt", "--storage", str(storage)])
    assert bad_restore.exit_code == 1

    bad_restore2 = runner.invoke(
        cli.app,
        [
            "restore",
            "/a.txt",
            "--version",
            "1",
            "--before",
            "2026-01-01",
            "--storage",
            str(storage),
        ],
    )
    assert bad_restore2.exit_code == 1

    out_of_range = runner.invoke(
        cli.app, ["restore", "/a.txt", "--version", "99", "--storage", str(storage)]
    )
    assert out_of_range.exit_code == 1

    # Snapshot show missing name path.
    show_missing = runner.invoke(
        cli.app, ["snapshot", "show", "missing", "--storage", str(storage)]
    )
    assert show_missing.exit_code == 1

    # Keep new + before constraint already validated in dedicated tests; verify parse error branch.
    bad_before = runner.invoke(
        cli.app, ["gc", "--storage", str(storage), "--before", "not-a-date", "--json"]
    )
    assert bad_before.exit_code == 2

    # Ensure seeded inode still present.
    db = MetadataDB(storage / "metadata.db")
    db.connect()
    assert db.get_file(inode) is not None
    db.close()


def test_resolve_storage_env_and_normalize_path(tmp_path: Path, monkeypatch) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    monkeypatch.setenv("COWFS_STORAGE", str(storage))
    resolved = cli._resolve_storage(None)
    assert resolved == storage.resolve()
    assert cli._normalize_file_path("a.txt") == "/a.txt"
    assert cli._normalize_file_path("/a.txt/") == "/a.txt"
    monkeypatch.setenv("COWFS_STORAGE", str(tmp_path / "missing"))
    assert cli._resolve_storage(None) is None
    monkeypatch.delenv("COWFS_STORAGE", raising=False)
    assert cli._resolve_storage(None) is None


def test_parse_datetime_and_human_size() -> None:
    dt = cli._parse_datetime("2026-01-01 00:00:00")
    assert dt.year == 2026
    assert cli._human_size(0) == "0 B"
    assert cli._human_size(1024).endswith("KB")


def test_log_command_json_and_limit(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _init_storage(storage)
    inode = _seed_versions(storage)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.soft_delete_file(inode)
    db.record_event("SNAPSHOT_CREATE", path="snapshot:baseline")
    db.close()

    runner = CliRunner()
    out = runner.invoke(
        cli.app,
        ["log", "--storage", str(storage), "--limit", "2", "--json"],
    )
    assert out.exit_code == 0
    rows = json.loads(out.stdout)
    assert len(rows) == 2
    assert rows[-1]["action"] == "SNAPSHOT_CREATE"
    assert rows[-1]["path"] == "snapshot:baseline"
