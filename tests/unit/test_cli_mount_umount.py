"""Unit tests for mount/umount CLI commands."""

import json
import subprocess
from pathlib import Path

from typer.testing import CliRunner

from cowfs import cli


def test_mount_initializes_new_storage(tmp_path: Path, monkeypatch) -> None:
    storage = tmp_path / "storage"
    mount = tmp_path / "mnt"
    mount.mkdir()

    lock_state = {"acquired": False, "released": False}

    def fake_acquire(_storage: Path) -> int:
        lock_state["acquired"] = True
        return 123

    def fake_release(fd: int) -> None:
        assert fd == 123
        lock_state["released"] = True

    monkeypatch.setattr(cli, "_acquire_lock", fake_acquire)
    monkeypatch.setattr(cli, "_release_lock", fake_release)
    monkeypatch.setattr(cli.trio, "run", lambda *args, **kwargs: None)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["mount", str(storage), str(mount)])
    assert result.exit_code == 0
    assert (storage / cli.FORMAT_MARKER_FILE).exists()
    marker = json.loads((storage / cli.FORMAT_MARKER_FILE).read_text())
    assert marker["version"] == cli.FORMAT_VERSION
    assert marker["hash_algo"] == "sha256"
    assert lock_state["acquired"] and lock_state["released"]


def test_mount_rejects_non_empty_mountpoint(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    mount = tmp_path / "mnt"
    mount.mkdir()
    (mount / "existing.txt").write_text("x")

    runner = CliRunner()
    result = runner.invoke(cli.app, ["mount", str(storage), str(mount)])
    assert result.exit_code == 1


def test_mount_rejects_hash_algo_mismatch(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    mount = tmp_path / "mnt"
    mount.mkdir()
    storage.mkdir()
    (storage / cli.FORMAT_MARKER_FILE).write_text(
        json.dumps({"version": cli.FORMAT_VERSION, "hash_algo": "sha256"})
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app, ["mount", str(storage), str(mount), "--hash-algo", "blake3"]
    )
    assert result.exit_code == 1


def test_mount_rejects_nonvalid_nonempty_storage(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    mount = tmp_path / "mnt"
    mount.mkdir()
    storage.mkdir()
    (storage / "random.bin").write_bytes(b"x")

    runner = CliRunner()
    result = runner.invoke(cli.app, ["mount", str(storage), str(mount)])
    assert result.exit_code == 1


def test_umount_uses_fallback_binary(tmp_path: Path, monkeypatch) -> None:
    mount = tmp_path / "mnt"
    mount.mkdir()

    calls: list[list[str]] = []

    def fake_run(cmd, check, capture_output, text):  # type: ignore[no-untyped-def]
        calls.append(cmd)
        if cmd[0] == "fusermount3":
            raise FileNotFoundError
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["umount", str(mount)])
    assert result.exit_code == 0
    assert calls[0][0] == "fusermount3"
    assert calls[1][0] == "fusermount"


def test_umount_surfaces_called_process_error(tmp_path: Path, monkeypatch) -> None:
    mount = tmp_path / "mnt"
    mount.mkdir()

    def fake_run(cmd, check, capture_output, text):  # type: ignore[no-untyped-def]
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd, stderr="boom")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["umount", str(mount)])
    assert result.exit_code == 1


def test_umount_reports_missing_fusermount(tmp_path: Path, monkeypatch) -> None:
    mount = tmp_path / "mnt"
    mount.mkdir()

    def fake_run(cmd, check, capture_output, text):  # type: ignore[no-untyped-def]
        raise FileNotFoundError

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["umount", str(mount)])
    assert result.exit_code == 1
