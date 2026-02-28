"""COWFS CLI — companion command-line tool for the COW filesystem."""

import difflib
import fcntl
import json
import logging
import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import pyfuse3
import trio
import typer
from rich.console import Console
from rich.table import Table

from cowfs.fuse_handler import COWFS

app = typer.Typer(
    name="cowfs",
    help="COWFS — Copy-on-Write Filesystem Manager",
    no_args_is_help=True,
)
snapshot_app = typer.Typer(help="Manage filesystem snapshots")
app.add_typer(snapshot_app, name="snapshot")
console = Console()

FORMAT_MARKER_FILE = ".cowfs"
LOCK_FILE = ".cowfs.lock"
FORMAT_VERSION = 1


def _read_format_marker(storage_dir: Path) -> dict[str, Any] | None:
    marker_path = storage_dir / FORMAT_MARKER_FILE
    if not marker_path.exists():
        return None
    try:
        parsed = json.loads(marker_path.read_text())
        if isinstance(parsed, dict):
            return parsed
        return None
    except (json.JSONDecodeError, OSError):
        return None


def _write_format_marker(storage_dir: Path, hash_algo: str = "sha256") -> None:
    marker = {
        "version": FORMAT_VERSION,
        "created": datetime.now().isoformat(),
        "hash_algo": hash_algo,
    }
    (storage_dir / FORMAT_MARKER_FILE).write_text(json.dumps(marker, indent=2))


def _acquire_lock(storage_dir: Path) -> int:
    lock_path = storage_dir / LOCK_FILE
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        os.close(fd)
        console.print(
            f"[red]Error:[/red] Another COWFS instance is already mounted "
            f"on [bold]{storage_dir}[/bold]"
        )
        raise typer.Exit(1) from e
    return fd


def _release_lock(fd: int) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    except OSError:
        pass


@app.command()
def mount(
    storage_dir: str = typer.Argument(..., help="Storage backend directory"),
    mount_point: str = typer.Argument(..., help="Directory to mount COWFS at"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
    auto_snapshot: bool = typer.Option(False, "--auto-snapshot"),
    hash_algo: str = typer.Option("sha256", "--hash-algo"),
) -> None:
    """Mount the COWFS filesystem."""
    storage_path = Path(storage_dir).resolve()
    mount_path = Path(mount_point).resolve()

    # Validate mount point
    if not mount_path.exists():
        mount_path.mkdir(parents=True)
    if not mount_path.is_dir():
        console.print(f"[red]Error:[/red] {mount_path} is not a directory")
        raise typer.Exit(1)
    if list(mount_path.iterdir()):
        console.print(f"[red]Error:[/red] {mount_path} is not empty")
        raise typer.Exit(1)

    # Initialize or validate storage
    if not storage_path.exists():
        storage_path.mkdir(parents=True)
        _write_format_marker(storage_path, hash_algo)
        console.print(f"Initialized new COWFS storage at [bold]{storage_path}[/bold]")
    else:
        marker = _read_format_marker(storage_path)
        if marker is None:
            if not any(storage_path.iterdir()):
                _write_format_marker(storage_path, hash_algo)
                console.print(f"Initialized new COWFS storage at [bold]{storage_path}[/bold]")
            else:
                console.print(f"[red]Error:[/red] {storage_path} is not a valid COWFS storage")
                raise typer.Exit(1)
        else:
            if marker.get("version", 0) > FORMAT_VERSION:
                console.print(f"[red]Error:[/red] Unsupported format version {marker['version']}")
                raise typer.Exit(1)
            stored_algo = marker.get("hash_algo", "sha256")
            if stored_algo != hash_algo:
                console.print(
                    f"[red]Error:[/red] Storage uses {stored_algo}, "
                    f"cannot switch to {hash_algo}"
                )
                raise typer.Exit(1)

    lock_fd = _acquire_lock(storage_path)

    log_level = logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(level=log_level, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    try:
        trio.run(_run_fuse, storage_path, mount_path, debug)
    except KeyboardInterrupt:
        console.print("\n[yellow]Caught interrupt, unmounting...[/yellow]")
    finally:
        _release_lock(lock_fd)
        console.print("[green]COWFS unmounted.[/green]")


async def _run_fuse(storage_path: Path, mount_path: Path, debug: bool) -> None:
    fs = COWFS(str(storage_path))
    fs.init()

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add("fsname=cowfs")
    if debug:
        fuse_options.add("debug")

    pyfuse3.init(fs, str(mount_path), fuse_options)

    console.print(
        f"[green]COWFS mounted:[/green] [bold]{storage_path}[/bold] → [bold]{mount_path}[/bold]"
    )
    console.print("Press Ctrl+C to unmount")

    try:
        await pyfuse3.main()
    finally:
        fs.shutdown()
        pyfuse3.close(unmount=True)


@app.command()
def umount(
    mount_point: str = typer.Argument(..., help="Mount point to unmount"),
) -> None:
    """Unmount the COWFS filesystem."""
    mount_path = Path(mount_point).resolve()
    for cmd in [["fusermount3", "-u"], ["fusermount", "-u"]]:
        try:
            subprocess.run([*cmd, str(mount_path)], check=True, capture_output=True, text=True)
            console.print(f"[green]Unmounted:[/green] {mount_path}")
            return
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError as e:
            console.print(f"[red]Error:[/red] {e.stderr.strip()}")
            raise typer.Exit(1) from e
    console.print("[red]Error:[/red] fusermount not found. Is FUSE installed?")
    raise typer.Exit(1)


@app.command()
def restore(
    file_path: str = typer.Argument(..., help="File path"),
    version: int | None = typer.Option(None, "--version", "-v", help="Version number (1-based)"),
    before: str | None = typer.Option(
        None,
        "--before",
        help='Restore latest version before timestamp (e.g. "2026-02-23 10:02:00")',
    ),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Restore a file to a previous version."""
    from cowfs.metadata import MetadataDB

    if (version is None) == (before is None):
        console.print("[red]Error:[/red] Provide exactly one of --version or --before")
        raise typer.Exit(1)

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()

    try:
        db.begin()
        normalized = _normalize_file_path(file_path)
        file_row = db.get_file_by_path(normalized, include_deleted=True)
        if file_row is None:
            console.print(f"[red]Error:[/red] File not found: {normalized}")
            raise typer.Exit(1)

        target = None
        selected_version_number = None

        if version is not None:
            versions = db.list_versions(file_row["id"])
            if version < 1 or version > len(versions):
                console.print(
                    f"[red]Error:[/red] Version {version} out of range (1..{len(versions)})"
                )
                raise typer.Exit(1)
            target = versions[version - 1]
            selected_version_number = version
        else:
            before_dt = _parse_datetime(before)
            target = db.get_latest_version_before(
                file_row["id"], before_dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            if target is None:
                console.print(f"[red]Error:[/red] No version found before {before}")
                raise typer.Exit(1)
            versions = db.list_versions(file_row["id"])
            for idx, v in enumerate(versions, 1):
                if v["id"] == target["id"]:
                    selected_version_number = idx
                    break

        assert target is not None
        assert selected_version_number is not None

        result = {
            "path": normalized,
            "restored_from_version": selected_version_number,
            "target_hash": target["object_hash"],
            "target_size": target["size_bytes"],
            "dry_run": dry_run,
        }

        if not dry_run:
            db.create_version(
                file_row["id"],
                target["object_hash"],
                target["size_bytes"],
                commit=False,
                action="RESTORE",
            )
            if file_row["is_deleted"]:
                db.set_file_deleted(file_row["id"], False, commit=False)
        db.commit()

        if output_json:
            console.print_json(json.dumps(result))
        else:
            action = "Would restore" if dry_run else "Restored"
            console.print(
                f"{action} [bold]{normalized}[/bold] "
                f"to version [bold]{selected_version_number}[/bold] "
                f"(hash={target['object_hash'][:12]}..., size={_human_size(target['size_bytes'])})"
            )
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


@app.command()
def history(
    file_path: str = typer.Argument(..., help="File path"),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Show version history of a file."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()

    try:
        normalized = _normalize_file_path(file_path)
        file_row = db.get_file_by_path(normalized)
        if file_row is None:
            console.print(f"[red]Error:[/red] File not found: {normalized}")
            raise typer.Exit(1)

        versions = db.list_versions(file_row["id"])
        current_vid = file_row["current_version_id"]

        if output_json:
            data = []
            for i, v in enumerate(versions, 1):
                data.append({
                    "version": i,
                    "id": v["id"],
                    "date": v["created_at"],
                    "size": v["size_bytes"],
                    "hash": v["object_hash"],
                    "current": v["id"] == current_vid,
                })
            console.print_json(json.dumps(data))
        else:
            table = Table(title=f"Version History: {normalized}")
            table.add_column("Ver", style="bold")
            table.add_column("Date")
            table.add_column("Size", justify="right")
            table.add_column("Hash")
            for i, v in enumerate(versions, 1):
                marker = " *" if v["id"] == current_vid else ""
                table.add_row(
                    f"{i}{marker}",
                    v["created_at"],
                    _human_size(v["size_bytes"]),
                    v["object_hash"][:12] + "...",
                )
            console.print(table)
    finally:
        db.close()


@app.command()
def log(
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    limit: int = typer.Option(50, "--limit", "-n", min=1, max=5000),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Show chronological activity feed across files and snapshots."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    try:
        events = db.list_events(limit=limit)
        if output_json:
            data = [
                {
                    "time": row["created_at"],
                    "action": row["action"],
                    "path": row["path"],
                    "version_id": row["version_id"],
                    "hash": row["object_hash"],
                }
                for row in events
            ]
            console.print_json(json.dumps(data))
            return

        table = Table(title=f"Activity Log (last {len(events)})")
        table.add_column("Time")
        table.add_column("Action", style="bold")
        table.add_column("Path")
        table.add_column("Version")
        table.add_column("Hash")
        for row in events:
            version = str(row["version_id"]) if row["version_id"] is not None else "-"
            short_hash = (row["object_hash"][:12] + "...") if row["object_hash"] else "-"
            table.add_row(
                row["created_at"],
                row["action"],
                row["path"] or "-",
                version,
                short_hash,
            )
        console.print(table)
    finally:
        db.close()


@app.command()
def diff(
    file_path: str = typer.Argument(..., help="File path"),
    v1: int | None = typer.Option(None, "--v1", help="First version number (1-based)"),
    v2: int | None = typer.Option(None, "--v2", help="Second version number (1-based)"),
    version: int | None = typer.Option(
        None, "--version", help="Diff current version against this version number"
    ),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Show diff between file versions."""
    from cowfs.metadata import MetadataDB
    from cowfs.object_store import ObjectStore

    by_pair = v1 is not None or v2 is not None
    by_current = version is not None
    if by_pair == by_current:
        console.print("[red]Error:[/red] Use either (--v1 and --v2) or --version")
        raise typer.Exit(1)
    if by_pair and (v1 is None or v2 is None):
        console.print("[red]Error:[/red] Both --v1 and --v2 are required together")
        raise typer.Exit(1)

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    store = ObjectStore(storage_path / "objects")
    try:
        normalized = _normalize_file_path(file_path)
        file_row = db.get_file_by_path(normalized, include_deleted=True)
        if file_row is None:
            console.print(f"[red]Error:[/red] File not found: {normalized}")
            raise typer.Exit(1)

        versions = db.list_versions(file_row["id"])
        if not versions:
            console.print(f"[red]Error:[/red] No versions available for {normalized}")
            raise typer.Exit(1)

        if by_current:
            left_num: int | None = len(versions)
            right_num: int | None = version
        else:
            left_num = v1
            right_num = v2
        assert left_num is not None and right_num is not None

        if left_num < 1 or left_num > len(versions) or right_num < 1 or right_num > len(versions):
            console.print(f"[red]Error:[/red] Version out of range (1..{len(versions)})")
            raise typer.Exit(1)

        left = versions[left_num - 1]
        right = versions[right_num - 1]
        left_data = store.read_sync(left["object_hash"])
        right_data = store.read_sync(right["object_hash"])

        binary = _is_binary(left_data) or _is_binary(right_data)
        if binary:
            result = {
                "path": normalized,
                "mode": "binary",
                "left_version": left_num,
                "right_version": right_num,
                "left_size": len(left_data),
                "right_size": len(right_data),
                "size_delta": len(right_data) - len(left_data),
                "same_content": left["object_hash"] == right["object_hash"],
            }
            if output_json:
                console.print_json(json.dumps(result))
            else:
                console.print(
                    f"Binary diff {normalized} (v{left_num} -> v{right_num}): "
                    f"{_human_size(len(left_data))} -> {_human_size(len(right_data))} "
                    f"(delta {result['size_delta']} B)"
                )
            return

        left_text = left_data.decode("utf-8")
        right_text = right_data.decode("utf-8")
        diff_lines = list(
            difflib.unified_diff(
                left_text.splitlines(),
                right_text.splitlines(),
                fromfile=f"{normalized}@v{left_num}",
                tofile=f"{normalized}@v{right_num}",
                lineterm="",
            )
        )
        result = {
            "path": normalized,
            "mode": "text",
            "left_version": left_num,
            "right_version": right_num,
            "diff": diff_lines,
        }

        if output_json:
            console.print_json(json.dumps(result))
        else:
            if not diff_lines:
                console.print(f"No differences for {normalized} (v{left_num} vs v{right_num})")
            else:
                for line in diff_lines:
                    console.print(line)
    finally:
        db.close()


@app.command()
def stats(
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Show storage statistics."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()

    try:
        s = db.get_stats()
        marker = _read_format_marker(storage_path)
        hash_algo = marker.get("hash_algo", "sha256") if marker else "sha256"

        dedup_savings = s["logical_size_bytes"] - s["actual_size_bytes"]
        dedup_pct = (
            (dedup_savings / s["logical_size_bytes"] * 100) if s["logical_size_bytes"] > 0 else 0.0
        )

        if output_json:
            s["dedup_savings_bytes"] = dedup_savings
            s["dedup_percentage"] = round(dedup_pct, 1)
            s["hash_algo"] = hash_algo
            console.print_json(json.dumps(s))
        else:
            console.print("[bold]COWFS Storage Statistics[/bold]")
            console.print(f"  Hash algorithm:   {hash_algo}")
            console.print(f"  Logical size:     {_human_size(s['logical_size_bytes'])}")
            console.print(f"  Actual size:      {_human_size(s['actual_size_bytes'])}")
            console.print(f"  Dedup savings:    {_human_size(dedup_savings)} ({dedup_pct:.1f}%)")
            console.print(f"  Total files:      {s['total_files']}")
            console.print(f"  Total versions:   {s['total_versions']}")
            console.print(f"  Total objects:    {s['total_objects']}")
            console.print(f"  Orphaned objects: {s['orphaned_objects']}")
    finally:
        db.close()


@app.command(name="gc")
def gc_command(
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    keep_last: int | None = typer.Option(None, "--keep-last", min=1),
    before: str | None = typer.Option(None, "--before"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Collect unreferenced objects from storage."""
    from cowfs.metadata import MetadataDB
    from cowfs.object_store import ObjectStore

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)
    if keep_last is not None and before is not None:
        console.print("[red]Error:[/red] Use either --keep-last or --before, not both")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    store = ObjectStore(storage_path / "objects")

    try:
        db.begin()
        pruned_rows = []
        before_sql: str | None = None
        if before is not None:
            before_dt = _parse_datetime(before)
            before_sql = before_dt.strftime("%Y-%m-%d %H:%M:%S")
            if dry_run:
                pruned_rows = db.list_prunable_versions_before(before_sql)
            else:
                pruned_rows = db.prune_versions_before(before_sql, commit=False)
        elif keep_last is not None:
            if dry_run:
                pruned_rows = db.list_prunable_versions(keep_last)
            else:
                pruned_rows = db.prune_versions_keep_last(keep_last, commit=False)

        orphans = db.get_orphaned_objects()

        processed_objects = 0
        reclaimed_bytes = 0
        missing_on_disk = 0
        skipped_referenced = 0
        versions_pruned = len(pruned_rows)
        versions_pruned_bytes = sum(row["size_bytes"] for row in pruned_rows)

        if dry_run:
            projected_orphan_hashes = {obj["hash"] for obj in orphans}
            projected_decrements: dict[str, int] = {}
            for row in pruned_rows:
                obj_hash = row["object_hash"]
                projected_decrements[obj_hash] = projected_decrements.get(obj_hash, 0) + 1
            for obj_hash, dec in projected_decrements.items():
                obj = db.get_object(obj_hash)
                if obj is not None and obj["ref_count"] - dec <= 0:
                    projected_orphan_hashes.add(obj_hash)

            processed_objects = len(projected_orphan_hashes)
            for obj_hash in projected_orphan_hashes:
                obj = db.get_object(obj_hash)
                if obj is not None:
                    reclaimed_bytes += obj["size_bytes"]
        else:
            for obj in orphans:
                obj_hash = obj["hash"]

                try:
                    db.delete_object_record(obj_hash, commit=False)
                except sqlite3.IntegrityError:
                    skipped_referenced += 1
                    continue

                freed = store.delete_sync(obj_hash)
                if freed == 0:
                    missing_on_disk += 1
                reclaimed_bytes += freed
                processed_objects += 1

        db.commit()

        result = {
            "dry_run": dry_run,
            "keep_last": keep_last,
            "before": before,
            "versions_pruned": versions_pruned,
            "versions_pruned_bytes": versions_pruned_bytes,
            "orphaned_objects": len(orphans),
            "processed_objects": processed_objects,
            "reclaimed_bytes": reclaimed_bytes,
            "missing_on_disk": missing_on_disk,
            "skipped_referenced": skipped_referenced,
        }

        if output_json:
            console.print_json(json.dumps(result))
        else:
            action = "Would collect" if dry_run else "Collected"
            console.print(
                f"{action} {processed_objects} orphaned object(s), "
                f"reclaimed {_human_size(reclaimed_bytes)}"
            )
            if keep_last is not None:
                console.print(
                    f"Pruned {versions_pruned} old version(s) "
                    f"(logical {_human_size(versions_pruned_bytes)})"
                )
            if before_sql is not None:
                console.print(
                    f"Pruned {versions_pruned} version(s) older than {before_sql} "
                    f"(logical {_human_size(versions_pruned_bytes)})"
                )
            if missing_on_disk > 0 and not dry_run:
                console.print(
                    f"[yellow]Warning:[/yellow] {missing_on_disk} object(s) missing on disk"
                )
            if skipped_referenced > 0 and not dry_run:
                console.print(
                    f"[yellow]Warning:[/yellow] "
                    f"Skipped {skipped_referenced} object(s) still referenced by versions"
                )
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


@snapshot_app.command("create")
def snapshot_create(
    name: str = typer.Argument(..., help="Snapshot name"),
    description: str | None = typer.Option(None, "--description"),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Create a named filesystem snapshot."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    try:
        try:
            snapshot_id = db.create_snapshot(name, description=description)
        except sqlite3.IntegrityError as e:
            console.print(f"[red]Error:[/red] Snapshot already exists: {name}")
            raise typer.Exit(1) from e
        db.record_event("SNAPSHOT_CREATE", path=f"snapshot:{name}")
        snapshot = db.get_snapshot_by_name(name)
        file_count = len(db.get_snapshot_entries(snapshot_id))
        result = {
            "id": snapshot_id,
            "name": name,
            "description": description,
            "created_at": snapshot["created_at"] if snapshot else None,
            "file_count": file_count,
        }
        if output_json:
            console.print_json(json.dumps(result))
        else:
            console.print(f"Created snapshot [bold]{name}[/bold] with {file_count} file(s)")
    finally:
        db.close()


@snapshot_app.command("list")
def snapshot_list(
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """List snapshots."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    try:
        snapshots = db.list_snapshots()
        if output_json:
            data = [
                {
                    "id": s["id"],
                    "name": s["name"],
                    "description": s["description"],
                    "created_at": s["created_at"],
                    "file_count": s["file_count"],
                }
                for s in snapshots
            ]
            console.print_json(json.dumps(data))
            return

        table = Table(title="Snapshots")
        table.add_column("Name", style="bold")
        table.add_column("Created")
        table.add_column("Files", justify="right")
        table.add_column("Description")
        for s in snapshots:
            table.add_row(
                s["name"],
                s["created_at"],
                str(s["file_count"]),
                s["description"] or "",
            )
        console.print(table)
    finally:
        db.close()


@snapshot_app.command("delete")
def snapshot_delete(
    name: str = typer.Argument(..., help="Snapshot name"),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Delete a snapshot."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    try:
        snapshot = db.get_snapshot_by_name(name)
        if snapshot is None:
            console.print(f"[red]Error:[/red] Snapshot not found: {name}")
            raise typer.Exit(1)
        db.delete_snapshot(snapshot["id"])
        db.record_event("SNAPSHOT_DELETE", path=f"snapshot:{name}")
        result = {"deleted": True, "name": name}
        if output_json:
            console.print_json(json.dumps(result))
        else:
            console.print(f"Deleted snapshot [bold]{name}[/bold]")
    finally:
        db.close()


@snapshot_app.command("show")
def snapshot_show(
    name: str = typer.Argument(..., help="Snapshot name"),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Show files captured in a snapshot."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    try:
        snapshot = db.get_snapshot_by_name(name)
        if snapshot is None:
            console.print(f"[red]Error:[/red] Snapshot not found: {name}")
            raise typer.Exit(1)

        entries = db.get_snapshot_entries_detailed(snapshot["id"])
        if output_json:
            data = [
                {
                    "path": e["path"],
                    "version_id": e["version_id"],
                    "hash": e["object_hash"],
                    "size": e["size_bytes"],
                    "created_at": e["created_at"],
                }
                for e in entries
            ]
            console.print_json(json.dumps(data))
            return

        table = Table(title=f"Snapshot: {name}")
        table.add_column("Path", style="bold")
        table.add_column("Version")
        table.add_column("Size", justify="right")
        table.add_column("Hash")
        table.add_column("Created")
        for e in entries:
            table.add_row(
                e["path"],
                str(e["version_id"]),
                _human_size(e["size_bytes"]),
                e["object_hash"][:12] + "...",
                e["created_at"],
            )
        console.print(table)
    finally:
        db.close()


@snapshot_app.command("restore")
def snapshot_restore(
    name: str = typer.Argument(..., help="Snapshot name"),
    keep_new: bool = typer.Option(False, "--keep-new"),
    storage_dir: str = typer.Option(None, "--storage", "-s"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    output_json: bool = typer.Option(False, "--json"),
) -> None:
    """Restore filesystem state to a snapshot."""
    from cowfs.metadata import MetadataDB

    storage_path = _resolve_storage(storage_dir)
    if storage_path is None:
        console.print("[red]Error:[/red] Could not find storage directory. Use --storage.")
        raise typer.Exit(1)

    db = MetadataDB(storage_path / "metadata.db")
    db.connect()
    try:
        snapshot = db.get_snapshot_by_name(name)
        if snapshot is None:
            console.print(f"[red]Error:[/red] Snapshot not found: {name}")
            raise typer.Exit(1)

        entries = db.get_snapshot_entries(snapshot["id"])
        snapshot_file_ids = {e["file_id"] for e in entries}
        active_file_ids = set(db.list_active_file_ids())
        file_ids_to_delete = [] if keep_new else sorted(active_file_ids - snapshot_file_ids)

        files_restored = 0
        files_soft_deleted = len(file_ids_to_delete)
        skipped_missing_versions = 0

        if not dry_run:
            for file_id in file_ids_to_delete:
                db.soft_delete_file(file_id, commit=False)

            for entry in entries:
                version = db.get_version(entry["version_id"])
                if version is None:
                    skipped_missing_versions += 1
                    continue
                db.create_version(
                    entry["file_id"],
                    version["object_hash"],
                    version["size_bytes"],
                    commit=False,
                    action="SNAPSHOT_RESTORE",
                )
                db.set_file_deleted(entry["file_id"], False, commit=False)
                files_restored += 1
            db.record_event("SNAPSHOT_RESTORE", path=f"snapshot:{name}", commit=False)
        else:
            files_restored = len(entries)

        db.commit()

        result = {
            "snapshot": name,
            "dry_run": dry_run,
            "keep_new": keep_new,
            "files_in_snapshot": len(entries),
            "files_restored": files_restored,
            "files_soft_deleted": files_soft_deleted,
            "skipped_missing_versions": skipped_missing_versions,
        }
        if output_json:
            console.print_json(json.dumps(result))
        else:
            action = "Would restore" if dry_run else "Restored"
            console.print(
                f"{action} snapshot [bold]{name}[/bold]: "
                f"restored {files_restored}, soft-deleted {files_soft_deleted}"
            )
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def _resolve_storage(storage_dir: str | None) -> Path | None:
    if storage_dir:
        p = Path(storage_dir).resolve()
        if p.exists() and (p / FORMAT_MARKER_FILE).exists():
            return p
        return None
    env = os.environ.get("COWFS_STORAGE")
    if env:
        p = Path(env).resolve()
        if p.exists() and (p / FORMAT_MARKER_FILE).exists():
            return p
    return None


def _normalize_file_path(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return path


def _parse_datetime(value: str | None) -> datetime:
    if value is None:
        raise typer.BadParameter("Missing datetime value")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            raise typer.BadParameter(
                f"Invalid datetime '{value}'. Use ISO format like '2026-02-23T10:02:00'."
            ) from e


def _is_binary(data: bytes) -> bool:
    if b"\x00" in data:
        return True
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return True
    return False


def _human_size(size_bytes: int) -> str:
    size = float(size_bytes)
    if size == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size) < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} PB"


if __name__ == "__main__":
    app()
