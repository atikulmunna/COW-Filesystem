"""COWFS CLI — companion command-line tool for the COW filesystem."""

import fcntl
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

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
console = Console()

FORMAT_MARKER_FILE = ".cowfs"
LOCK_FILE = ".cowfs.lock"
FORMAT_VERSION = 1


def _read_format_marker(storage_dir: Path) -> dict | None:
    marker_path = storage_dir / FORMAT_MARKER_FILE
    if not marker_path.exists():
        return None
    try:
        return json.loads(marker_path.read_text())
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
    except OSError:
        os.close(fd)
        console.print(
            f"[red]Error:[/red] Another COWFS instance is already mounted "
            f"on [bold]{storage_dir}[/bold]"
        )
        raise typer.Exit(1)
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
            subprocess.run(cmd + [str(mount_path)], check=True, capture_output=True, text=True)
            console.print(f"[green]Unmounted:[/green] {mount_path}")
            return
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError as e:
            console.print(f"[red]Error:[/red] {e.stderr.strip()}")
            raise typer.Exit(1)
    console.print("[red]Error:[/red] fusermount not found. Is FUSE installed?")
    raise typer.Exit(1)


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


def _human_size(size_bytes: int) -> str:
    if size_bytes == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} B"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


if __name__ == "__main__":
    app()
