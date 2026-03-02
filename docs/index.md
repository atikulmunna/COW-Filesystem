# COWFS

COWFS is a userspace Copy-on-Write filesystem built with FUSE.
Every write creates a new immutable version, enabling restore, snapshots,
and storage deduplication.

## Key Features

- Copy-on-Write versioning on each write
- Content-addressable object storage (SHA-256)
- File history and point-in-time restore
- Snapshot create/list/show/restore/delete
- Garbage collection (`--dry-run`, `--keep-last`, `--before`)
- Version diff and chronological activity log
- CLI built with Typer + Rich

## Install (Linux/WSL2)

```bash
python -m venv /tmp/cowfs-test
source /tmp/cowfs-test/bin/activate
sudo apt-get update
sudo apt-get install -y libfuse3-dev pkg-config build-essential python3-dev
python -m pip install -U pip
python -m pip install --no-cache-dir cowfs==0.2.8
cowfs --help
```

Windows native PowerShell supports install and CLI help, but mount is Linux-only.
Use WSL2 Ubuntu for filesystem mounting.

## Quick Start

```bash
# Mount
cowfs mount ~/storage ~/mnt

# Write multiple versions
echo "hello" > ~/mnt/test.txt
echo "world" > ~/mnt/test.txt

# Inspect
cowfs history /test.txt --storage ~/storage
cowfs stats --storage ~/storage
cowfs log --storage ~/storage --limit 20

# Restore
cowfs restore /test.txt --version 1 --storage ~/storage

# Unmount
cowfs umount ~/mnt
```

## Requirements

- Linux with FUSE support (`libfuse3-dev`)
- Python 3.11+
- Windows users should run COWFS in WSL2 (Ubuntu)

## Next Pages

- CLI commands: `CLI Reference`
- Test commands: `Testing`
- Error handling and fixes: `Troubleshooting`
