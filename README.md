# COWFS - Copy-on-Write Filesystem

A userspace Copy-on-Write filesystem built with FUSE. Every write transparently creates a new version - enabling instant restore, snapshot tagging, and content deduplication.

## Status

- Core FUSE read/write path implemented
- Version history + restore implemented
- GC implemented (`--dry-run`, `--keep-last`, `--before`)
- Snapshot lifecycle implemented (`create`, `list`, `show`, `restore`, `delete`)
- GitHub Actions CI workflow added (tests + coverage + required static analysis)
- release/publish polish items are still pending

## Quick Start

```bash
pip install cowfs

# Mount
cowfs mount ~/storage ~/mnt

# Use like a normal filesystem
echo "hello" > ~/mnt/test.txt
echo "world" > ~/mnt/test.txt

# Inspect version history and stats
cowfs history /test.txt --storage ~/storage
cowfs stats --storage ~/storage

# Restore to an older version
cowfs restore /test.txt --version 1 --storage ~/storage

# Create and inspect snapshot
cowfs snapshot create baseline --storage ~/storage
cowfs snapshot list --storage ~/storage
cowfs snapshot show baseline --storage ~/storage

# Restore filesystem to snapshot
cowfs snapshot restore baseline --storage ~/storage
# keep files created after snapshot:
cowfs snapshot restore baseline --keep-new --storage ~/storage

# Garbage collection
cowfs gc --storage ~/storage --dry-run
cowfs gc --storage ~/storage --keep-last 3
cowfs gc --storage ~/storage --before "2026-01-01 00:00:00"

# Unmount when done
cowfs umount ~/mnt
```

## CLI Commands

```bash
cowfs mount <storage_dir> <mount_point> [--debug]
cowfs umount <mount_point>

cowfs history <file_path> --storage <storage_dir> [--json]
cowfs restore <file_path> (--version <n> | --before "<datetime>") --storage <storage_dir> [--dry-run] [--json]

cowfs stats --storage <storage_dir> [--json]
cowfs gc --storage <storage_dir> [--dry-run] [--keep-last <n>] [--before "<datetime>"] [--json]

cowfs snapshot create <name> [--description "..."] --storage <storage_dir> [--json]
cowfs snapshot list --storage <storage_dir> [--json]
cowfs snapshot show <name> --storage <storage_dir> [--json]
cowfs snapshot restore <name> [--keep-new] --storage <storage_dir> [--dry-run] [--json]
cowfs snapshot delete <name> --storage <storage_dir> [--json]
```

## Development

```bash
git clone https://github.com/yourusername/cowfs.git
cd cowfs
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Release

- Tag-based publishing is configured in `.github/workflows/publish.yml`.
- See full release instructions in [RELEASE.md](RELEASE.md).
- Expected tag format: `vX.Y.Z` (example: `v0.2.0`).

## Requirements

- Linux with FUSE support (`libfuse3-dev`)
- Python 3.11+
