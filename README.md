# COWFS - Copy-on-Write Filesystem

[![PyPI](https://img.shields.io/pypi/v/cowfs)](https://pypi.org/project/cowfs/)

A userspace Copy-on-Write filesystem built with FUSE. Every write creates a new version, enabling instant restore, snapshot workflows, activity logs, and deduplication.

## Status

- Core FUSE read/write path implemented
- Version history + restore implemented
- Snapshot lifecycle implemented (`create`, `list`, `show`, `restore`, `delete`)
- GC implemented (`--dry-run`, `--keep-last`, `--before`)
- Diff + global activity log implemented
- CI implemented (tests, coverage, static analysis, Windows smoke)
- PyPI publishing implemented and validated (`cowfs` `0.2.8` live)

## Platform Support

- Linux: fully supported
- WSL2 (Ubuntu): fully supported
- Windows native PowerShell:
  - install and `cowfs --help` are supported
  - `cowfs mount` is not supported (use WSL2)

## Requirements

- Python 3.11+
- Linux/WSL2 mount support requires:
  - `libfuse3-dev`
  - `pkg-config`
  - `build-essential`
  - `python3-dev`

## Install

### Linux / WSL2 (recommended)

```bash
python -m venv /tmp/cowfs-test
source /tmp/cowfs-test/bin/activate
sudo apt-get update
sudo apt-get install -y libfuse3-dev pkg-config build-essential python3-dev
python -m pip install -U pip
python -m pip install --no-cache-dir cowfs==0.2.8
cowfs --help
```

### Windows PowerShell (CLI-only install)

```powershell
python -m pip install -U pip
python -m pip install --no-cache-dir cowfs==0.2.8
python -m pip show cowfs
python -c "import cowfs; print(cowfs.__version__)"
cowfs --help
```

Expected on Windows native:
- install succeeds
- `cowfs --help` works
- `cowfs mount ...` exits with Linux-only guidance

## First End-to-End Run (WSL2/Linux)

Use two terminals.

### Terminal 1: mount and keep running

```bash
mkdir -p ~/cowfs-storage ~/cowfs-mnt
cowfs mount ~/cowfs-storage ~/cowfs-mnt
```

### Terminal 2: file operations and verification

```bash
# create versions
echo "hello" > ~/cowfs-mnt/test.txt
echo "world" > ~/cowfs-mnt/test.txt

# inspect
cowfs history /test.txt --storage ~/cowfs-storage
cowfs diff /test.txt --v1 1 --v2 2 --storage ~/cowfs-storage
cowfs stats --storage ~/cowfs-storage
cowfs log --storage ~/cowfs-storage --limit 20

# restore
cowfs restore /test.txt --version 1 --storage ~/cowfs-storage
cat ~/cowfs-mnt/test.txt

# snapshot cycle
cowfs snapshot create baseline --storage ~/cowfs-storage
echo "new file" > ~/cowfs-mnt/new.txt
cowfs snapshot list --storage ~/cowfs-storage
cowfs snapshot restore baseline --storage ~/cowfs-storage
ls -la ~/cowfs-mnt

# garbage collection dry run
cowfs gc --storage ~/cowfs-storage --dry-run
```

### Terminal 1: unmount

```bash
# If mounted foreground is active, Ctrl+C first, then:
cowfs umount ~/cowfs-mnt
```

## Full CLI Commands

```bash
cowfs mount <storage_dir> <mount_point> [--debug]
cowfs umount <mount_point>

cowfs history <file_path> --storage <storage_dir> [--json]
cowfs restore <file_path> (--version <n> | --before "<datetime>") --storage <storage_dir> [--dry-run] [--json]
cowfs log --storage <storage_dir> [--limit <n>] [--action <name>] [--path-prefix <prefix>] [--since "<datetime>"] [--until "<datetime>"] [--json]

cowfs diff <file_path> (--version <n> | --v1 <n> --v2 <m>) --storage <storage_dir> [--json]
cowfs stats --storage <storage_dir> [--json]
cowfs gc --storage <storage_dir> [--dry-run] [--keep-last <n>] [--before "<datetime>"] [--json]

cowfs snapshot create <name> [--description "..."] --storage <storage_dir> [--json]
cowfs snapshot list --storage <storage_dir> [--json]
cowfs snapshot show <name> --storage <storage_dir> [--json]
cowfs snapshot restore <name> [--keep-new] --storage <storage_dir> [--dry-run] [--json]
cowfs snapshot delete <name> --storage <storage_dir> [--json]
```

## Testing Commands (Complete Set)

### Package install sanity

```bash
python -m pip install -U --no-cache-dir cowfs==0.2.8
python -m pip show cowfs | rg 'Version|Location'
python -c "import cowfs; print(cowfs.__version__)"
cowfs --help
```

### WSL2/Linux functional smoke

```bash
mkdir -p ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
cowfs mount ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
```

In second terminal:

```bash
echo "v1" > ~/cowfs-mnt-fresh/a.txt
echo "v2" > ~/cowfs-mnt-fresh/a.txt
cowfs history /a.txt --storage ~/cowfs-storage-fresh
cowfs restore /a.txt --version 1 --storage ~/cowfs-storage-fresh
cat ~/cowfs-mnt-fresh/a.txt
cowfs snapshot create snap1 --storage ~/cowfs-storage-fresh
echo "tmp" > ~/cowfs-mnt-fresh/tmp.txt
cowfs snapshot restore snap1 --storage ~/cowfs-storage-fresh
ls -la ~/cowfs-mnt-fresh
cowfs gc --storage ~/cowfs-storage-fresh --dry-run
```

### JSON output checks

```bash
cowfs history /a.txt --storage ~/cowfs-storage-fresh --json
cowfs stats --storage ~/cowfs-storage-fresh --json
cowfs gc --storage ~/cowfs-storage-fresh --dry-run --json
cowfs snapshot list --storage ~/cowfs-storage-fresh --json
cowfs log --storage ~/cowfs-storage-fresh --limit 10 --json
```

### Developer test suite (repo clone)

```bash
git clone https://github.com/atikulmunna/COW-Filesystem.git
cd COW-Filesystem
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/unit tests/integration tests/stress --cov=src/cowfs --cov-report=term-missing
ruff check src tests
mypy src tests --ignore-missing-imports
```

## Troubleshooting

### `ModuleNotFoundError: No module named 'fcntl'` on Windows

Cause:
- You are using an older package version.

Fix:

```powershell
python -m pip install -U --no-cache-dir cowfs==0.2.8
python -c "import cowfs; print(cowfs.__version__)"
```

### `Could not find a version that satisfies cowfs==X.Y.Z`

Cause:
- PyPI index propagation delay after release.

Fix:

```bash
python -m pip install --no-cache-dir --index-url https://pypi.org/simple cowfs==0.2.8
python -m pip index versions cowfs
```

### `Package fuse3 was not found` / `fuse3.pc not found`

Cause:
- Missing Linux FUSE build dependencies.

Fix:

```bash
sudo apt-get update
sudo apt-get install -y libfuse3-dev pkg-config build-essential python3-dev
```

### `Error: Snapshot already exists: <name>`

Cause:
- Duplicate snapshot name.

Fix:

```bash
cowfs snapshot list --storage ~/cowfs-storage
cowfs snapshot delete <name> --storage ~/cowfs-storage
# or use a new name
cowfs snapshot create <new-name> --storage ~/cowfs-storage
```

### `Software caused connection abort` while writing under mount path

Cause:
- Mount process crashed or disconnected.

Fix:

```bash
# check mount terminal logs first
cowfs umount ~/cowfs-mnt
mkdir -p ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
cowfs mount ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
```

### `Error: Another COWFS instance is already mounted`

Cause:
- Storage lock file is held by an active mount process.

Fix:

```bash
# stop old mount process, then retry
cowfs umount ~/cowfs-mnt
```

### `Error: <mount_point> is not empty`

Cause:
- Mount target directory already has files.

Fix:

```bash
mkdir -p ~/empty-mnt
cowfs mount ~/cowfs-storage ~/empty-mnt
```

### Restore says success but content does not change

Cause:
- You are on an old version without the restore cache fix.

Fix:

```bash
python -m pip install -U --no-cache-dir cowfs==0.2.8
```

## Documentation Site

```bash
pip install "mkdocs>=1.6.0,<2.0.0" "mkdocs-material>=9.5.0"
mkdocs serve
mkdocs build
```

## Release

- Tag-based publishing is configured in `.github/workflows/publish.yml`
- Release instructions: [RELEASE.md](RELEASE.md)
- Tag format: `vX.Y.Z`

### Release Checklist

```bash
# 1) bump version in pyproject.toml
git add pyproject.toml
git commit -m "Release X.Y.Z"
git push origin master

# 2) tag and push
git tag vX.Y.Z
git push origin vX.Y.Z

# 3) verify install once publish succeeds
python -m pip install -U --no-cache-dir cowfs==X.Y.Z
python -c "import cowfs; print(cowfs.__version__)"
```
