# COWFS — Copy-on-Write Filesystem

A userspace Copy-on-Write filesystem built with FUSE. Every write transparently creates a new version — enabling instant restore, snapshot tagging, and content deduplication.

## Quick Start

```bash
pip install cowfs

# Mount
cowfs mount ~/storage ~/mnt

# Use like a normal filesystem
echo "hello" > ~/mnt/test.txt
echo "world" > ~/mnt/test.txt

# Inspect version history
cowfs history /test.txt --storage ~/storage

# Unmount
cowfs umount ~/mnt
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

## Requirements

- Linux with FUSE support (`libfuse3-dev`)
- Python 3.11+
