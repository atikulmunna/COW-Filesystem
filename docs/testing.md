# Testing

## Install Sanity

```bash
python -m pip install -U --no-cache-dir cowfs==0.2.8
python -m pip show cowfs | rg 'Version|Location'
python -c "import cowfs; print(cowfs.__version__)"
cowfs --help
```

## Functional Smoke (WSL2/Linux)

Terminal 1:

```bash
mkdir -p ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
cowfs mount ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
```

Terminal 2:

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

## JSON Output Checks

```bash
cowfs history /a.txt --storage ~/cowfs-storage-fresh --json
cowfs stats --storage ~/cowfs-storage-fresh --json
cowfs gc --storage ~/cowfs-storage-fresh --dry-run --json
cowfs snapshot list --storage ~/cowfs-storage-fresh --json
cowfs log --storage ~/cowfs-storage-fresh --limit 10 --json
```

## Developer Test Suite

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/unit tests/integration tests/stress --cov=src/cowfs --cov-report=term-missing
ruff check src tests
mypy src tests --ignore-missing-imports
```
