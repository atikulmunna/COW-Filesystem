# Troubleshooting

## `ModuleNotFoundError: No module named 'fcntl'` on Windows

Cause: old package version.

Fix:

```powershell
python -m pip install -U --no-cache-dir cowfs==0.2.8
python -c "import cowfs; print(cowfs.__version__)"
```

## `Could not find a version that satisfies cowfs==X.Y.Z`

Cause: release propagation delay or stale index cache.

Fix:

```bash
python -m pip install --no-cache-dir --index-url https://pypi.org/simple cowfs==0.2.8
python -m pip index versions cowfs
```

## `Package fuse3 was not found` / `fuse3.pc not found`

Cause: Linux FUSE build dependencies are missing.

Fix:

```bash
sudo apt-get update
sudo apt-get install -y libfuse3-dev pkg-config build-essential python3-dev
```

## `Snapshot already exists: <name>`

Cause: duplicate snapshot name.

Fix:

```bash
cowfs snapshot list --storage ~/cowfs-storage
cowfs snapshot delete <name> --storage ~/cowfs-storage
# or create with a unique name
cowfs snapshot create <new-name> --storage ~/cowfs-storage
```

## `Software caused connection abort` under mount path

Cause: mount process terminated/disconnected.

Fix:

```bash
# inspect logs in mount terminal
cowfs umount ~/cowfs-mnt
mkdir -p ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
cowfs mount ~/cowfs-storage-fresh ~/cowfs-mnt-fresh
```

## `Another COWFS instance is already mounted`

Cause: active storage lock.

Fix:

```bash
cowfs umount ~/cowfs-mnt
```

## `Error: <mount_point> is not empty`

Cause: mount target has existing files.

Fix:

```bash
mkdir -p ~/empty-mnt
cowfs mount ~/cowfs-storage ~/empty-mnt
```

## Restore says success but content does not change

Cause: old package without restore cache fix.

Fix:

```bash
python -m pip install -U --no-cache-dir cowfs==0.2.8
```
