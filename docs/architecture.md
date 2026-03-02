# Architecture

## High-level Components

- `fuse_handler.py`: FUSE operation handlers (lookup/read/write/unlink/rename)
- `metadata.py`: SQLite metadata layer for files, versions, objects, snapshots, events
- `object_store.py`: Content-addressable blob storage with hash-based paths
- `cli.py`: Operational commands (`history`, `restore`, `snapshot`, `gc`, `diff`, `log`)

## Storage Layout

```text
<storage_dir>/
  .cowfs
  .cowfs.lock
  metadata.db
  objects/
    ab/
      cdef...   # object hash shards
```

## Write Flow

1. New content hash is computed.
2. Blob is stored only if hash is not already present (dedup).
3. New version row is appended in SQLite.
4. File `current_version_id` is updated.
5. Activity event is recorded.

## Restore/Snapshot/GC Safety

- Multi-step operations use metadata transactions (`begin/commit/rollback`).
- Restore and snapshot restore create new versions (no in-place overwrite).
- GC prunes old versions (policy-based) and removes orphaned objects.
