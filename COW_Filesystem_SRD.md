# COWFS — Copy-on-Write Filesystem
## Software Requirements & Specification Document

**Version:** 1.0  
**Author:** Atikul Islam Munna  
**Date:** February 2026  
**Status:** Draft

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Goals & Success Criteria](#2-goals--success-criteria)
3. [System Architecture](#3-system-architecture)
4. [Core Concepts](#4-core-concepts)
5. [Feature Specification](#5-feature-specification)
6. [Workflow & Data Flow](#6-workflow--data-flow)
7. [Storage Backend Design](#7-storage-backend-design)
8. [Database Schema](#8-database-schema)
9. [FUSE Operations](#9-fuse-operations)
10. [CLI Interface Design](#10-cli-interface-design)
11. [Tech Stack & Tools](#11-tech-stack--tools)
12. [Task Breakdown & Milestones](#12-task-breakdown--milestones)
13. [Testing Plan](#13-testing-plan)
14. [Non-Functional Requirements](#14-non-functional-requirements)
15. [Benchmarking Plan](#15-benchmarking-plan)
16. [Future Roadmap](#16-future-roadmap)

---

## 1. Project Overview

**COWFS** is a userspace Copy-on-Write filesystem implemented using FUSE (Filesystem in Userspace). It provides a mountable filesystem where every write operation transparently creates a new version of the modified file without overwriting the original. Users interact with it like a normal filesystem, while COWFS silently maintains a complete version history of every file — enabling instant restore, snapshot tagging, and content deduplication.

### Motivation

Modern filesystems like ZFS and Btrfs implement COW at the kernel level. Docker uses COW for image layers. PostgreSQL uses MVCC (a database-level COW variant) for transaction isolation. Git's entire object store is COW-based. Yet most engineers who use these tools daily have no idea how COW actually works underneath. Building COWFS from scratch bridges that gap and produces a genuinely useful tool in the process.

### What Makes It Unique

- Fully userspace — no kernel module, no root required to understand it
- Content-addressable object store (like git's blob storage)
- Filesystem-level versioning transparent to any application
- Snapshot tagging across the entire mount, not just individual files
- Built with progressive complexity — useful at week 1, impressive at week 4

---

## 2. Goals & Success Criteria

### Primary Goals

- Implement a mountable FUSE filesystem with transparent COW semantics on writes
- Maintain complete version history per file with timestamps and metadata
- Provide a companion CLI for history inspection, restore, snapshot, and GC
- Implement content deduplication using SHA-256 hashing of object content
- Achieve read performance within 2x of native filesystem (acceptable FUSE overhead)

### Success Criteria

| Metric | Target |
|---|---|
| Write correctness | Every write produces a new, distinct version |
| Read correctness | Reads always return current version data |
| Deduplication ratio | Identical content stored exactly once |
| Restore accuracy | Any version restores byte-for-byte correctly |
| Mount stability | No crashes during 1-hour stress test |
| Test coverage | > 75% on core logic |
| Demo quality | Mountable, usable, compelling 5-min demo |

### Out of Scope (v1)

- Network filesystem support (NFS/SAMBA export)
- Encryption at rest
- Multi-user concurrent mount (single-user focus)
- Windows/macOS support (Linux only for v1)
- Directory versioning (file-level versioning only in v1)

### Known Limitations (v1)

- **Whole-file COW on large files**: COWFS uses whole-file copy-on-write — writing 1 byte to a 500MB file re-hashes and re-stores the entire 500MB. This is acceptable for typical document/config/code workloads but not suitable for large binary blob editing (e.g., video files). Block-level COW (like ZFS) is a future consideration.
- **Single-process write safety**: If two processes write to different offsets of the same file simultaneously, the read-modify-write cycle in the `write()` handler may cause one write to clobber the other. A per-inode write lock serializes concurrent writes to the same file within the FUSE handler.
- **No hardlink support**: Files are identified by inode, but multiple hardlinks to the same inode are not supported in v1. `link()` returns `ENOTSUP`.

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     User Application                            │
│          (any program reading/writing to ~/mnt/)                │
└──────────────────────────┬──────────────────────────────────────┘
                           │ POSIX syscalls (open, read, write...)
┌──────────────────────────▼──────────────────────────────────────┐
│                    Linux Kernel VFS Layer                        │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Forwarded via /dev/fuse
┌──────────────────────────▼──────────────────────────────────────┐
│                    COWFS FUSE Handler                            │
│                    (Python / pyfuse3)                            │
│                                                                  │
│   Operations: lookup, getattr, read, write, create,             │
│               readdir, unlink, rename, truncate, flush          │
└──────┬───────────────────┬──────────────────────────────────────┘
       │                   │
┌──────▼──────┐   ┌────────▼──────────────────────────────────────┐
│  Metadata   │   │              Object Store                      │
│  Layer      │   │                                                │
│  (SQLite)   │   │  storage/objects/                              │
│             │   │  ├── a3f9c2d4...  (sha256 → raw bytes)        │
│  - files    │   │  ├── b7d1e4f8...                               │
│  - versions │   │  └── c9a2f1a3...                               │
│  - snapshots│   │                                                │
│  - objects  │   │  Content-addressable: same content = same hash │
└─────────────┘   └────────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                    COWFS CLI (cowfs)                             │
│                                                                  │
│   mount | umount | history | restore | snapshot | gc | diff     │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

**FUSE Handler** — Registers operation callbacks with the FUSE kernel module. Intercepts all filesystem syscalls on the mount point and routes them through COW logic. Never modifies existing objects — always creates new ones on write.

**Object Store** — A flat directory of immutable binary files named by their SHA-256 hash. This is content-addressable storage: two files with identical content share one object on disk. Inspired directly by git's blob storage model.

**Metadata Layer (SQLite)** — Tracks the mapping from logical file paths to their current object hash, maintains full version history with timestamps, stores snapshot state, and records object reference counts for garbage collection.

**COWFS CLI** — A companion command-line tool (built with Typer) that lets users inspect and manage version history, restore files, tag snapshots, and run garbage collection. Communicates with the metadata layer directly via SQLite.

---

## 4. Core Concepts

### 4.1 Content-Addressable Storage

Every file's content is stored as an immutable object named by its SHA-256 hash:

```
object_id = sha256(file_content)
storage/objects/{object_id} = raw bytes
```

Two different files with identical content map to the same object. This is deduplication by design — zero extra code needed once the addressing scheme is in place.

### 4.2 Version Chain

Each logical file maintains an ordered list of versions:

```
notes.txt
  └── version 1 → object: a3f9c2...  (created: 10:01:02)
  └── version 2 → object: b7d1e4...  (created: 10:01:45)
  └── version 3 → object: a3f9c2...  (created: 10:02:11)  ← same object as v1 (dedup)
```

The "current" version is always the latest. Reading the file returns the latest version's object content.

### 4.3 Write Path (Never Overwrite)

```
Traditional filesystem write:
  file.txt [block A] → overwrite → file.txt [block B]   # A is gone

COWFS write:
  file.txt → v1 → object_A
  write() called (buffered in memory)
  ...
  flush()/release() called
  new_object_B = store(buffered_content)
  file.txt → v1 → object_A
             v2 → object_B              # A still exists
```

**Flush-based versioning**: Individual `write()` syscalls are buffered in memory. A new version is created only when `flush()` or `release()` (file close) is called. This prevents applications that issue many small writes per logical save (e.g., text editors) from generating hundreds of intermediate versions. Each version corresponds to a complete, user-intended state of the file.

### 4.4 Snapshots

A snapshot is an atomic point-in-time record of the entire filesystem state — specifically, a mapping of every logical path to its current version at snapshot time.

**Restore semantics:**
- Files that existed at snapshot time are restored to their snapshot-time versions (a new version is created pointing to the snapshot's object, preserving post-snapshot history).
- Files created *after* the snapshot are **soft-deleted** by default (marked `is_deleted=True`). Use `cowfs snapshot restore <name> --keep-new` to leave post-snapshot files untouched.
- Files that were deleted after the snapshot are **restored** (marked `is_deleted=False`, current version set to the snapshot's version).

### 4.5 Garbage Collection

Objects with no version references are orphaned. GC walks all versions, collects referenced object hashes, and deletes any object file not in that set. Reference counting in SQLite makes this efficient.

---

## 5. Feature Specification

### 5.1 Core Filesystem (FUSE)

| Feature | Description |
|---|---|
| Transparent mounting | `cowfs mount <storage_dir> <mount_point>` mounts as a normal directory |
| COW writes | Every write creates a new version; original is never modified |
| Normal reads | `read()` returns current version content transparently |
| File creation | New files initialize a version history entry |
| File deletion | Marks current version as deleted; history retained |
| File rename | Updates path in metadata; version history follows the file |
| Directory support | Full directory create, list, delete support (dirs not versioned in v1) |
| Truncate | Treated as a write — produces new version |
| Atomic flush | Version record written to SQLite only after object fully written to disk |

### 5.2 Content Deduplication

| Feature | Description |
|---|---|
| SHA-256 addressing | Every object named by hash of content |
| Write-time dedup | Before storing, check if hash already exists; reuse if so |
| Reference counting | SQLite tracks how many versions reference each object |
| Space reporting | `cowfs stats` shows actual vs logical disk usage and dedup ratio |

### 5.3 Version History

| Feature | Description |
|---|---|
| Per-file history | Every file maintains ordered list of versions with timestamps |
| Version metadata | Size, timestamp, object hash stored per version |
| History display | `cowfs history <file>` lists all versions in table format |
| Restore by version | `cowfs restore <file> --version <n>` sets current version |
| Restore by time | `cowfs restore <file> --before "2026-02-23 10:00"` |
| Deleted file restore | Restore a file that has since been deleted |

### 5.4 Snapshots

| Feature | Description |
|---|---|
| Create snapshot | `cowfs snapshot create <name>` records entire filesystem state |
| List snapshots | `cowfs snapshot list` shows all named snapshots with timestamps |
| Restore snapshot | `cowfs snapshot restore <name>` resets all files to snapshot state |
| Delete snapshot | `cowfs snapshot delete <name>` removes snapshot (objects retained until GC) |
| Auto-snapshot | Optional: automatic snapshot on mount/umount |

### 5.5 Garbage Collection

| Feature | Description |
|---|---|
| Manual GC | `cowfs gc` deletes all unreferenced objects |
| Policy GC | `cowfs gc --keep-last <n>` retains only last N versions per file |
| Dry run | `cowfs gc --dry-run` shows what would be deleted without deleting |
| Space reclaim report | Shows bytes reclaimed after GC |

### 5.6 Diff

| Feature | Description |
|---|---|
| Version diff | `cowfs diff <file> --v1 <n> --v2 <m>` shows line-level diff |
| Current vs version | `cowfs diff <file> --version <n>` diffs current against version N |
| Binary detection | Skips diff for binary files, reports size delta instead |

---

## 6. Workflow & Data Flow

### 6.1 Mount Workflow

```
cowfs mount ~/storage ~/mnt
      │
      ▼
[Validate storage directory exists or create it]
      │
      ▼
[Initialize SQLite DB if first mount]
      │  Creates: storage/metadata.db
      │           storage/objects/   (directory)
      │
      ▼
[Register FUSE operations with pyfuse3]
      │
      ▼
[Enter FUSE event loop]
      │  Kernel now forwards all VFS calls on ~/mnt to COWFS
      ▼
[Handle operations until umount]
```

### 6.2 Write Operation Flow (Flush-Based Versioning)

```
Application calls: write("notes.txt", new_data)          ← may happen many times
      │
      ▼
[FUSE write() handler triggered]
      │
      ▼
[Acquire per-inode write lock]
      │
      ▼
[Buffer write in memory: apply new_data at offset to in-memory buffer]
      │  (no disk I/O, no version created yet)
      ▼
[Release lock, return len(buf) to kernel immediately]

... Application may call write() again (buffered) ...

Application calls: close() or fsync()
      │
      ▼
[FUSE flush()/release() handler triggered]
      │
      ▼
[Compute SHA-256 of complete buffered content]
      │  object_hash = sha256(full_buffer)
      │
      ▼
[Check if object already exists in storage/objects/]
      │
      ├── EXISTS → skip write (deduplication)
      │
      └── NOT EXISTS → write full_buffer to storage/objects/{object_hash}
                       fsync() to ensure durability
      │
      ▼
[Open SQLite transaction]
      │
      ├── INSERT INTO versions (file_id, object_hash, timestamp, size)
      │
      ├── UPDATE files SET current_version_id = new_version_id
      │
      ├── UPDATE objects SET ref_count = ref_count + 1
      │
      └── COMMIT transaction
      │
      ▼
[Clear in-memory buffer, return success to kernel]
```

### 6.3 Read Operation Flow

```
Application calls: read("notes.txt")
      │
      ▼
[FUSE read() handler triggered]
      │
      ▼
[Lookup file in SQLite: SELECT current_version_id FROM files WHERE path = ?]
      │
      ▼
[Get object hash: SELECT object_hash FROM versions WHERE id = current_version_id]
      │
      ▼
[Read bytes from storage/objects/{object_hash}]
      │
      ▼
[Return bytes to kernel → application receives data]
```

### 6.4 Restore Operation Flow

```
cowfs restore notes.txt --version 2
      │
      ▼
[CLI reads metadata.db directly (filesystem not required to be mounted)]
      │
      ▼
[Validate version 2 exists for notes.txt]
      │
      ▼
[Get object_hash for version 2]
      │
      ▼
[Open SQLite transaction]
      │
      ├── INSERT INTO versions (file_id, object_hash, ...) ← restore creates NEW version
      │                                                       pointing to old object
      ├── UPDATE files SET current_version_id = restored_version_id
      │
      └── COMMIT
      │
      ▼
[Report: "notes.txt restored to version 2 (now version 4)"]
```

### 6.5 Garbage Collection Flow

```
cowfs gc --keep-last 3
      │
      ▼
[For each file: keep only last 3 versions, mark older versions for deletion]
      │
      ▼
[Collect all object_hashes still referenced by remaining versions]
      │
      ▼
[Collect all object_hashes still referenced by any snapshot]
      │
      ▼
[Set of referenced = union of above two sets]
      │
      ▼
[Walk storage/objects/ directory]
      │
      └── For each object file:
            if hash NOT IN referenced set:
                if object created_at < (now - safety_window):    ← GC safety window
                    DELETE file
                    DELETE FROM objects WHERE hash = ?
                    bytes_reclaimed += file_size
      │
      ▼
[Report: "Deleted N objects, reclaimed X MB"]
```

**GC Safety**: GC skips objects created within the last 60 seconds (configurable via `--safety-window`). This prevents a race condition where GC deletes a freshly-written object before its version record is committed to SQLite. GC can run while the filesystem is mounted.

---

## 7. Storage Backend Design

### 7.1 Directory Structure

```
~/storage/                          ← user-specified storage root
├── .cowfs                          ← format marker (JSON: {"version": 1, "created": "..."})
├── metadata.db                     ← SQLite database (all metadata)
├── metadata.db-wal                 ← SQLite WAL file (auto-managed)
└── objects/                        ← content-addressable object store
    ├── a3/                         ← 2-char prefix sharding (like git)
    │   └── f9c2d4e1b8a7...         ← remaining 62 chars of SHA-256
    ├── b7/
    │   └── d1e4f8c2a9b3...
    └── c9/
        └── a2f1a3d7e4b1...
```

**Format marker file (`.cowfs`)**: A small JSON file at the storage root that identifies the directory as a valid COWFS storage backend and records the format version. `cowfs mount` checks for this file and refuses to mount if it is missing (preventing accidental mounts of arbitrary directories) or if the format version is unsupported.

**Why prefix sharding?** Filesystems slow down dramatically with thousands of files in a single directory. Using the first 2 hex chars as a subdirectory (256 possible subdirs) keeps each directory bounded. This is exactly how git's `.git/objects/` is organized.

### 7.2 Object Lifecycle

```
BORN:    write() computes hash → object file created → ref_count = 1
SHARED:  another version points to same hash → ref_count incremented
FREED:   version deleted → ref_count decremented → if ref_count == 0 → eligible for GC
DELETED: GC runs → object file removed from disk → row deleted from objects table
```

### 7.3 Durability Guarantees

Every write follows this sequence to prevent corruption:

```
1. Write object file to storage/objects/ (new file, atomic on Linux for small writes)
2. fsync(object_file)           ← ensure bytes hit disk
3. BEGIN TRANSACTION in SQLite
4. Insert version record
5. Update file current pointer
6. COMMIT TRANSACTION
7. Return success to FUSE
```

If the process crashes between steps 1 and 6, the object file exists but has no version record — it becomes an unreferenced orphan, safely cleaned up by GC. The previously current version remains intact.

---

## 8. Database Schema

```sql
-- Format version tracking (checked on mount)
CREATE TABLE format_version (
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO format_version (version) VALUES (1);

-- Logical files (paths in the mounted filesystem)
-- `id` doubles as the FUSE inode number (pyfuse3 operates on inodes, not paths).
-- The inode-to-path mapping is maintained here; lookup() resolves path → inode,
-- and all subsequent FUSE ops use the inode (id) directly.
CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER NOT NULL DEFAULT 1, -- FK to files.id (parent directory inode)
    name TEXT NOT NULL,                   -- filename component (e.g. "todo.txt")
    path TEXT UNIQUE NOT NULL,            -- full path (e.g. "/notes/todo.txt"), denormalized for CLI
    is_dir BOOLEAN DEFAULT FALSE,         -- TRUE for directories, FALSE for regular files
    current_version_id INTEGER,           -- FK to versions.id (NULL for directories)
    is_deleted BOOLEAN DEFAULT FALSE,
    mode INTEGER DEFAULT 33188,           -- POSIX permission bits (default: 0o100644)
    uid INTEGER DEFAULT 0,                -- owner user ID (set from FUSE context)
    gid INTEGER DEFAULT 0,                -- owner group ID (set from FUSE context)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Root directory inode (inode 1 = pyfuse3.ROOT_INODE)
INSERT INTO files (id, parent_id, name, path, is_dir, mode)
    VALUES (1, 1, '', '/', TRUE, 16877);  -- mode 0o40755

-- Version history per file
CREATE TABLE versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    object_hash TEXT NOT NULL,           -- SHA-256 hex string
    size_bytes INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,    -- marks soft-deleted versions
    FOREIGN KEY (object_hash) REFERENCES objects(hash)
);

-- Content-addressable objects
CREATE TABLE objects (
    hash TEXT PRIMARY KEY,               -- SHA-256 hex (64 chars)
    size_bytes INTEGER NOT NULL,
    ref_count INTEGER DEFAULT 0,         -- how many versions reference this
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Named filesystem snapshots
CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Maps each snapshot to the version of each file at snapshot time
CREATE TABLE snapshot_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
    file_id INTEGER NOT NULL REFERENCES files(id),
    version_id INTEGER NOT NULL REFERENCES versions(id)
);

-- Indexes for performance
CREATE INDEX idx_versions_file_id ON versions(file_id);
CREATE INDEX idx_versions_object_hash ON versions(object_hash);
CREATE INDEX idx_snapshot_entries_snapshot_id ON snapshot_entries(snapshot_id);
CREATE INDEX idx_files_path ON files(path);
CREATE INDEX idx_files_parent ON files(parent_id, name);  -- fast lookup() resolution
```

---

## 9. FUSE Operations

### Complete Operation Handler Table

| FUSE Operation | Triggered By | COW Behavior | Implementation Notes |
|---|---|---|---|
| `lookup` | Any path access | Read-only | Resolve `(parent_inode, name)` → child inode via `files` table |
| `getattr` | `stat()`, `ls -l` | Read-only | Return size/mtime from current version; mode/uid/gid from `files` |
| `setattr` | `chmod`, `chown`, `utimes` | Metadata update | Update `mode`, `uid`, `gid`, `updated_at` in `files` table |
| `readdir` | `ls` | Read-only | List children by `parent_id` from SQLite |
| `open` | `open()` | Read-only | Validate inode exists, allocate file handle, initialize write buffer |
| `read` | `read()` | Read-only | Load object bytes from object store (or from write buffer if dirty) |
| `write` | `write()` | **Buffer** | Apply data at offset to in-memory write buffer (no version created yet) |
| `create` | `open(O_CREAT)` | Initialize | Create `files` record with `parent_id`, version with empty object |
| `unlink` | `rm` | Soft delete | Set `is_deleted=True` on file, decrement ref_count |
| `mkdir` | `mkdir` | Pass-through | Create directory entry in `files` (`is_dir=TRUE`) |
| `rmdir` | `rmdir` | Pass-through | Remove if empty (no children with `is_deleted=FALSE`) |
| `rename` | `mv` | Update path | Update `parent_id`, `name`, and `path` in `files` table |
| `truncate` | `truncate()` | **Buffer** | Truncate in-memory buffer (version created on flush) |
| `flush` | File close / `fsync()` | **COW** | Hash buffer → store object → create version → commit to SQLite |
| `release` | Last handle closed | **COW** | Final flush if dirty; deallocate write buffer and file handle |
| `statfs` | `df` | Stats | Return storage usage stats from SQLite |

### Implementation Skeleton (Python/pyfuse3)

```python
import pyfuse3
import hashlib
import asyncio
import aiosqlite
import os
from pathlib import Path
from collections import defaultdict

class COWFS(pyfuse3.Operations):

    def __init__(self, storage_root: str):
        self.storage_root = Path(storage_root)
        self.objects_dir = self.storage_root / "objects"
        self.db: aiosqlite.Connection = None  # initialized in _async_init()
        self._write_buffers: dict[int, bytearray] = {}   # inode → dirty buffer
        self._inode_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._init_storage()

    async def _async_init(self):
        """Call after __init__ — opens async SQLite connection."""
        self.db = await aiosqlite.connect(str(self.storage_root / "metadata.db"))
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self._init_db()

    def _compute_hash(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _object_path(self, obj_hash: str) -> Path:
        return self.objects_dir / obj_hash[:2] / obj_hash[2:]

    def _store_object(self, data: bytes) -> str:
        obj_hash = self._compute_hash(data)
        obj_path = self._object_path(obj_hash)
        if not obj_path.exists():
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            obj_path.write_bytes(data)
            # fsync for durability
            with open(obj_path, 'rb') as f:
                os.fsync(f.fileno())
        return obj_hash

    async def write(self, fh, offset, buf):
        inode = self._fh_to_inode(fh)
        async with self._inode_locks[inode]:
            # Buffer writes in memory — no version created yet
            if inode not in self._write_buffers:
                self._write_buffers[inode] = bytearray(await self._read_current(inode))
            data = self._write_buffers[inode]
            # Extend buffer if write goes past current end
            if offset + len(buf) > len(data):
                data.extend(b'\x00' * (offset + len(buf) - len(data)))
            data[offset:offset + len(buf)] = buf
        return len(buf)

    async def flush(self, fh):
        inode = self._fh_to_inode(fh)
        async with self._inode_locks[inode]:
            if inode not in self._write_buffers:
                return  # nothing to flush
            new_data = bytes(self._write_buffers.pop(inode))
        # COW: store new object, create new version
        object_hash = self._store_object(new_data)
        await self._create_version(inode, object_hash, len(new_data))

    async def read(self, fh, offset, length):
        inode = self._fh_to_inode(fh)
        # Return from write buffer if dirty, otherwise from object store
        if inode in self._write_buffers:
            return bytes(self._write_buffers[inode][offset:offset + length])
        object_hash = await self._get_current_object_hash(inode)
        obj_path = self._object_path(object_hash)
        data = obj_path.read_bytes()
        return data[offset:offset + length]

    async def setattr(self, inode, attr, fields, fh, ctx):
        # Update permission bits, ownership, and timestamps
        if fields.update_mode:
            await self.db.execute(
                "UPDATE files SET mode = ? WHERE id = ?", (attr.st_mode, inode))
        if fields.update_uid:
            await self.db.execute(
                "UPDATE files SET uid = ? WHERE id = ?", (attr.st_uid, inode))
        if fields.update_gid:
            await self.db.execute(
                "UPDATE files SET gid = ? WHERE id = ?", (attr.st_gid, inode))
        await self.db.commit()
        return await self.getattr(inode, ctx)
```

---

## 10. CLI Interface Design

### Command Structure

```
cowfs <command> [options] [--json]

Commands:
  mount     Mount the filesystem
  umount    Unmount the filesystem
  history   Show version history of a file
  restore   Restore a file to a previous version
  snapshot  Manage filesystem snapshots
  gc        Run garbage collection
  diff      Show diff between file versions
  log       Show chronological activity log across all files
  stats     Show storage statistics

Global Flags:
  --json    Output in JSON format (all commands, for scripting/integration)
```

### Detailed Command Reference

```bash
# Mount
cowfs mount <storage_dir> <mount_point> [--debug] [--auto-snapshot] [--hash-algo sha256|blake3]

# Unmount
cowfs umount <mount_point>

# History
cowfs history <file_path>
# Output:
# Ver  Date                 Size     Hash
# 1    2026-02-23 10:01:02  1.2 KB   a3f9c2...
# 2    2026-02-23 10:01:45  1.4 KB   b7d1e4...
# 3*   2026-02-23 10:02:11  1.2 KB   a3f9c2...  ← current (dedup: same as v1)

# Restore
cowfs restore <file_path> --version <n>
cowfs restore <file_path> --before "2026-02-23 10:02:00"
cowfs restore <file_path> --version <n> --dry-run

# Snapshot
cowfs snapshot create <name> [--description "text"]
cowfs snapshot list
cowfs snapshot restore <name> [--dry-run] [--keep-new]   # --keep-new: don't soft-delete post-snapshot files
cowfs snapshot delete <name>
cowfs snapshot show <name>        # list all files and their versions at snapshot time

# Garbage Collection
cowfs gc
cowfs gc --keep-last <n>          # keep last N versions per file
cowfs gc --before "2026-01-01"    # delete versions older than date
cowfs gc --dry-run                # show what would be deleted
cowfs gc --safety-window <secs>   # skip objects younger than N seconds (default: 60)

# Diff
cowfs diff <file_path> --v1 <n> --v2 <m>
cowfs diff <file_path> --version <n>    # diff current vs version N

# Stats
cowfs stats [--json]
# Output:
# Format version:       1
# Logical size:    142.3 MB
# Actual size:      89.1 MB
# Dedup savings:    53.2 MB (37.4%)
# Total files:         234
# Total versions:    1,847
# Total objects:       912
# Orphaned objects:      0
# Hash algorithm:   sha256

# Log (chronological activity feed across all files)
cowfs log [--limit <n>] [--since "2026-02-01"] [--json]
# Output:
# Time                  Action   Path                    Version  Hash
# 2026-02-23 10:01:02   WRITE    /notes/todo.txt         v1       a3f9c2...
# 2026-02-23 10:01:45   WRITE    /notes/todo.txt         v2       b7d1e4...
# 2026-02-23 10:02:11   WRITE    /notes/todo.txt         v3       a3f9c2...
# 2026-02-23 10:03:00   DELETE   /temp/scratch.txt       -        -
# 2026-02-23 10:04:30   RESTORE  /notes/todo.txt         v4       a3f9c2...
# 2026-02-23 10:05:00   SNAPSHOT baseline                -        -
```

### CLI Implementation (Typer)

```python
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(name="cowfs", help="Copy-on-Write Filesystem Manager")
console = Console()

@app.command()
def mount(
    storage_dir: str = typer.Argument(..., help="Storage backend directory"),
    mount_point: str = typer.Argument(..., help="Directory to mount at"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
):
    """Mount the COWFS filesystem."""
    ...

@app.command()
def history(
    file_path: str = typer.Argument(..., help="Path to file (relative to mount)"),
):
    """Show version history of a file."""
    ...

@app.command()
def restore(
    file_path: str = typer.Argument(...),
    version: int = typer.Option(None, "--version", "-v"),
    before: str = typer.Option(None, "--before"),
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """Restore a file to a previous version."""
    ...

if __name__ == "__main__":
    app()
```

---

## 11. Tech Stack & Tools

### Core

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Language | Python | 3.11+ | Primary implementation language |
| FUSE binding | pyfuse3 | Latest | Userspace filesystem interface |
| Database | SQLite | 3.x (stdlib) | Metadata, version history, snapshots |
| Async DB | aiosqlite | Latest | Non-blocking SQLite for async FUSE handlers |
| Hashing | hashlib (SHA-256) | stdlib | Content addressing (default) |
| Fast hashing | blake3 (optional) | Latest | ~5x faster hashing for large files; opt-in via `--hash-algo blake3` |
| Async | asyncio | stdlib | pyfuse3 requires async handlers |

### CLI & UX

| Component | Technology | Purpose |
|---|---|---|
| CLI framework | Typer | Command parsing, help generation |
| Terminal UI | Rich | Tables, progress bars, colored output |
| Diff display | difflib (stdlib) | Line-level diff between versions |

### Development & Testing

| Component | Technology | Purpose |
|---|---|---|
| Testing | Pytest | Unit and integration tests |
| Filesystem testing | pyfakefs | Mock filesystem for unit tests |
| Coverage | pytest-cov | Coverage reporting |
| Linting | Ruff | Fast Python linter |
| Formatting | Black | Code formatting |
| Type checking | mypy | Static type checking |
| Pre-commit | pre-commit | Enforce lint/format on commit |

### Build & Distribution

| Component | Technology | Purpose |
|---|---|---|
| Packaging | pyproject.toml (hatchling) | Modern Python packaging |
| Distribution | PyPI | `pip install cowfs` |
| CI/CD | GitHub Actions | Test, lint, publish pipeline |
| Documentation | MkDocs + Material | Project docs site |

### System Requirements

| Requirement | Detail |
|---|---|
| OS | Linux (kernel 2.6.14+ for FUSE support) |
| FUSE | `libfuse3-dev` package required |
| Python | 3.11 or higher |
| Disk | Any POSIX-compliant filesystem for storage backend |
| Permissions | User must be in `fuse` group (or run with sudo for testing) |

---

## 12. Task Breakdown & Milestones

### Phase 1 — Foundation (Week 1)

**Goal:** A working FUSE mount with basic passthrough (no COW yet). Files readable and writable through the mount point.

- [ ] Set up project structure (`cowfs/`, `tests/`, `docs/`)
- [ ] Configure `pyproject.toml` with dependencies
- [ ] Set up pre-commit hooks (ruff, black, mypy)
- [ ] Install and verify `pyfuse3` and FUSE kernel module
- [ ] Implement `COWFS` class skeleton with all required pyfuse3 methods
- [ ] Implement `lookup`, `getattr`, `readdir` — basic directory listing works
- [ ] Implement `open`, `read`, `create` — files readable through mount
- [ ] Implement `write` as passthrough (overwrite, no COW yet)
- [ ] Implement `unlink`, `mkdir`, `rmdir`, `rename`
- [ ] Initialize SQLite schema and migration
- [ ] Implement `cowfs mount` and `cowfs umount` CLI commands
- [ ] Write smoke tests: mount, create file, read file, unmount
- [ ] **Checkpoint:** `echo "hello" > ~/mnt/test.txt && cat ~/mnt/test.txt` works

### Phase 2 — COW Core + Deduplication (Week 2)

**Goal:** Every write creates a new version. Version history queryable via CLI.

- [ ] Implement content-addressable object store (`_store_object`, `_object_path`)
- [ ] Implement SHA-256 hashing and prefix-sharded directory structure
- [ ] Rewrite `write()` handler with full COW logic
- [ ] Implement `truncate()` as COW write
- [ ] Implement deduplication (hash existence check before write)
- [ ] Implement reference counting (increment on version create, decrement on delete)
- [ ] Implement `unlink()` as soft delete with ref_count decrement
- [ ] Implement `cowfs history <file>` CLI command
- [ ] Implement `cowfs stats` CLI command with dedup ratio
- [ ] Handle partial writes correctly (read-modify-write cycle)
- [ ] Write unit tests for object store (store, dedup, retrieval)
- [ ] Write integration tests for write/version/history cycle
- [ ] **Checkpoint:** Write same content twice, verify only one object stored

### Phase 3 — Restore, Snapshots & GC (Week 3)

**Goal:** Full version management — restore any file to any version, snapshot entire filesystem, garbage collect orphaned objects.

- [ ] Implement `cowfs restore <file> --version <n>`
- [ ] Implement `cowfs restore <file> --before <datetime>`
- [ ] Implement restore of deleted files
- [ ] Implement `cowfs snapshot create <name>`
- [ ] Implement `cowfs snapshot list`
- [ ] Implement `cowfs snapshot restore <name>`
- [ ] Implement `cowfs snapshot delete <name>`
- [ ] Implement `cowfs gc` (collect unreferenced objects)
- [ ] Implement `cowfs gc --keep-last <n>`
- [ ] Implement `cowfs gc --dry-run`
- [ ] Implement `cowfs diff <file> --v1 <n> --v2 <m>` using difflib
- [ ] Write tests for restore correctness (byte-for-byte verification)
- [ ] Write tests for GC (verify orphaned objects deleted, referenced objects retained)
- [ ] Write tests for snapshot create/restore cycle
- [ ] **Checkpoint:** Delete a file, restore it from history; take snapshot, modify files, restore snapshot

### Phase 4 — Polish, Performance & Release (Week 4)

**Goal:** Production-quality code, good docs, PyPI publication, compelling demo.

- [ ] Add structured logging (Python `logging` module, configurable level)
- [ ] Add `--debug` flag to mount command for verbose FUSE operation logging
- [ ] Implement durability guarantees (fsync before SQLite commit)
- [ ] Handle edge cases: empty files, binary files, large files (>100MB), unicode paths
- [ ] Write stress test: 1000 writes, verify version count and dedup ratio
- [ ] Achieve >75% test coverage (`pytest --cov`)
- [ ] Write comprehensive README with architecture diagram, install guide, usage examples
- [ ] Create demo GIF (terminalizer or asciinema)
- [ ] Set up GitHub Actions CI (lint → typecheck → test → coverage report)
- [ ] Configure PyPI publication workflow
- [ ] Publish to PyPI (`pip install cowfs`)
- [ ] Write MkDocs documentation site
- [ ] Record YouTube demo walkthrough
- [ ] **Checkpoint:** `pip install cowfs` works; full demo runnable in 5 minutes

---

## 13. Testing Plan

### Test Structure

```
tests/
├── unit/
│   ├── test_object_store.py      # store, retrieve, dedup, hash correctness
│   ├── test_metadata.py          # SQLite CRUD, version chain, ref counting
│   ├── test_gc.py                # GC logic, keep-last policy, dry-run
│   └── test_snapshot.py          # snapshot create/restore/delete
├── integration/
│   ├── test_mount.py             # full mount/umount cycle
│   ├── test_write_read.py        # write then read returns correct data
│   ├── test_versioning.py        # multiple writes produce correct history
│   ├── test_restore.py           # restore byte-for-byte correctness
│   └── test_dedup.py             # same content → single object
└── stress/
    └── test_stress.py            # 1000 writes, concurrent access, large files
```

### Key Test Cases

```python
def test_write_creates_new_version(mounted_fs):
    path = mounted_fs / "test.txt"
    path.write_text("version 1")
    path.write_text("version 2")
    versions = cowfs_cli.history(path)
    assert len(versions) == 2

def test_deduplication(mounted_fs):
    (mounted_fs / "a.txt").write_bytes(b"same content")
    (mounted_fs / "b.txt").write_bytes(b"same content")
    stats = cowfs_cli.stats()
    assert stats.total_objects == 1  # one object, two files

def test_restore_byte_perfect(mounted_fs):
    path = mounted_fs / "data.bin"
    original = os.urandom(4096)  # random bytes
    path.write_bytes(original)
    path.write_bytes(os.urandom(4096))  # overwrite
    cowfs_cli.restore(path, version=1)
    assert path.read_bytes() == original

def test_gc_removes_orphaned_objects(mounted_fs, db):
    path = mounted_fs / "temp.txt"
    path.write_text("old content")
    path.write_text("new content")
    cowfs_cli.gc(keep_last=1)
    orphaned = db.execute("SELECT count(*) FROM objects WHERE ref_count = 0").fetchone()[0]
    assert orphaned == 0

def test_snapshot_restore(mounted_fs):
    (mounted_fs / "config.txt").write_text("original")
    cowfs_cli.snapshot_create("baseline")
    (mounted_fs / "config.txt").write_text("modified")
    cowfs_cli.snapshot_restore("baseline")
    assert (mounted_fs / "config.txt").read_text() == "original"
```

---

## 14. Non-Functional Requirements

### Correctness

- Writes must be atomic from the application's perspective — no partial versions visible.
- A crash between object write and SQLite commit must leave the filesystem in a consistent state (old version still current, new object becomes orphan for GC).
- Restore must be byte-perfect — restored file content must be identical to the original version's bytes.

### Performance

- Read overhead vs native filesystem: < 3x (acceptable for FUSE userspace penalty).
- Write overhead vs native filesystem: < 5x (FUSE + SHA-256 + SQLite write).
- `cowfs history` on a file with 10,000 versions must return in < 100ms.
- GC on 100,000 objects must complete in < 30 seconds.

### Reliability

- SQLite WAL mode enabled for better concurrent read performance and crash safety.
- All object writes followed by `fsync()` before SQLite transaction commit.
- No data loss on clean unmount.

### Usability

- `pip install cowfs` must work on any Linux system with FUSE available.
- `cowfs mount` must produce a clear error if FUSE module not loaded.
- All CLI commands must have `--help` with examples.
- Error messages must be human-readable, not stack traces.

---

## 15. Benchmarking Plan

Run benchmarks comparing COWFS against native filesystem (ext4) on the same machine:

```bash
# Sequential write benchmark
fio --name=seq_write --rw=write --bs=4k --size=100M --numjobs=1

# Random write benchmark  
fio --name=rand_write --rw=randwrite --bs=4k --size=100M --numjobs=1

# Read benchmark (after writes)
fio --name=seq_read --rw=read --bs=4k --size=100M --numjobs=1

# Deduplication ratio test
# Write 1000 files with 80% duplicate content, measure objects created vs files
```

**Report metrics:**
- Read IOPS: COWFS vs ext4
- Write IOPS: COWFS vs ext4
- Write latency p50/p95/p99
- Dedup ratio under various workloads (all unique, 50% dup, 90% dup)
- GC duration vs object count

Include benchmark results in README as a table.

---

## 16. Future Roadmap

### v1.1 — Directory Versioning
Extend version history to cover directory structure changes — track file additions, deletions, and renames within a snapshot.

### v1.2 — Encryption at Rest
Encrypt object files using a user-provided passphrase (AES-256-GCM). Key derived via Argon2. Metadata DB encrypted using SQLCipher.

### v1.3 — Remote Backend
Swap local `objects/` directory for an S3-compatible backend (Cloudflare R2, AWS S3). Metadata stays local; objects pushed to remote on write, pulled on read. Enables cloud-backed versioned storage.

### v2.0 — C Extension for Hot Path
Rewrite the `write()` and `read()` FUSE handlers in C using libfuse directly for 10x performance improvement. Keep Python CLI and metadata layer.

### v2.1 — FUSE on macOS
Port to macOS using macFUSE. Handle macOS-specific VFS quirks (resource forks, extended attributes).

### v3.0 — Multi-user Support
Implement locking and concurrent write serialization. Support multiple users mounting the same storage backend simultaneously with consistent version history.

---

*Document maintained by Atikul Islam Munna. Update version number and date on each revision.*
