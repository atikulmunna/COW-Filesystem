"""SQLite metadata layer for COWFS.

Manages all metadata: files (inodes), versions, objects, snapshots.
Uses synchronous sqlite3 — pyfuse3 runs on Trio, and SQLite on local disk
is fast enough that thread offloading is only needed for bulk operations.
"""

import sqlite3
from pathlib import Path
from typing import cast

SCHEMA_SQL = """
-- Format version tracking (checked on mount)
CREATE TABLE IF NOT EXISTS format_version (
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Logical files (paths in the mounted filesystem)
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    path TEXT UNIQUE NOT NULL,
    is_dir BOOLEAN DEFAULT FALSE,
    current_version_id INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    mode INTEGER DEFAULT 33188,
    uid INTEGER DEFAULT 0,
    gid INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Version history per file
CREATE TABLE IF NOT EXISTS versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    object_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (object_hash) REFERENCES objects(hash)
);

-- Content-addressable objects
CREATE TABLE IF NOT EXISTS objects (
    hash TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL,
    ref_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Named filesystem snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Maps each snapshot to the version of each file at snapshot time
CREATE TABLE IF NOT EXISTS snapshot_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
    file_id INTEGER NOT NULL REFERENCES files(id),
    version_id INTEGER NOT NULL REFERENCES versions(id)
);

-- Chronological activity feed
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    path TEXT,
    version_id INTEGER,
    object_hash TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_versions_file_id ON versions(file_id);
CREATE INDEX IF NOT EXISTS idx_versions_object_hash ON versions(object_hash);
CREATE INDEX IF NOT EXISTS idx_snapshot_entries_snapshot_id ON snapshot_entries(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_id, name);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at, id);
"""

ROOT_INODE_SQL = """
INSERT OR IGNORE INTO files (id, parent_id, name, path, is_dir, mode)
    VALUES (1, 1, '', '/', TRUE, 16877);
"""

FORMAT_VERSION_SQL = """
INSERT INTO format_version (version) SELECT 1
    WHERE NOT EXISTS (SELECT 1 FROM format_version);
"""


class MetadataDB:
    """Synchronous SQLite wrapper for COWFS metadata operations."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db: sqlite3.Connection | None = None
        self._manual_tx = False

    def connect(self) -> None:
        self.db = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=NORMAL")

    def initialize(self) -> None:
        assert self.db is not None, "Call connect() first"
        self.db.executescript(SCHEMA_SQL)
        self.db.execute(ROOT_INODE_SQL)
        self.db.execute(FORMAT_VERSION_SQL)
        self.db.commit()

    def close(self) -> None:
        if self.db:
            self.db.close()
            self.db = None
            self._manual_tx = False

    def begin(self) -> None:
        assert self.db is not None
        if self._manual_tx:
            return
        self.db.execute("BEGIN")
        self._manual_tx = True

    def commit(self) -> None:
        assert self.db is not None
        self.db.commit()
        self._manual_tx = False

    def rollback(self) -> None:
        assert self.db is not None
        self.db.rollback()
        self._manual_tx = False

    def _commit_if_needed(self) -> None:
        assert self.db is not None
        if not self._manual_tx:
            self.db.commit()

    # ──────────────────────────── File operations ────────────────────────────

    def lookup(self, parent_id: int, name: str) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT * FROM files WHERE parent_id = ? AND name = ? AND is_deleted = FALSE",
            (parent_id, name),
        )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def get_file(self, inode: int) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT * FROM files WHERE id = ? AND is_deleted = FALSE", (inode,)
        )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def get_file_by_path(self, path: str, include_deleted: bool = False) -> sqlite3.Row | None:
        assert self.db is not None
        if include_deleted:
            cursor = self.db.execute("SELECT * FROM files WHERE path = ?", (path,))
        else:
            cursor = self.db.execute(
                "SELECT * FROM files WHERE path = ? AND is_deleted = FALSE", (path,)
            )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def list_children(self, parent_id: int) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT * FROM files WHERE parent_id = ? AND is_deleted = FALSE AND id != ?",
            (parent_id, parent_id),
        )
        return cursor.fetchall()

    def create_file(
        self,
        parent_id: int,
        name: str,
        path: str,
        is_dir: bool = False,
        mode: int = 33188,
        uid: int = 0,
        gid: int = 0,
    ) -> int:
        assert self.db is not None
        cursor = self.db.execute(
            """INSERT INTO files (parent_id, name, path, is_dir, mode, uid, gid)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (parent_id, name, path, is_dir, mode, uid, gid),
        )
        self.db.commit()
        assert cursor.lastrowid is not None
        return cursor.lastrowid

    def soft_delete_file(
        self,
        inode: int,
        *,
        commit: bool = True,
        action: str = "DELETE",
    ) -> None:
        assert self.db is not None
        file_row = self.get_file(inode)
        path = file_row["path"] if file_row is not None else None
        self.db.execute(
            "UPDATE files SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (inode,),
        )
        self.record_event(action, path=path, commit=False)
        if commit:
            self._commit_if_needed()

    def set_file_deleted(self, inode: int, is_deleted: bool, *, commit: bool = True) -> None:
        assert self.db is not None
        self.db.execute(
            "UPDATE files SET is_deleted = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (is_deleted, inode),
        )
        if commit:
            self._commit_if_needed()

    def rename_file(
        self, inode: int, new_parent_id: int, new_name: str, new_path: str
    ) -> None:
        assert self.db is not None
        row = self.get_file(inode)
        if row is None:
            return
        old_path = row["path"]
        is_dir = row["is_dir"]

        self.db.execute(
            """UPDATE files SET parent_id = ?, name = ?, path = ?,
               updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
            (new_parent_id, new_name, new_path, inode),
        )
        if is_dir:
            self.db.execute(
                """UPDATE files SET path = ? || substr(path, ?),
                   updated_at = CURRENT_TIMESTAMP
                   WHERE path LIKE ? || '/%'""",
                (new_path, len(old_path) + 1, old_path),
            )
        self.db.commit()

    def update_attrs(
        self,
        inode: int,
        mode: int | None = None,
        uid: int | None = None,
        gid: int | None = None,
    ) -> None:
        assert self.db is not None
        if mode is not None:
            self.db.execute(
                "UPDATE files SET mode = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (mode, inode),
            )
        if uid is not None:
            self.db.execute(
                "UPDATE files SET uid = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (uid, inode),
            )
        if gid is not None:
            self.db.execute(
                "UPDATE files SET gid = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (gid, inode),
            )
        self.db.commit()

    def set_current_version(self, inode: int, version_id: int) -> None:
        assert self.db is not None
        self.db.execute(
            "UPDATE files SET current_version_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (version_id, inode),
        )
        self.db.commit()

    # ──────────────────────────── Version operations ─────────────────────────

    def create_version(
        self,
        file_id: int,
        object_hash: str,
        size_bytes: int,
        *,
        commit: bool = True,
        action: str = "WRITE",
    ) -> int:
        assert self.db is not None
        self.db.execute(
            """INSERT INTO objects (hash, size_bytes, ref_count)
               VALUES (?, ?, 1)
               ON CONFLICT(hash) DO UPDATE SET ref_count = ref_count + 1""",
            (object_hash, size_bytes),
        )
        cursor = self.db.execute(
            """INSERT INTO versions (file_id, object_hash, size_bytes)
               VALUES (?, ?, ?)""",
            (file_id, object_hash, size_bytes),
        )
        version_id = cursor.lastrowid
        assert version_id is not None
        self.db.execute(
            "UPDATE files SET current_version_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (version_id, file_id),
        )
        file_row = self.get_file(file_id)
        path = file_row["path"] if file_row is not None else None
        self.record_event(
            action,
            path=path,
            version_id=version_id,
            object_hash=object_hash,
            commit=False,
        )
        if commit:
            self._commit_if_needed()
        return version_id

    def get_current_version(self, inode: int) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT v.* FROM versions v
               JOIN files f ON f.current_version_id = v.id
               WHERE f.id = ? AND f.is_deleted = FALSE""",
            (inode,),
        )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def get_version(self, version_id: int) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT * FROM versions WHERE id = ?", (version_id,)
        )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def list_versions(self, file_id: int) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT * FROM versions WHERE file_id = ? AND is_deleted = FALSE
               ORDER BY created_at ASC""",
            (file_id,),
        )
        return cursor.fetchall()

    def get_latest_version_before(self, file_id: int, before: str) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT * FROM versions
               WHERE file_id = ? AND is_deleted = FALSE AND created_at <= ?
               ORDER BY created_at DESC, id DESC
               LIMIT 1""",
            (file_id, before),
        )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def list_prunable_versions(self, keep_last: int) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """WITH ranked AS (
                   SELECT v.*,
                          ROW_NUMBER() OVER (
                              PARTITION BY v.file_id
                              ORDER BY v.created_at DESC, v.id DESC
                          ) AS rn
                   FROM versions v
                   WHERE v.is_deleted = FALSE
               )
               SELECT id, file_id, object_hash, size_bytes, created_at
               FROM ranked
               WHERE rn > ?
               ORDER BY file_id ASC, created_at ASC, id ASC""",
            (keep_last,),
        )
        return cursor.fetchall()

    def prune_versions_keep_last(self, keep_last: int, *, commit: bool = True) -> list[sqlite3.Row]:
        assert self.db is not None
        rows = self.list_prunable_versions(keep_last)
        for row in rows:
            self.db.execute(
                "DELETE FROM versions WHERE id = ?",
                (row["id"],),
            )
            self.db.execute(
                "UPDATE objects SET ref_count = ref_count - 1 WHERE hash = ?",
                (row["object_hash"],),
            )
        if commit:
            self._commit_if_needed()
        return rows

    def list_prunable_versions_before(self, before: str) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT v.id, v.file_id, v.object_hash, v.size_bytes, v.created_at
               FROM versions v
               LEFT JOIN files f ON f.current_version_id = v.id
               WHERE v.is_deleted = FALSE
                 AND v.created_at < ?
                 AND f.id IS NULL
               ORDER BY v.file_id ASC, v.created_at ASC, v.id ASC""",
            (before,),
        )
        return cursor.fetchall()

    def prune_versions_before(self, before: str, *, commit: bool = True) -> list[sqlite3.Row]:
        assert self.db is not None
        rows = self.list_prunable_versions_before(before)
        for row in rows:
            self.db.execute(
                "DELETE FROM versions WHERE id = ?",
                (row["id"],),
            )
            self.db.execute(
                "UPDATE objects SET ref_count = ref_count - 1 WHERE hash = ?",
                (row["object_hash"],),
            )
        if commit:
            self._commit_if_needed()
        return rows

    # ──────────────────────────── Object operations ──────────────────────────

    def get_object(self, obj_hash: str) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT * FROM objects WHERE hash = ?", (obj_hash,)
        )
        return cast(sqlite3.Row | None, cursor.fetchone())

    def decrement_ref_count(self, obj_hash: str) -> int:
        assert self.db is not None
        self.db.execute(
            "UPDATE objects SET ref_count = ref_count - 1 WHERE hash = ?",
            (obj_hash,),
        )
        self.db.commit()
        cursor = self.db.execute(
            "SELECT ref_count FROM objects WHERE hash = ?", (obj_hash,)
        )
        row = cursor.fetchone()
        return row["ref_count"] if row else 0

    def get_orphaned_objects(self) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT * FROM objects WHERE ref_count <= 0"
        )
        return cursor.fetchall()

    def delete_object_record(self, obj_hash: str, *, commit: bool = True) -> None:
        assert self.db is not None
        self.db.execute("DELETE FROM objects WHERE hash = ?", (obj_hash,))
        if commit:
            self._commit_if_needed()

    # ──────────────────────────── Snapshot operations ───────────────────────

    def create_snapshot(self, name: str, description: str | None = None) -> int:
        assert self.db is not None
        cursor = self.db.execute(
            "INSERT INTO snapshots (name, description) VALUES (?, ?)",
            (name, description),
        )
        snapshot_id = cursor.lastrowid
        assert snapshot_id is not None
        self.db.execute(
            """INSERT INTO snapshot_entries (snapshot_id, file_id, version_id)
               SELECT ?, id, current_version_id
               FROM files
               WHERE is_deleted = FALSE AND is_dir = FALSE AND current_version_id IS NOT NULL""",
            (snapshot_id,),
        )
        self.db.commit()
        return snapshot_id

    def list_snapshots(self) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT s.*, COUNT(se.id) AS file_count
               FROM snapshots s
               LEFT JOIN snapshot_entries se ON se.snapshot_id = s.id
               GROUP BY s.id
               ORDER BY s.created_at ASC, s.id ASC"""
        )
        return cursor.fetchall()

    def get_snapshot_by_name(self, name: str) -> sqlite3.Row | None:
        assert self.db is not None
        cursor = self.db.execute("SELECT * FROM snapshots WHERE name = ?", (name,))
        return cast(sqlite3.Row | None, cursor.fetchone())

    def get_snapshot_entries(self, snapshot_id: int) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT se.file_id, se.version_id
               FROM snapshot_entries se
               WHERE se.snapshot_id = ?""",
            (snapshot_id,),
        )
        return cursor.fetchall()

    def get_snapshot_entries_detailed(self, snapshot_id: int) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT se.file_id, se.version_id, f.path, v.object_hash, v.size_bytes, v.created_at
               FROM snapshot_entries se
               JOIN files f ON f.id = se.file_id
               JOIN versions v ON v.id = se.version_id
               WHERE se.snapshot_id = ?
               ORDER BY f.path ASC""",
            (snapshot_id,),
        )
        return cursor.fetchall()

    def delete_snapshot(self, snapshot_id: int) -> None:
        assert self.db is not None
        self.db.execute("DELETE FROM snapshot_entries WHERE snapshot_id = ?", (snapshot_id,))
        self.db.execute("DELETE FROM snapshots WHERE id = ?", (snapshot_id,))
        self.db.commit()

    def list_active_file_ids(self) -> list[int]:
        assert self.db is not None
        cursor = self.db.execute(
            "SELECT id FROM files WHERE is_deleted = FALSE AND is_dir = FALSE"
        )
        return [row["id"] for row in cursor.fetchall()]

    # ──────────────────────────── Event operations ──────────────────────────

    def record_event(
        self,
        action: str,
        *,
        path: str | None = None,
        version_id: int | None = None,
        object_hash: str | None = None,
        commit: bool = True,
    ) -> int:
        assert self.db is not None
        cursor = self.db.execute(
            """INSERT INTO events (action, path, version_id, object_hash)
               VALUES (?, ?, ?, ?)""",
            (action, path, version_id, object_hash),
        )
        event_id = cursor.lastrowid
        assert event_id is not None
        if commit:
            self._commit_if_needed()
        return event_id

    def list_events(self, limit: int = 50) -> list[sqlite3.Row]:
        assert self.db is not None
        cursor = self.db.execute(
            """SELECT created_at, action, path, version_id, object_hash
               FROM events
               ORDER BY created_at DESC, id DESC
               LIMIT ?""",
            (limit,),
        )
        rows = cursor.fetchall()
        rows.reverse()
        return rows

    # ──────────────────────────── Stats ───────────────────────────────────────

    def get_stats(self) -> dict:
        assert self.db is not None
        stats = {}

        cursor = self.db.execute(
            "SELECT COUNT(*) as c FROM files WHERE is_deleted = FALSE AND is_dir = FALSE"
        )
        row = cursor.fetchone()
        stats["total_files"] = row["c"] if row else 0

        cursor = self.db.execute(
            "SELECT COUNT(*) as c FROM versions WHERE is_deleted = FALSE"
        )
        row = cursor.fetchone()
        stats["total_versions"] = row["c"] if row else 0

        cursor = self.db.execute("SELECT COUNT(*) as c FROM objects")
        row = cursor.fetchone()
        stats["total_objects"] = row["c"] if row else 0

        cursor = self.db.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) as s FROM objects"
        )
        row = cursor.fetchone()
        stats["actual_size_bytes"] = row["s"] if row else 0

        cursor = self.db.execute(
            """SELECT COALESCE(SUM(v.size_bytes), 0) as s
               FROM versions v WHERE v.is_deleted = FALSE"""
        )
        row = cursor.fetchone()
        stats["logical_size_bytes"] = row["s"] if row else 0

        cursor = self.db.execute(
            "SELECT COUNT(*) as c FROM objects WHERE ref_count <= 0"
        )
        row = cursor.fetchone()
        stats["orphaned_objects"] = row["c"] if row else 0

        return stats
