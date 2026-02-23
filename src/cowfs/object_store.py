"""Content-addressable object store for COWFS.

Stores immutable binary objects named by SHA-256 hash in a prefix-sharded
directory structure (like git's .git/objects/).
"""

import hashlib
import os
from pathlib import Path


class ObjectStore:
    """Manages the content-addressable object store on disk."""

    # SHA-256 of empty bytes — well-known hash for empty files
    EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def __init__(self, objects_dir: Path) -> None:
        self.objects_dir = objects_dir
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    def compute_hash(self, data: bytes) -> str:
        """Compute SHA-256 hex digest of data."""
        return hashlib.sha256(data).hexdigest()

    def object_path(self, obj_hash: str) -> Path:
        """Return the filesystem path for a given object hash.

        Uses 2-char prefix sharding: objects/a3/f9c2d4e1b8a7...
        """
        return self.objects_dir / obj_hash[:2] / obj_hash[2:]

    def exists(self, obj_hash: str) -> bool:
        """Check if an object already exists on disk."""
        return self.object_path(obj_hash).exists()

    async def store(self, data: bytes) -> str:
        """Store data as an immutable object. Returns the SHA-256 hash.

        If the object already exists (deduplication), the write is skipped.
        Uses trio.to_thread to avoid blocking the event loop.
        """
        import trio

        obj_hash = self.compute_hash(data)
        obj_path = self.object_path(obj_hash)

        if obj_path.exists():
            return obj_hash  # deduplication: already stored

        await trio.to_thread.run_sync(self._write_object, obj_path, data)
        return obj_hash

    def store_sync(self, data: bytes) -> str:
        """Synchronous version of store() for use outside the event loop."""
        obj_hash = self.compute_hash(data)
        obj_path = self.object_path(obj_hash)
        if not obj_path.exists():
            self._write_object(obj_path, data)
        return obj_hash

    def _write_object(self, obj_path: Path, data: bytes) -> None:
        """Atomically write object to disk with fsync.

        Writes to a temp file first, then renames for atomicity.
        """
        obj_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to a temporary file first, then rename for atomicity
        tmp_path = obj_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            # Atomic rename (on same filesystem)
            tmp_path.rename(obj_path)
        except BaseException:
            # Clean up temp file on any failure
            tmp_path.unlink(missing_ok=True)
            raise

    async def read(self, obj_hash: str) -> bytes:
        """Read object content by hash. Uses trio.to_thread for async I/O."""
        import trio

        obj_path = self.object_path(obj_hash)
        return await trio.to_thread.run_sync(obj_path.read_bytes)

    def read_sync(self, obj_hash: str) -> bytes:
        """Synchronous version of read()."""
        return self.object_path(obj_hash).read_bytes()

    async def delete(self, obj_hash: str) -> int:
        """Delete an object from disk. Returns bytes freed."""
        import trio

        obj_path = self.object_path(obj_hash)
        if not obj_path.exists():
            return 0
        size = obj_path.stat().st_size
        await trio.to_thread.run_sync(obj_path.unlink)
        # Clean up empty prefix directory
        try:
            obj_path.parent.rmdir()  # only succeeds if empty
        except OSError:
            pass
        return size

    def delete_sync(self, obj_hash: str) -> int:
        """Synchronous version of delete()."""
        obj_path = self.object_path(obj_hash)
        if not obj_path.exists():
            return 0
        size = obj_path.stat().st_size
        obj_path.unlink()
        try:
            obj_path.parent.rmdir()
        except OSError:
            pass
        return size
