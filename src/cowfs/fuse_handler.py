"""COWFS FUSE handler — the core filesystem implementation.

Uses pyfuse3 (Trio-based) with synchronous SQLite and async object I/O.
"""

import errno
import logging
import os
import stat
import threading
import time
from pathlib import Path

import pyfuse3
import trio

from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore

log = logging.getLogger("cowfs.fuse")


class FileHandle:
    __slots__ = ("dirty", "flags", "inode")

    def __init__(self, inode: int, flags: int) -> None:
        self.inode = inode
        self.flags = flags
        self.dirty = False


class COWFS(pyfuse3.Operations):
    """Copy-on-Write FUSE filesystem using pyfuse3 (Trio)."""

    def __init__(self, storage_root: str) -> None:
        super().__init__()
        self.storage_root = Path(storage_root)
        self.objects = ObjectStore(self.storage_root / "objects")
        self.db = MetadataDB(self.storage_root / "metadata.db")

        self._write_buffers: dict[int, bytearray] = {}
        self._inode_locks: dict[int, threading.Lock] = {}
        self._file_handles: dict[int, FileHandle] = {}
        self._dir_handles: dict[int, int] = {}  # fh → inode for opendir
        self._next_fh: int = 1

        # Cache: inode → (object_hash, size)
        self._inode_hash_cache: dict[int, str] = {}
        self._inode_size_cache: dict[int, int] = {}

    def init(self) -> None:
        """Initialize DB (called before FUSE loop starts)."""
        self.db.connect()
        self.db.initialize()
        # Store empty object so empty files work
        self.objects.store_sync(b"")
        # Fix root inode ownership to current user so default_permissions works
        uid, gid = os.getuid(), os.getgid()
        self.db.update_attrs(1, uid=uid, gid=gid)

    def shutdown(self) -> None:
        self.db.close()

    def _get_inode_lock(self, inode: int) -> threading.Lock:
        if inode not in self._inode_locks:
            self._inode_locks[inode] = threading.Lock()
        return self._inode_locks[inode]

    def _alloc_fh(self, inode: int, flags: int) -> int:
        fh = self._next_fh
        self._next_fh += 1
        self._file_handles[fh] = FileHandle(inode, flags)
        return fh

    def _get_fh(self, fh: int) -> FileHandle:
        return self._file_handles[fh]

    def _invalidate_cache(self, inode: int) -> None:
        self._inode_hash_cache.pop(inode, None)
        self._inode_size_cache.pop(inode, None)

    def _get_current_hash_and_size(self, inode: int) -> tuple[str, int]:
        if inode in self._inode_hash_cache:
            return self._inode_hash_cache[inode], self._inode_size_cache[inode]

        version = self.db.get_current_version(inode)
        if version is None:
            return ObjectStore.EMPTY_HASH, 0

        obj_hash = version["object_hash"]
        size = version["size_bytes"]
        self._inode_hash_cache[inode] = obj_hash
        self._inode_size_cache[inode] = size
        return obj_hash, size

    def _make_entry(self, row) -> pyfuse3.EntryAttributes:
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = row["id"]
        entry.generation = 0
        entry.entry_timeout = 1
        entry.attr_timeout = 1

        is_dir = row["is_dir"]
        entry.st_mode = row["mode"]
        entry.st_uid = row["uid"]
        entry.st_gid = row["gid"]
        entry.st_nlink = 2 if is_dir else 1

        inode = row["id"]
        if is_dir:
            entry.st_size = 4096
        elif inode in self._write_buffers:
            entry.st_size = len(self._write_buffers[inode])
        elif inode in self._inode_size_cache:
            entry.st_size = self._inode_size_cache[inode]
        else:
            # Try to populate cache
            version = self.db.get_current_version(inode)
            if version:
                entry.st_size = version["size_bytes"]
                self._inode_hash_cache[inode] = version["object_hash"]
                self._inode_size_cache[inode] = version["size_bytes"]
            else:
                entry.st_size = 0

        now_ns = int(time.time() * 1e9)
        entry.st_atime_ns = now_ns
        entry.st_mtime_ns = now_ns
        entry.st_ctime_ns = now_ns
        entry.st_blksize = 4096
        entry.st_blocks = (entry.st_size + 511) // 512
        return entry

    # ═══════════════════════════ FUSE Operations ═══════════════════════════

    async def lookup(self, parent_inode, name, ctx):
        name_str = name.decode("utf-8") if isinstance(name, bytes) else name
        log.debug("lookup(parent=%d, name=%s)", parent_inode, name_str)

        row = self.db.lookup(parent_inode, name_str)
        if row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        inode = row["id"]
        if not row["is_dir"] and row["current_version_id"]:
            self._get_current_hash_and_size(inode)

        return self._make_entry(row)

    async def getattr(self, inode, ctx):
        log.debug("getattr(inode=%d)", inode)
        row = self.db.get_file(inode)
        if row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if not row["is_dir"] and row["current_version_id"]:
            self._get_current_hash_and_size(inode)
        return self._make_entry(row)

    async def setattr(self, inode, attr, fields, fh, ctx):
        log.debug("setattr(inode=%d)", inode)

        mode = attr.st_mode if fields.update_mode else None
        uid = attr.st_uid if fields.update_uid else None
        gid = attr.st_gid if fields.update_gid else None

        if mode is not None or uid is not None or gid is not None:
            self.db.update_attrs(inode, mode=mode, uid=uid, gid=gid)

        if fields.update_size:
            lock = self._get_inode_lock(inode)
            with lock:
                if inode not in self._write_buffers:
                    obj_hash, _ = self._get_current_hash_and_size(inode)
                    current_data = self.objects.read_sync(obj_hash)
                    self._write_buffers[inode] = bytearray(current_data)
                buf = self._write_buffers[inode]
                new_size = attr.st_size
                if new_size < len(buf):
                    del buf[new_size:]
                else:
                    buf.extend(b"\x00" * (new_size - len(buf)))
                for fh_obj in self._file_handles.values():
                    if fh_obj.inode == inode:
                        fh_obj.dirty = True

        return await self.getattr(inode, ctx)

    async def opendir(self, inode, ctx):
        log.debug("opendir(inode=%d)", inode)
        row = self.db.get_file(inode)
        if row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if not row["is_dir"]:
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        fh = self._next_fh
        self._next_fh += 1
        self._dir_handles[fh] = inode
        return fh

    async def releasedir(self, fh):
        log.debug("releasedir(fh=%d)", fh)
        self._dir_handles.pop(fh, None)

    async def readdir(self, fh, start_id, token):
        inode = self._dir_handles.get(fh, pyfuse3.ROOT_INODE)
        log.debug("readdir(fh=%d, inode=%d, start_id=%d)", fh, inode, start_id)
        try:
            children = self.db.list_children(inode)
            for i, child in enumerate(children):
                if i < start_id:
                    continue
                name = child["name"].encode("utf-8")
                child_attr = self._make_entry(child)
                if not pyfuse3.readdir_reply(token, name, child_attr, i + 1):
                    break
        except Exception:
            log.exception("readdir failed for inode=%d fh=%d", inode, fh)
            raise

    async def open(self, inode, flags, ctx):
        log.debug("open(inode=%d, flags=%o)", inode, flags)
        row = self.db.get_file(inode)
        if row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if row["is_dir"]:
            raise pyfuse3.FUSEError(errno.EISDIR)

        fh = self._alloc_fh(inode, flags)
        fi = pyfuse3.FileInfo()
        fi.fh = fh
        fi.keep_cache = True
        return fi

    async def create(self, parent_inode, name, mode, flags, ctx):
        name_str = name.decode("utf-8") if isinstance(name, bytes) else name
        log.debug("create(parent=%d, name=%s, mode=%o)", parent_inode, name_str, mode)

        parent = self.db.get_file(parent_inode)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        parent_path = parent["path"]
        full_path = f"/{name_str}" if parent_path == "/" else f"{parent_path}/{name_str}"

        if not stat.S_ISREG(mode):
            mode = stat.S_IFREG | (mode & 0o7777)

        inode = self.db.create_file(
            parent_id=parent_inode,
            name=name_str,
            path=full_path,
            is_dir=False,
            mode=mode,
            uid=ctx.uid,
            gid=ctx.gid,
        )

        empty_hash = self.objects.store_sync(b"")
        self.db.create_version(inode, empty_hash, 0)

        self._write_buffers[inode] = bytearray()

        fh = self._alloc_fh(inode, flags)
        self._file_handles[fh].dirty = True

        fi = pyfuse3.FileInfo()
        fi.fh = fh
        fi.keep_cache = True

        row = self.db.get_file(inode)
        entry = self._make_entry(row)
        return fi, entry

    async def read(self, fh, offset, length):
        handle = self._get_fh(fh)
        inode = handle.inode
        log.debug("read(inode=%d, offset=%d, length=%d)", inode, offset, length)

        if inode in self._write_buffers:
            buf = self._write_buffers[inode]
            return bytes(buf[offset:offset + length])

        obj_hash, _ = self._get_current_hash_and_size(inode)
        data = await trio.to_thread.run_sync(self.objects.read_sync, obj_hash)
        return data[offset:offset + length]

    async def write(self, fh, offset, buf):
        handle = self._get_fh(fh)
        inode = handle.inode
        log.debug("write(inode=%d, offset=%d, len=%d)", inode, offset, len(buf))

        lock = self._get_inode_lock(inode)
        with lock:
            if inode not in self._write_buffers:
                obj_hash, _ = self._get_current_hash_and_size(inode)
                current_data = self.objects.read_sync(obj_hash)
                self._write_buffers[inode] = bytearray(current_data)

            data = self._write_buffers[inode]
            end = offset + len(buf)
            if end > len(data):
                data.extend(b"\x00" * (end - len(data)))
            data[offset:end] = buf
            handle.dirty = True

        return len(buf)

    def _flush_inode_sync(self, inode: int) -> None:
        """Flush dirty buffer for an inode: hash → store → create version."""
        lock = self._get_inode_lock(inode)
        with lock:
            if inode not in self._write_buffers:
                return
            new_data = bytes(self._write_buffers.pop(inode))

        obj_hash = self.objects.store_sync(new_data)
        self.db.create_version(inode, obj_hash, len(new_data))

        self._inode_hash_cache[inode] = obj_hash
        self._inode_size_cache[inode] = len(new_data)
        log.debug("flush: inode=%d, hash=%s, size=%d", inode, obj_hash[:12], len(new_data))

    async def flush(self, fh):
        handle = self._get_fh(fh)
        log.debug("flush(inode=%d, dirty=%s)", handle.inode, handle.dirty)
        if handle.dirty:
            await trio.to_thread.run_sync(self._flush_inode_sync, handle.inode)
            handle.dirty = False

    async def fsync(self, fh, datasync):
        handle = self._get_fh(fh)
        log.debug("fsync(inode=%d, dirty=%s)", handle.inode, handle.dirty)
        if handle.dirty:
            await trio.to_thread.run_sync(self._flush_inode_sync, handle.inode)
            handle.dirty = False

    async def release(self, fh):
        handle = self._get_fh(fh)
        log.debug("release(inode=%d)", handle.inode)

        if handle.dirty:
            await trio.to_thread.run_sync(self._flush_inode_sync, handle.inode)

        inode = handle.inode
        del self._file_handles[fh]

        still_open = any(h.inode == inode for h in self._file_handles.values())
        if not still_open:
            self._write_buffers.pop(inode, None)

    async def unlink(self, parent_inode, name, ctx):
        name_str = name.decode("utf-8") if isinstance(name, bytes) else name
        log.debug("unlink(parent=%d, name=%s)", parent_inode, name_str)

        row = self.db.lookup(parent_inode, name_str)
        if row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if row["is_dir"]:
            raise pyfuse3.FUSEError(errno.EISDIR)

        inode = row["id"]
        self.db.soft_delete_file(inode)
        self._invalidate_cache(inode)

    async def mkdir(self, parent_inode, name, mode, ctx):
        name_str = name.decode("utf-8") if isinstance(name, bytes) else name
        log.debug("mkdir(parent=%d, name=%s, mode=%o)", parent_inode, name_str, mode)

        parent = self.db.get_file(parent_inode)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        parent_path = parent["path"]
        full_path = f"/{name_str}" if parent_path == "/" else f"{parent_path}/{name_str}"

        if not stat.S_ISDIR(mode):
            mode = stat.S_IFDIR | (mode & 0o7777)

        inode = self.db.create_file(
            parent_id=parent_inode,
            name=name_str,
            path=full_path,
            is_dir=True,
            mode=mode,
            uid=ctx.uid,
            gid=ctx.gid,
        )

        row = self.db.get_file(inode)
        return self._make_entry(row)

    async def rmdir(self, parent_inode, name, ctx):
        name_str = name.decode("utf-8") if isinstance(name, bytes) else name
        log.debug("rmdir(parent=%d, name=%s)", parent_inode, name_str)

        row = self.db.lookup(parent_inode, name_str)
        if row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if not row["is_dir"]:
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        inode = row["id"]
        children = self.db.list_children(inode)
        if children:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        self.db.soft_delete_file(inode)

    async def rename(self, old_parent_inode, old_name, new_parent_inode, new_name, flags, ctx):
        old_name_str = old_name.decode("utf-8") if isinstance(old_name, bytes) else old_name
        new_name_str = new_name.decode("utf-8") if isinstance(new_name, bytes) else new_name
        log.debug("rename(%s -> %s)", old_name_str, new_name_str)

        src_row = self.db.lookup(old_parent_inode, old_name_str)
        if src_row is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        new_parent = self.db.get_file(new_parent_inode)
        if new_parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        new_parent_path = new_parent["path"]
        new_path = (
            f"/{new_name_str}" if new_parent_path == "/" else f"{new_parent_path}/{new_name_str}"
        )

        dst_row = self.db.lookup(new_parent_inode, new_name_str)
        if dst_row is not None:
            dst_inode = dst_row["id"]
            if dst_row["is_dir"]:
                children = self.db.list_children(dst_inode)
                if children:
                    raise pyfuse3.FUSEError(errno.ENOTEMPTY)
            self.db.soft_delete_file(dst_inode)
            self._invalidate_cache(dst_inode)

        self.db.rename_file(src_row["id"], new_parent_inode, new_name_str, new_path)
        self._invalidate_cache(src_row["id"])

    async def statfs(self, ctx):
        log.debug("statfs()")
        stats = self.db.get_stats()

        svfs = pyfuse3.StatvfsData()
        try:
            os_stat = os.statvfs(str(self.storage_root))
            svfs.f_bsize = os_stat.f_bsize
            svfs.f_frsize = os_stat.f_frsize
            svfs.f_blocks = os_stat.f_blocks
            svfs.f_bfree = os_stat.f_bfree
            svfs.f_bavail = os_stat.f_bavail
        except OSError:
            svfs.f_bsize = 4096
            svfs.f_frsize = 4096
            svfs.f_blocks = 0
            svfs.f_bfree = 0
            svfs.f_bavail = 0

        svfs.f_files = stats["total_files"]
        svfs.f_ffree = 0
        svfs.f_favail = 0
        svfs.f_namemax = 255
        return svfs

    # ──────────────── Unsupported operations ────────────────

    async def symlink(self, parent_inode, name, target, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)

    async def readlink(self, inode, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)

    async def link(self, inode, new_parent_inode, new_name, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)

    async def getxattr(self, inode, name, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)

    async def setxattr(self, inode, name, value, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)

    async def listxattr(self, inode, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)

    async def removexattr(self, inode, name, ctx):
        raise pyfuse3.FUSEError(errno.ENOTSUP)
