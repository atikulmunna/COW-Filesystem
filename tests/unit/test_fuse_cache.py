"""Regression tests for FUSE cache coherence."""

from pathlib import Path

import pytest

from cowfs.fuse_handler import COWFS


def test_hash_cache_refreshes_after_external_restore(tmp_path: Path) -> None:
    """Mounted daemon should observe metadata restore without stale hash cache."""
    pytest.importorskip("pyfuse3")

    storage = tmp_path / "storage"
    fs = COWFS(str(storage))
    fs.init()
    try:
        inode = fs.db.create_file(parent_id=1, name="x.txt", path="/x.txt")
        h_old = fs.objects.store_sync(b"old")
        h_new = fs.objects.store_sync(b"new")
        fs.db.create_version(inode, h_old, 3)
        fs.db.create_version(inode, h_new, 3)

        # Prime cache with "new" and then simulate external CLI restore to "old".
        assert fs._get_current_hash_and_size(inode)[0] == h_new
        fs.db.create_version(inode, h_old, 3, action="RESTORE")

        obj_hash, size = fs._get_current_hash_and_size(inode)
        assert obj_hash == h_old
        assert size == 3
    finally:
        fs.shutdown()
