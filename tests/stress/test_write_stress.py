"""Stress tests for large version/write volumes."""

import random
from pathlib import Path

from cowfs.metadata import MetadataDB
from cowfs.object_store import ObjectStore


def test_stress_1000_writes_versioning_and_dedup(tmp_path: Path) -> None:
    """Run 1000 writes and verify version count and dedup behavior."""
    storage = tmp_path / "storage"
    storage.mkdir(parents=True, exist_ok=True)

    db = MetadataDB(storage / "metadata.db")
    db.connect()
    db.initialize()
    store = ObjectStore(storage / "objects")

    inode = db.create_file(parent_id=1, name="stress.txt", path="/stress.txt")
    rng = random.Random(42)

    duplicate_pool = [
        (f"common-payload-{i}-" + ("x" * 256)).encode("utf-8")
        for i in range(40)
    ]

    total_writes = 1000
    unique_hashes: set[str] = set()
    last_hash = ""
    logical_bytes = 0

    for i in range(total_writes):
        if i % 5 == 0:
            payload = f"unique-write-{i}-{rng.getrandbits(64)}".encode()
        else:
            payload = duplicate_pool[rng.randrange(len(duplicate_pool))]

        obj_hash = store.store_sync(payload)
        db.create_version(inode, obj_hash, len(payload))
        unique_hashes.add(obj_hash)
        last_hash = obj_hash
        logical_bytes += len(payload)

    versions = db.list_versions(inode)
    stats = db.get_stats()
    current = db.get_current_version(inode)
    db.close()

    assert len(versions) == total_writes
    assert stats["total_versions"] == total_writes
    assert stats["total_objects"] == len(unique_hashes)
    assert stats["total_objects"] < total_writes  # dedup happened
    assert stats["logical_size_bytes"] == logical_bytes
    assert stats["actual_size_bytes"] < stats["logical_size_bytes"]
    assert current is not None
    assert current["object_hash"] == last_hash
