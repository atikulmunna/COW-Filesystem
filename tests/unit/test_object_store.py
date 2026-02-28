"""Unit tests for the COWFS object store."""

from pathlib import Path

import pytest
import trio

from cowfs.object_store import ObjectStore


@pytest.fixture
def obj_store(tmp_path: Path) -> ObjectStore:
    """Create an ObjectStore in a temp directory."""
    return ObjectStore(tmp_path / "objects")


class TestObjectStore:
    """Tests for ObjectStore."""

    def test_compute_hash_deterministic(self, obj_store: ObjectStore) -> None:
        """Same content always produces the same hash."""
        data = b"hello world"
        h1 = obj_store.compute_hash(data)
        h2 = obj_store.compute_hash(data)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex = 64 chars

    def test_compute_hash_different_content(self, obj_store: ObjectStore) -> None:
        """Different content produces different hashes."""
        h1 = obj_store.compute_hash(b"hello")
        h2 = obj_store.compute_hash(b"world")
        assert h1 != h2

    def test_empty_hash_constant(self, obj_store: ObjectStore) -> None:
        """SHA-256 of empty bytes matches well-known value."""
        assert obj_store.compute_hash(b"") == ObjectStore.EMPTY_HASH

    def test_object_path_sharding(self, obj_store: ObjectStore) -> None:
        """Object path uses 2-char prefix sharding."""
        h = "a3f9c2d4e1b8a7" + "0" * 50  # 64 chars
        path = obj_store.object_path(h)
        assert path.parent.name == "a3"
        assert path.name == h[2:]

    def test_store_sync_and_read_sync(self, obj_store: ObjectStore) -> None:
        """store_sync writes, read_sync retrieves the same bytes."""
        data = b"test content 12345"
        obj_hash = obj_store.store_sync(data)
        retrieved = obj_store.read_sync(obj_hash)
        assert retrieved == data

    def test_store_sync_dedup(self, obj_store: ObjectStore) -> None:
        """Storing same content twice returns same hash, no duplicate on disk."""
        data = b"duplicate me"
        h1 = obj_store.store_sync(data)
        h2 = obj_store.store_sync(data)
        assert h1 == h2
        # Only one file on disk
        obj_path = obj_store.object_path(h1)
        assert obj_path.exists()

    def test_store_sync_different_content(self, obj_store: ObjectStore) -> None:
        """Different content produces different objects."""
        h1 = obj_store.store_sync(b"content A")
        h2 = obj_store.store_sync(b"content B")
        assert h1 != h2
        assert obj_store.exists(h1)
        assert obj_store.exists(h2)

    def test_exists(self, obj_store: ObjectStore) -> None:
        """exists() returns correct result."""
        h = obj_store.store_sync(b"exists test")
        assert obj_store.exists(h)
        assert not obj_store.exists("0" * 64)

    def test_delete_sync(self, obj_store: ObjectStore) -> None:
        """delete_sync removes the object and returns freed bytes."""
        data = b"delete me please"
        h = obj_store.store_sync(data)
        assert obj_store.exists(h)

        freed = obj_store.delete_sync(h)
        assert freed == len(data)
        assert not obj_store.exists(h)

    def test_delete_sync_nonexistent(self, obj_store: ObjectStore) -> None:
        """Deleting a nonexistent object returns 0."""
        freed = obj_store.delete_sync("0" * 64)
        assert freed == 0

    def test_store_empty(self, obj_store: ObjectStore) -> None:
        """Empty content is stored correctly."""
        h = obj_store.store_sync(b"")
        assert h == ObjectStore.EMPTY_HASH
        assert obj_store.read_sync(h) == b""

    def test_store_binary(self, obj_store: ObjectStore) -> None:
        """Binary content (all byte values) is stored correctly."""
        data = bytes(range(256)) * 100
        h = obj_store.store_sync(data)
        assert obj_store.read_sync(h) == data

    def test_async_store_and_read(self, obj_store: ObjectStore) -> None:
        """Async store and read work correctly."""
        async def _test():
            data = b"async content"
            h = await obj_store.store(data)
            retrieved = await obj_store.read(h)
            assert retrieved == data

        trio.run(_test)

    def test_async_dedup(self, obj_store: ObjectStore) -> None:
        """Async dedup: same content stored twice returns same hash."""
        async def _test():
            data = b"async dedup"
            h1 = await obj_store.store(data)
            h2 = await obj_store.store(data)
            assert h1 == h2

        trio.run(_test)

    def test_async_delete(self, obj_store: ObjectStore) -> None:
        """Async delete works correctly."""
        async def _test():
            data = b"async delete me"
            h = await obj_store.store(data)
            freed = await obj_store.delete(h)
            assert freed == len(data)
            assert not obj_store.exists(h)

        trio.run(_test)
