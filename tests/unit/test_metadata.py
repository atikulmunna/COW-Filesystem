"""Unit tests for the COWFS metadata layer (SQLite)."""

from pathlib import Path

import pytest

from cowfs.metadata import MetadataDB


@pytest.fixture
def db(tmp_path: Path) -> MetadataDB:
    """Create a MetadataDB in a temp directory, initialized with schema."""
    mdb = MetadataDB(tmp_path / "metadata.db")
    mdb.connect()
    mdb.initialize()
    yield mdb
    mdb.close()


class TestMetadataDB:
    """Tests for MetadataDB."""

    def test_root_inode_exists(self, db: MetadataDB) -> None:
        """Root inode (id=1) is created by initialize()."""
        row = db.get_file(1)
        assert row is not None
        assert row["path"] == "/"
        assert row["is_dir"]
        assert row["name"] == ""

    def test_create_file(self, db: MetadataDB) -> None:
        """Create a regular file and retrieve it."""
        inode = db.create_file(
            parent_id=1, name="test.txt", path="/test.txt", mode=33188, uid=1000, gid=1000
        )
        assert inode > 1

        row = db.get_file(inode)
        assert row is not None
        assert row["name"] == "test.txt"
        assert row["path"] == "/test.txt"
        assert not row["is_dir"]
        assert row["uid"] == 1000

    def test_create_directory(self, db: MetadataDB) -> None:
        """Create a directory."""
        inode = db.create_file(
            parent_id=1, name="docs", path="/docs", is_dir=True, mode=16877
        )
        row = db.get_file(inode)
        assert row["is_dir"]

    def test_lookup(self, db: MetadataDB) -> None:
        """Lookup resolves (parent_id, name) -> file row."""
        inode = db.create_file(parent_id=1, name="found.txt", path="/found.txt")
        row = db.lookup(1, "found.txt")
        assert row is not None
        assert row["id"] == inode

    def test_lookup_missing(self, db: MetadataDB) -> None:
        """Lookup returns None for nonexistent files."""
        row = db.lookup(1, "nope.txt")
        assert row is None

    def test_lookup_deleted(self, db: MetadataDB) -> None:
        """Lookup does not find soft-deleted files."""
        inode = db.create_file(parent_id=1, name="del.txt", path="/del.txt")
        db.soft_delete_file(inode)
        row = db.lookup(1, "del.txt")
        assert row is None

    def test_list_children(self, db: MetadataDB) -> None:
        """list_children returns all non-deleted children."""
        db.create_file(parent_id=1, name="a.txt", path="/a.txt")
        db.create_file(parent_id=1, name="b.txt", path="/b.txt")
        inode_c = db.create_file(parent_id=1, name="c.txt", path="/c.txt")
        db.soft_delete_file(inode_c)

        children = db.list_children(1)
        names = {c["name"] for c in children}
        assert "a.txt" in names
        assert "b.txt" in names
        assert "c.txt" not in names

    def test_create_version(self, db: MetadataDB) -> None:
        """Creating a version updates current_version and ref_count."""
        inode = db.create_file(parent_id=1, name="v.txt", path="/v.txt")
        vid = db.create_version(inode, "abc123" + "0" * 58, 100)
        assert vid > 0

        # File's current version is updated
        row = db.get_file(inode)
        assert row["current_version_id"] == vid

        # Object ref_count = 1
        obj = db.get_object("abc123" + "0" * 58)
        assert obj is not None
        assert obj["ref_count"] == 1

    def test_multiple_versions(self, db: MetadataDB) -> None:
        """Multiple versions are tracked in order."""
        inode = db.create_file(parent_id=1, name="multi.txt", path="/multi.txt")
        h1 = "a" * 64
        h2 = "b" * 64
        h3 = "a" * 64  # same as h1 -> dedup

        db.create_version(inode, h1, 10)
        db.create_version(inode, h2, 20)
        v3 = db.create_version(inode, h3, 10)

        versions = db.list_versions(inode)
        assert len(versions) == 3

        # Current version is v3
        row = db.get_file(inode)
        assert row["current_version_id"] == v3

        # h1 (=h3) ref_count should be 2, h2 should be 1
        obj_a = db.get_object(h1)
        assert obj_a["ref_count"] == 2

        obj_b = db.get_object(h2)
        assert obj_b["ref_count"] == 1

    def test_decrement_ref_count(self, db: MetadataDB) -> None:
        """Decrementing ref_count works correctly."""
        inode = db.create_file(parent_id=1, name="ref.txt", path="/ref.txt")
        h = "c" * 64
        db.create_version(inode, h, 5)
        db.create_version(inode, h, 5)  # ref_count = 2

        new_count = db.decrement_ref_count(h)
        assert new_count == 1

        new_count = db.decrement_ref_count(h)
        assert new_count == 0

    def test_rename_file(self, db: MetadataDB) -> None:
        """Rename updates parent, name, and path."""
        inode = db.create_file(parent_id=1, name="old.txt", path="/old.txt")
        db.rename_file(inode, 1, "new.txt", "/new.txt")

        row = db.get_file(inode)
        assert row["name"] == "new.txt"
        assert row["path"] == "/new.txt"

    def test_rename_directory_recursive(self, db: MetadataDB) -> None:
        """Renaming a directory recursively updates children paths."""
        dir_inode = db.create_file(
            parent_id=1, name="src", path="/src", is_dir=True, mode=16877
        )
        child1 = db.create_file(
            parent_id=dir_inode, name="main.py", path="/src/main.py"
        )
        # Nested subdir
        sub_dir = db.create_file(
            parent_id=dir_inode, name="utils", path="/src/utils", is_dir=True, mode=16877
        )
        child2 = db.create_file(
            parent_id=sub_dir, name="helper.py", path="/src/utils/helper.py"
        )

        # Rename /src -> /lib
        db.rename_file(dir_inode, 1, "lib", "/lib")

        # Check all paths updated
        row_dir = db.get_file(dir_inode)
        assert row_dir["path"] == "/lib"

        row_c1 = db.get_file(child1)
        assert row_c1["path"] == "/lib/main.py"

        row_sub = db.get_file(sub_dir)
        assert row_sub["path"] == "/lib/utils"

        row_c2 = db.get_file(child2)
        assert row_c2["path"] == "/lib/utils/helper.py"

    def test_update_attrs(self, db: MetadataDB) -> None:
        """update_attrs changes mode/uid/gid."""
        inode = db.create_file(parent_id=1, name="attr.txt", path="/attr.txt")
        db.update_attrs(inode, mode=33261, uid=500, gid=500)

        row = db.get_file(inode)
        assert row["mode"] == 33261
        assert row["uid"] == 500
        assert row["gid"] == 500

    def test_get_stats(self, db: MetadataDB) -> None:
        """get_stats returns correct counts."""
        stats = db.get_stats()
        assert stats["total_files"] == 0
        assert stats["total_versions"] == 0
        assert stats["total_objects"] == 0

        inode = db.create_file(parent_id=1, name="s.txt", path="/s.txt")
        db.create_version(inode, "d" * 64, 100)

        stats = db.get_stats()
        assert stats["total_files"] == 1
        assert stats["total_versions"] == 1
        assert stats["total_objects"] == 1

    def test_get_file_by_path(self, db: MetadataDB) -> None:
        """get_file_by_path resolves full path."""
        inode = db.create_file(parent_id=1, name="bypath.txt", path="/bypath.txt")
        row = db.get_file_by_path("/bypath.txt")
        assert row is not None
        assert row["id"] == inode

    def test_get_file_by_path_include_deleted(self, db: MetadataDB) -> None:
        """include_deleted=True can resolve soft-deleted files."""
        inode = db.create_file(parent_id=1, name="gone.txt", path="/gone.txt")
        db.soft_delete_file(inode)
        assert db.get_file_by_path("/gone.txt") is None
        row = db.get_file_by_path("/gone.txt", include_deleted=True)
        assert row is not None
        assert row["id"] == inode

    def test_set_file_deleted(self, db: MetadataDB) -> None:
        """set_file_deleted toggles deletion state."""
        inode = db.create_file(parent_id=1, name="toggle.txt", path="/toggle.txt")
        db.set_file_deleted(inode, True)
        assert db.get_file(inode) is None
        db.set_file_deleted(inode, False)
        row = db.get_file(inode)
        assert row is not None
        assert row["id"] == inode

    def test_get_latest_version_before(self, db: MetadataDB) -> None:
        """get_latest_version_before returns the newest eligible version."""
        inode = db.create_file(parent_id=1, name="time.txt", path="/time.txt")
        db.create_version(inode, "f" * 64, 1)
        v2 = db.create_version(inode, "a" * 64, 2)

        row = db.get_latest_version_before(inode, "9999-12-31 23:59:59")
        assert row is not None
        assert row["id"] == v2

        # Before any created version should return None
        row = db.get_latest_version_before(inode, "1970-01-01 00:00:00")
        assert row is None

    def test_orphaned_objects(self, db: MetadataDB) -> None:
        """get_orphaned_objects returns objects with ref_count <= 0."""
        inode = db.create_file(parent_id=1, name="orph.txt", path="/orph.txt")
        h = "e" * 64
        db.create_version(inode, h, 50)
        db.decrement_ref_count(h)  # ref_count = 0

        orphans = db.get_orphaned_objects()
        hashes = {o["hash"] for o in orphans}
        assert h in hashes

    def test_activity_events_from_versions_and_delete(self, db: MetadataDB) -> None:
        """Version writes and file deletions are captured in the events feed."""
        inode = db.create_file(parent_id=1, name="log.txt", path="/log.txt")
        h1 = "1" * 64
        h2 = "2" * 64
        db.create_version(inode, h1, 10, action="WRITE")
        db.create_version(inode, h2, 20, action="RESTORE")
        db.soft_delete_file(inode)

        events = db.list_events(limit=10)
        assert len(events) == 3
        assert events[0]["action"] == "WRITE"
        assert events[1]["action"] == "RESTORE"
        assert events[2]["action"] == "DELETE"
        assert all(e["path"] == "/log.txt" for e in events)
        assert events[0]["version_id"] is not None

    def test_list_events_filter_by_time_window(self, db: MetadataDB) -> None:
        """list_events supports since/until datetime filtering."""
        db.record_event("SNAPSHOT_CREATE", path="snapshot:alpha")
        db.record_event("SNAPSHOT_DELETE", path="snapshot:alpha")

        events = db.list_events(
            limit=10,
            since="1970-01-01 00:00:00",
            until="9999-12-31 23:59:59",
        )
        assert len(events) == 2

        none = db.list_events(
            limit=10,
            since="9999-12-31 23:59:59",
            until="9999-12-31 23:59:59",
        )
        assert none == []
