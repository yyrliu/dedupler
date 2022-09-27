import unittest
import db as DB
import sqlite3

class TestPrepMock(unittest.TestCase):
    def setUp(self):
        self.db = DB.Database(":memory:")
        self.db.initialize()

    def tearDown(self):
        self.db.close()

    def test_insert_dir(self):
        dirID = self.db.insertDir("test/path/to/dir", self.db.rootDirID)
        self.assertEqual(dirID, 2)
        res = self.db._sqlExecute("""SELECT * FROM dirs""")
        self.assertEqual(res[1], (2, "test/path/to/dir", 1, None, None))

    def test_insert_new_small_file(self):
        dirID = self.db.insertDir("test/path/to", self.db.rootDirID)
        self.db.insertFile("test/path/to/file", 50, dirID, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 50, dirID, "hashOfTestFile", "hashOfTestFile", None)])

    def test_insert_new_small_without_parent_dir(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.insertFile("test/path/to/file", 50, 2, "hashOfTestFile")

    def test_insert_new_large_file(self):
        dirID = self.db.insertDir("test/path/to", self.db.rootDirID)
        self.db.insertFile("test/path/to/file", 3000, dirID, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, dirID, "hashOfTestFile", None, None)])

    def test_insert_dup_small_file(self):
        dirID_1 = self.db.insertDir("test/path/to", self.db.rootDirID)
        dirID_2 = self.db.insertDir("test/path2/to", self.db.rootDirID)
        self.db.insertFile("test/path/to/file", 50, dirID_1, "hashOfTestFile")
        self.db.insertFile("test/path2/to/file", 50, dirID_2, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        (duplID, *_), *_ = res
        self.assertEqual(res, [(1, "file")])
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 50, dirID_1, "hashOfTestFile", "hashOfTestFile", duplID),
            (2, "test/path2/to/file", 50, dirID_2, "hashOfTestFile", "hashOfTestFile", duplID)])

    def test_insert_dup_large_file(self):
        dirID = self.db.insertDir("test/path/to", self.db.rootDirID)
        self.db.insertFile("test/path/to/file", 3000, dirID, "hashOfTestFile")
        with self.assertRaises(DB.PartialHashCollisionException) as e:
            self.db.insertFile("test/path2/to/file", 3000, dirID, "hashOfTestFile")

        exception = e.exception
        self.assertEqual(exception.id, 1)
        self.assertEqual(exception.path, "test/path/to/file")
        self.assertFalse(exception.has_hash_complete)

    def test_update_file_complete_hash(self):
        dirID = self.db.insertDir("test/path/to", self.db.rootDirID)
        self.db.insertFile("test/path/to/file", 3000, dirID, "hashOfTestFile")
        id = self.db._lastRowID()
        self.db.updateFileCompleteHash(id, "completeHashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, dirID, "hashOfTestFile", "completeHashOfTestFile", None)])

    def test_insert_dup_large_file_with_complete_hash(self):
        dirID_1 = self.db.insertDir("test/path/to", self.db.rootDirID)
        dirID_2 = self.db.insertDir("test/path2/to", self.db.rootDirID)
        self.db.insertFile("test/path/to/file", 3000, dirID_1, "hashOfTestFile")
        id = self.db._lastRowID()
        self.db.updateFileCompleteHash(id, "completeHashOfTestFile")
        self.db.insertFile("test/path2/to/file", 3000, dirID_2, "hashOfTestFile", "completeHashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        (duplID, *_), *_ = res
        self.assertEqual(res, [(1, "file")])
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, dirID_1, "hashOfTestFile", "completeHashOfTestFile", duplID),
            (2, "test/path2/to/file", 3000, dirID_2, "hashOfTestFile", "completeHashOfTestFile", duplID)])

    def test_update_dir_hash(self):
        dirID = self.db.insertDir("test/path/to/dir", self.db.rootDirID)
        self.db.updateDirHash(dirID, "hashOfTestDir")
        res = self.db._sqlExecute("""SELECT * FROM dirs""")
        self.assertEqual(res[1], (2, "test/path/to/dir", 1, "hashOfTestDir", None))

    def test_update_dir_hash_create_new_dup(self):
        dirID1 = self.db.insertDir("test/path/to/dir1", self.db.rootDirID)
        dirID2 = self.db.insertDir("test/path/to/dir2", self.db.rootDirID)
        self.db.updateDirHash(dirID1, "hashOfTestDir")
        self.db.updateDirHash(dirID2, "hashOfTestDir")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        (duplID, *_), *_ = res
        res = self.db._sqlExecute("""--sql
            SELECT id, duplicate_id FROM dirs ORDER BY id ASC
        """)
        res = res[1:]
        self.assertEqual(res, [(dirID1, duplID), (dirID2, duplID)])

    def test_update_dir_hash_update_hash(self):
        dirID1 = self.db.insertDir("test/path/to/dir1", self.db.rootDirID)
        dirID2 = self.db.insertDir("test/path/to/dir2", self.db.rootDirID)
        dirID3 = self.db.insertDir("test/path/to/dir3", self.db.rootDirID)
        self.db.updateDirHash(dirID1, "hashOfTestDir")
        self.db.updateDirHash(dirID2, "hashOfTestDir")
        self.db.updateDirHash(dirID3, "hashOfTestDir")
        self.db.updateDirHash(dirID1, "newHashOfTestDir")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        self.assertEqual(res, [(1, "dir")])
        (duplID, *_), *_ = res
        res = self.db._sqlExecute("""--sql
            SELECT id, duplicate_id FROM dirs ORDER BY id ASC
        """)
        res = res[2:]
        self.assertEqual(res, [(dirID2, duplID), (dirID3, duplID)])

    def test_update_dir_hash_same_hash(self):
        dirID1 = self.db.insertDir("test/path/to/dir1", self.db.rootDirID)
        dirID2 = self.db.insertDir("test/path/to/dir2", self.db.rootDirID)
        self.db.updateDirHash(dirID1, "hashOfTestDir")
        self.db.updateDirHash(dirID2, "hashOfTestDir")
        self.db.updateDirHash(dirID1, "hashOfTestDir")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        (duplID, *_), *_ = res
        res = self.db._sqlExecute("""--sql
            SELECT id, duplicate_id FROM dirs ORDER BY id ASC
        """)
        res = res[1:]
        self.assertEqual(res, [(dirID1, duplID), (dirID2, duplID)])

    def test_update_dir_hash_update_hash_and_remove_dup(self):
        dirID1 = self.db.insertDir("test/path/to/dir1", self.db.rootDirID)
        dirID2 = self.db.insertDir("test/path/to/dir2", self.db.rootDirID)
        self.db.updateDirHash(dirID1, "hashOfTestDir")
        self.db.updateDirHash(dirID2, "hashOfTestDir")
        self.db.updateDirHash(dirID1, "newHashOfTestDir")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        self.assertEqual(res, None)
        res = self.db._sqlExecute("""--sql
            SELECT id, duplicate_id FROM dirs ORDER BY id ASC
        """)
        res = res[1:]
        self.assertEqual(res, [(dirID1, None), (dirID2, None)])

    def test_get_children_hashes(self):
        dirID_1 = self.db.insertDir("test/path/to", self.db.rootDirID)
        dirID_2 = self.db.insertDir("test/path2/to", self.db.rootDirID)
        dirID_child = self.db.insertDir("test/path/to/child_dir", dirID_1)
        self.db.updateDirHash(dirID_child, "hashOfTestDir")
        self.db.insertFile("test/path/to/file_small", 3000, dirID_1, "hashOfTestFileSmall")
        self.db.insertFile("test/path/to/file_big", 3000, dirID_1, "hashOfTestFileBig", "completeHashOfTestFile")
        self.db.insertFile("test/path2/to/file", 3000, dirID_2, "hashOfTestFile")
        res = self.db.getChildrenHashes(dirID_1)
        print(res)
        self.assertEqual(res, ['hashOfTestFileSmall', 'completeHashOfTestFile', 'hashOfTestDir'])

    def test_get_dir_hash(self):
        dirID = self.db.insertDir("test/path/to/dir", self.db.rootDirID)
        self.db.updateDirHash(dirID, "hashOfTestDir")
        res = self.db.getDirHash(dirID)
        self.assertEqual(res, "hashOfTestDir")

    def test_get_dir_parent_ID(self):
        dirID1 = self.db.insertDir("test/path/to/dir", self.db.rootDirID)
        dirID2 = self.db.insertDir("test/path/to/dir/child", dirID1)
        res = self.db.getDirParentID(dirID2)
        self.assertEqual(res, dirID1)
        