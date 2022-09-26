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
        dirID = self.db.insertDir("test/path/to/dir")
        self.assertEqual(dirID, 1)
        res = self.db._sqlExecute("""SELECT * FROM dirs""")
        self.assertEqual(res, [(1, "test/path/to/dir", None, None)])

    def test_insert_new_small_file(self):
        dirID = self.db.insertDir("test/path/to")
        self.db.insertFile("test/path/to/file", 50, dirID, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 50, dirID, "hashOfTestFile", "hashOfTestFile", None)])

    def test_insert_new_small_without_parent_dir(self):
        with self.assertRaises(sqlite3.IntegrityError) as e:
            self.db.insertFile("test/path/to/file", 50, 1, "hashOfTestFile")

    def test_insert_new_large_file(self):
        dirID = self.db.insertDir("test/path/to")
        self.db.insertFile("test/path/to/file", 3000, dirID, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, dirID, "hashOfTestFile", None, None)])

    def test_insert_dup_small_file(self):
        dirID_1 = self.db.insertDir("test/path/to")
        dirID_2 = self.db.insertDir("test/path2/to")
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
        dirID = self.db.insertDir("test/path/to")
        self.db.insertFile("test/path/to/file", 3000, dirID, "hashOfTestFile")
        with self.assertRaises(DB.PartialHashCollisionException) as e:
            self.db.insertFile("test/path2/to/file", 3000, dirID, "hashOfTestFile")

        exception = e.exception
        self.assertEqual(exception.id, 1)
        self.assertEqual(exception.path, "test/path/to/file")
        self.assertFalse(exception.has_hash_complete)

    def test_update_file_complete_hash(self):
        dirID = self.db.insertDir("test/path/to")
        self.db.insertFile("test/path/to/file", 3000, dirID, "hashOfTestFile")
        id = self.db._lastRowID()
        self.db.updateFileCompleteHash(id, "completeHashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, dirID, "hashOfTestFile", "completeHashOfTestFile", None)])

    def test_insert_dup_large_file_with_complete_hash(self):
        dirID_1 = self.db.insertDir("test/path/to")
        dirID_2 = self.db.insertDir("test/path2/to")
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
        
