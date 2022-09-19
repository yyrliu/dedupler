import unittest
import db as DB

class TestPrepMock(unittest.TestCase):
    def setUp(self):
        self.db = DB.Database(":memory:")
        self.db.initialize()

    def tearDown(self):
        self.db.close()

    def test_insert_new_small_file(self):
        self.db.insertFile("test/path/to/file", 50, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 50, "hashOfTestFile", "hashOfTestFile", None)])

    def test_insert_new_large_file(self):
        self.db.insertFile("test/path/to/file", 3000, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, "hashOfTestFile", None, None)])

    def test_insert_dup_small_file(self):
        self.db.insertFile("test/path/to/file", 50, "hashOfTestFile")
        self.db.insertFile("test/path2/to/file", 50, "hashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        (duplID, *_), *_ = res
        self.assertEqual(res, [(1, "file")])
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 50, "hashOfTestFile", "hashOfTestFile", duplID),
            (2, "test/path2/to/file", 50, "hashOfTestFile", "hashOfTestFile", duplID)])

    def test_insert_dup_large_file(self):
        self.db.insertFile("test/path/to/file", 3000, "hashOfTestFile")
        with self.assertRaises(DB.PartialHashCollisionException) as e:
            self.db.insertFile("test/path2/to/file", 3000, "hashOfTestFile")

        exception = e.exception
        self.assertEqual(exception.id, 1)
        self.assertEqual(exception.path, "test/path/to/file")
        self.assertFalse(exception.has_hash_complete)

    def test_update_file_complete_hash(self):
        self.db.insertFile("test/path/to/file", 3000, "hashOfTestFile")
        id = self.db._lastRowID()
        self.db.updateFileCompleteHash(id, "completeHashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, "hashOfTestFile", "completeHashOfTestFile", None)])

    def test_insert_dup_large_file_with_complete_hash(self):
        self.db.insertFile("test/path/to/file", 3000, "hashOfTestFile")
        id = self.db._lastRowID()
        self.db.updateFileCompleteHash(id, "completeHashOfTestFile")
        self.db.insertFile("test/path2/to/file", 3000, "hashOfTestFile", "completeHashOfTestFile")
        res = self.db._sqlExecute("""SELECT * FROM duplicates""")
        (duplID, *_), *_ = res
        self.assertEqual(res, [(1, "file")])
        res = self.db._sqlExecute("""SELECT * FROM files""")
        self.assertEqual(res, [
            (1, "test/path/to/file", 3000, "hashOfTestFile", "completeHashOfTestFile", duplID),
            (2, "test/path2/to/file", 3000, "hashOfTestFile", "completeHashOfTestFile", duplID)])
        
