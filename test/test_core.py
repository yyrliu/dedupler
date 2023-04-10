import unittest
import logging
import core
import db as DB

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(name)s [%(levelname)s] %(message)s')

class TestBasebyDir(unittest.TestCase):
    """Test Base by Dir class"""
    def setUp(self):
        self.db = DB.Database(':memory:')
        self.db.initialize()

    def tearDown(self):
        self.db.close()

    def test_insert_dir(self):
        dirDict = { "path": "dir", "parent_dir": None }
        dir = core.Dir.insert(dirDict, self.db._conn)
        self.assertIsInstance(dir, core.Dir)
        for key, value in dirDict.items():
            self.assertEqual(getattr(dir, key), value)
        curs = self.db._conn.execute('SELECT * FROM dirs')
        self.assertDictEqual(curs.fetchone(), {
            'id': 1, 'path': 'dir', 'parent_dir': None, 'depth': 0, 'hash': None, 'duplicate_id': None
        })

    def test_get_dir_by_id(self):
        dirDict = { "path": "dir", "parent_dir": None }
        dir = core.Dir.insert(dirDict, self.db._conn)
        dirFromDB = core.Dir.fromId(dir.id, dbConn=self.db._conn)
        self.assertEqual(dir, dirFromDB)

    def test_update_dir_hash(self):
        dirDict = { "path": "dir", "parent_dir": None }
        dir = core.Dir.insert(dirDict, self.db._conn)
        dir.updateValue(hash='hash')
        self.assertEqual(dir.hash, 'hash')
        dir.update(self.db._conn)
        dirFromDB = core.Dir.fromId(dir.id, self.db._conn)
        self.assertEqual(dirFromDB.hash, 'hash')

    def test_update_dir_hash_again_before_syncing_to_db(self):
        dirDict = { "path": "dir", "parent_dir": None }
        dir = core.Dir.insert(dirDict, self.db._conn)
        dir.updateValue(hash='hash')
        with self.assertRaises(KeyError):
            dir.updateValue(hash='hash2')
        
    def test_get_by_parent_dir(self):
        dirDicts = {
            1: { "path": "dir1", "parent_dir": None },
            2: { "path": "dir1/dir2", "parent_dir": 1 },
            3: { "path": "dir1/dir2/dir3", "parent_dir": 2 },
            4: { "path": "dir1/dir2/dir4", "parent_dir": 2 },
            5: { "path": "dir1/dir5", "parent_dir": 1 },
            6: { "path": "dir1/dir5/dir6", "parent_dir": 5 },
            7: { "path": "dir1/dir5/dir7", "parent_dir": 5 },
            8: { "path": "dir1/dir5/dir6/dir8", "parent_dir": 6 }
        }
        for dirDict in dirDicts.values():
            core.Dir.insert(dirDict, self.db._conn)
        childrenFromDBOf1 = core.Dir.getByParentDir(1, self.db._conn)
        self.assertEqual([2, 5], [ dir.id for dir in childrenFromDBOf1 ])
        childrenFromDBOf2 = core.Dir.getByParentDir(2, self.db._conn)
        self.assertEqual([3, 4], [ dir.id for dir in childrenFromDBOf2 ])
        childrenFromDBOf7 = core.Dir.getByParentDir(7, self.db._conn)
        self.assertEqual([], [ dir.id for dir in childrenFromDBOf7 ])

    def test_instance_can_be_deleted(self):
        dirDict = { "path": "dir", "parent_dir": None }
        dir = core.Dir.insert(dirDict, self.db._conn)
        dir.delete(self.db._conn)
        with self.assertRaises(AttributeError):
            repr(dir)

class TestDir(unittest.TestCase):
    """Test Dir class"""
    def setUp(self):
        self.db = DB.Database(':memory:')
        self.db.initialize()

    def tearDown(self):
        self.db.close()

    def test_insert_dir(self):
        dirDict = { "path": "dir", "parent_dir": None }
        dir = core.Dir.insert(dirDict, self.db._conn)
        self.assertIsInstance(dir, core.Dir)
        for key, value in dirDict.items():
            self.assertEqual(getattr(dir, key), value)
        self.assertEqual(dir.id, 1)
        self.assertEqual(dir.depth, 0)
        curs = self.db._conn.execute('SELECT * FROM dirs')
        self.assertDictEqual(curs.fetchone(), {
            'id': 1, 'path': 'dir', 'parent_dir': None, 'depth': 0, 'hash': None, 'duplicate_id': None
        })

    def test_dir_depth(self):
        dirDicts = {
            1: { "path": "dir1", "parent_dir": None },
            2: { "path": "dir1/dir2", "parent_dir": 1 },
            3: { "path": "dir1/dir2/dir3", "parent_dir": 2 },
            4: { "path": "dir1/dir2/dir4", "parent_dir": 2 },
            5: { "path": "dir1/dir5", "parent_dir": 1 },
            6: { "path": "dir1/dir5/dir6", "parent_dir": 5 },
            7: { "path": "dir1/dir5/dir7", "parent_dir": 5 },
            8: { "path": "dir1/dir5/dir6/dir8", "parent_dir": 6 }
        }
        for dirDict in dirDicts.values():
            core.Dir.insert(dirDict, self.db._conn)
        for dirId, dirDict in dirDicts.items():
            dir = core.Dir.fromId(dirId, self.db._conn)
            self.assertEqual(dir.depth, dirDict['path'].count('/'))

    def test_get_all_dirs_by_DFS(self):
        dirDicts = {
            1: { "path": "dir1", "parent_dir": None },
            2: { "path": "dir1/dir2", "parent_dir": 1 },
            3: { "path": "dir1/dir2/dir3", "parent_dir": 2 },
            4: { "path": "dir1/dir2/dir4", "parent_dir": 2 },
            5: { "path": "dir1/dir5", "parent_dir": 1 },
            6: { "path": "dir1/dir5/dir6", "parent_dir": 5 },
            7: { "path": "dir1/dir5/dir7", "parent_dir": 5 },
            8: { "path": "dir1/dir5/dir6/dir8", "parent_dir": 6 }
        }
        for dirDict in dirDicts.values():
            core.Dir.insert(dirDict, self.db._conn)
        dirsFromDB = core.Dir.getAllByDFS(self.db._conn)
        dfsIdsByDepth = [8, 3, 4, 6, 7, 2, 5, 1]
        self.assertEqual(dfsIdsByDepth, [ dir.id for dir in dirsFromDB ])

    def test_get_files(self):
        dirDicts = [
            { "path": "dir1", "parent_dir": None },
            { "path": "dir1/dir2", "parent_dir": 1 },
            { "path": "dir1/dir3", "parent_dir": 1 }
        ]
        dirs = [ core.Dir.insert(v, self.db._conn) for v in dirDicts ]

        fileDicts = [
            { "path": "dir1/file1", "size": 10, "parent_dir": 1 },
            { "path": "dir1/dir2/file2", "size": 10, "parent_dir": 2 },
            { "path": "dir1/dir3/file3", "size": 10, "parent_dir": 3 },
            { "path": "dir1/dir3/file4", "size": 10, "parent_dir": 3 }
        ]
        for fileDict in fileDicts:
            core.File.insert(fileDict, self.db._conn)

        # { dirId: [fileIds] }
        files = { 1: [1], 2: [2], 3: [3, 4] }

        for dir in dirs:
            dirFiles = dir.getFiles(self.db._conn)
            self.assertEqual(files[dir.id], [dirFiles.id for dirFiles in dirFiles])

class TestFile(unittest.TestCase):
    """Test File class"""
    def setUp(self):
        self.db = DB.Database(':memory:')
        self.db.initialize()
        dirDict = { "path": "dir", "parent_dir": None }
        self.rootDir = core.Dir.insert(dirDict, self.db._conn)

    def tearDown(self):
        self.db.close()

    def test_insert_file(self):
        fileDict = { "path": "dir/file", "size": 10, "parent_dir": self.rootDir.id }
        file = core.File.insert(fileDict, self.db._conn)
        self.assertIsInstance(file, core.File)
        for key, value in fileDict.items():
            self.assertEqual(getattr(file, key), value)
        curs = self.db._conn.execute('SELECT * FROM files')
        self.assertDictEqual(curs.fetchone(), {
            'id': 1, 'path': 'dir/file', 'size': 10, 'parent_dir': self.rootDir.id, 'hash': None, 'complete_hash': None, 'duplicate_id': None
        })

    def test_insert_file_without_parent_dir(self):
        fileDict = { "path": "dir/file", "size": 10 }
        with self.assertRaises(ValueError):
            core.File.insert(fileDict, self.db._conn)
        fileDict["parent_dir"] = None
        with self.assertRaises(ValueError):
            core.File.insert(fileDict, self.db._conn)

    def test_set_complete_hash_to_None(self):
        fileDict = { "path": "dir/file", "size": 10, "parent_dir": self.rootDir.id }
        file = core.File.insert(fileDict, self.db._conn)
        file.update(self.db._conn, hash='hash', complete_hash='complete_hash')
        fileFromDB = core.File.fromId(file.id, self.db._conn)
        self.assertEqual(fileFromDB.hash, 'hash')
        self.assertEqual(fileFromDB.complete_hash, 'complete_hash')
        file.update(self.db._conn, complete_hash=None)
        self.assertEqual(file.complete_hash, None)
        fileFromDB = core.File.fromId(file.id, self.db._conn)
        self.assertEqual(fileFromDB.hash, 'hash')
        self.assertEqual(fileFromDB.complete_hash, None)