import unittest
from unittest.mock import Mock, patch, DEFAULT
import logging
import pathlib
from itertools import count

import scanner as SC

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(name)s [%(levelname)s] %(message)s')

class TestScanner(unittest.TestCase):
    def setUp(self):
        self.scanner = SC.Scanner(':memory:')

    def tearDown(self):
        pass

    def test_get_root_dir_id(self):
        assert self.scanner.current_dir_id is None

    def test_get_dir_id(self):
        self.scanner.dir_stack.append((1, '/test'))
        assert self.scanner.current_dir_id == 1

class TestFileHandler(unittest.TestCase):
    def setUp(self):
        self.scanner = SC.Scanner(':memory:')

    def tearDown(self):
        pass
    
    @patch('core.File.insert')
    @patch('pathlib.Path')
    def test_file_handler(self, mock_Path, mock_core_file_insert):
        path = pathlib.Path("/test/file")
        self.scanner.file_handler(path)
        mock_core_file_insert.assert_called_once_with({
            'path': str(path),
            'size': path.stat().st_size,
            'parent_dir': None
        }, self.scanner.db)

class TestDirHandler(unittest.TestCase):
    def setUp(self):
        self.scanner = SC.Scanner(':memory:')
        patcher = patch('core.Dir.insert', side_effect=(Mock(id=i) for i in count()))
        self.mock_core_dir_insert = patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        pass

    def test_insert_root_dir(self):
        path = pathlib.Path("/test")
        self.scanner.dir_handler(path)
        self.mock_core_dir_insert.assert_called_once_with({
            'path': str(path),
            'parent_dir': None
        }, self.scanner.db)

    def test_insert_dir(self):
        root_dir_path = pathlib.Path("/test")
        dir_path = pathlib.Path("/test/dir")
        self.scanner.dir_handler(root_dir_path)
        self.scanner.dir_handler(dir_path)
        self.mock_core_dir_insert.assert_called_with({
            'path': str(dir_path),
            'parent_dir': 0
        }, self.scanner.db)
    
    def test_pop_dir(self):
        root_dir_path = pathlib.Path("/test")
        dir_path = pathlib.Path("/test/dir")
        self.scanner.dir_handler(root_dir_path)
        self.scanner.dir_handler(dir_path)
        self.scanner.dir_handler(None)
        assert self.scanner.current_dir_id == 0

        dir2_path = pathlib.Path("/test/dir2")
        self.scanner.dir_handler(dir2_path)
        self.mock_core_dir_insert.assert_called_with({
            'path': str(dir2_path),
            'parent_dir': 0
        }, self.scanner.db)
    
    def test_pop_dir_empty_stack(self):
        with self.assertRaises(IndexError):
            self.scanner.dir_handler(None)

class TestSymlinkHandler(unittest.TestCase):
    def setUp(self):
        self.scanner = SC.Scanner(':memory:')

    def tearDown(self):
        pass

    @patch('pathlib.Path')
    def test_symlink_handler(self, mock_Path):
        path = pathlib.Path("/test/symlink")
        with self.assertRaises(SC.SymlinkFound):
            self.scanner.symlink_handler(path)

class TestHandlerSwitch(unittest.TestCase):
    def setUp(self):
        self.scanner = SC.Scanner(':memory:')
        patcher = patch.multiple(
            'scanner.Scanner',
            file_handler=DEFAULT,
            dir_handler=DEFAULT,
            symlink_handler=DEFAULT
        )
        self.mocks = patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        pass

    def assertions(self, target, *args, **kwargs):
        for key, mock in self.mocks.items():
            if key == target:
                mock.assert_called_once_with(*args, **kwargs)
            else:
                mock.assert_not_called()

    def test_switch_file(self):
        path = pathlib.Path("/test/file")
        self.scanner.handlerSwitch('file', path)
        self.assertions('file_handler', path)

    def test_switch_dir(self):
        path = pathlib.Path("/test/dir")
        self.scanner.handlerSwitch('dir', path)
        self.assertions('dir_handler', path)

    def test_switch_symlink(self):
        path = pathlib.Path("/test/symlink")
        self.scanner.handlerSwitch('symlink', path)
        self.assertions('symlink_handler', path)

    def test_unexpected_path_type(self):
        path = pathlib.Path("/test/unknown")
        with self.assertRaises(SC.UnexpectedPathType):
            self.scanner.handlerSwitch('unknown', path)

class TestDirDFS(unittest.TestCase):
    def setUp(self):
        self.scanner = SC.Scanner(':memory:')
        
    def tearDown(self):
        pass

    def test_dir_dfs(self):
        pass
