import unittest
from unittest.mock import Mock, patch, DEFAULT
import logging
import pathlib
from itertools import count
import inspect

import indexer
import core

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(name)s [%(levelname)s] %(message)s')

class TestFileHasher(unittest.TestCase):
    def setUp(self):
        self.indexer = indexer.Indexer(':memory:')
        self.indexer.hash_functions = {
            k: Mock(return_value=f'{k}_result') for k in self.indexer.hash_functions.keys()
        }

    def tearDown(self):
        pass

    def test_single_target(self):
        mock_file = Mock(path='/test/file')
        hashes = self.indexer.file_hasher(mock_file, ['partial_hash'])
        self.indexer.hash_functions['partial_hash'].assert_called_once_with(pathlib.Path(mock_file.path))
        self.assertDictEqual(hashes, {'partial_hash': 'partial_hash_result'})

    def test_multiple_targets(self):
        mock_file = Mock(path='/test/file')
        targets = ['partial_hash', 'image_hash']
        hashes = self.indexer.file_hasher(mock_file, ['partial_hash', 'image_hash'])
        for target in targets:
            self.indexer.hash_functions[target].assert_called_once_with(pathlib.Path(mock_file.path))
            
        self.assertDictEqual(hashes, {'partial_hash': 'partial_hash_result', 'image_hash': 'image_hash_result'})

    def test_target_not_supported(self):
        mock_file = Mock(path='/test/file')
        with self.assertRaises(ValueError):
            self.indexer.file_hasher(mock_file, ['not_supported_hash'])
