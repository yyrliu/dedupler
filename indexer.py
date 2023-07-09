from pathlib import Path
from contextlib import closing
from collections.abc import Mapping, Sequence, Generator, Callable
import logging

import core
import db
import hash_functions as hf

logger = logging.getLogger(__name__)

class Indexer():
    def __init__(self, db_path: Path | str) -> None:
        self.db = db.Database(db_path)
        self.hash_functions: dict[str, Callable] = {
            'partial_hash': hf.partial_hasher,
            'image_hash': hf.image_hasher,
            'full_hash': hf.full_hasher
        }

    def get_files_in_dir(self, dir: core.Dir, recursive: bool = False) -> Generator[core.File, None, None]:
        yield from dir.getFiles(self.db)
        if recursive:
            for child_dir in dir.getChildenByDFS(self.db):
                yield from child_dir.getFiles(self.db)

    def index_files_in_dir(self, dir: core.Dir, recursive: bool = False, force_reindex: bool = False) -> None:
        for file in self.get_files_in_dir(dir, recursive):
            if force_reindex or file.partial_hash is None:
                self.index_file(file)
            else:
                logger.info(f'Skipping file (already indexed): {file.path} ')

    def index_file(self, file: core.File) -> None:
        logger.info(f"Indexing file: {file.path}")
        file_hash = self.file_hasher(file, 'partial_hash')
        self.save_hash(file, file_hash)

        if file.is_image:
            logger.info(f"Indexing image: {file.path}")
            photo_dict = {
                "file_id": file.id,
                **self.file_hasher(file, 'image_hash'),
            }
            core.Photo.insert(photo_dict, self.db)

    def save_hash(self, obj: core.Dir | core.File, hashes: dict) -> None:
        obj.update(self.db, **hashes)

    def file_hasher(self, file: core.File, target: str) -> dict[str, str]:
        hash = {}
        try:
            hash[target] = self.hash_functions[target](Path(file.path))
        except KeyError as e:
            raise ValueError(f"Invalid hashing target: {target}") from e
            
        return hash
