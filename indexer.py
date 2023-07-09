from pathlib import Path
from contextlib import closing
from collections.abc import Mapping, Sequence, Generator, Callable

import core
import db
import hash_functions as hf

class Indexer():
    def __init__(self, db_path: Path | str) -> None:
        self.db = db.Database(db_path)
        self.hash_functions: dict[str, Callable] = {
            'partial_hash': hf.partial_hasher,
            'image_hash': hf.image_hasher,
            'full_hash': hf.full_hasher
        }

    def get_target_files(self, root: core.Dir) -> Generator[core.File, None, None]:
        yield from root.getFiles(self.db)
        for child_dir in root.getChildenByDFS(self.db):
            yield from child_dir.getFiles(self.db)

    def run_dir(self, dir: core.Dir, targets: list[str]) -> None:
        for file in dir.getFiles(self.db):
            self.run_one(file, targets)

    def run_all(self, root: core.Dir, targets: list[str]) -> None:
        for file in self.get_target_files(root):
            self.run_one(file, targets)

    def run_one(self, obj: core.File, targets: list[str] = ['partial_hash']) -> None:
        hashes = self.file_hasher(obj, targets)
        self.save_hash(obj, hashes)

    def save_hash(self, obj: core.Dir | core.File, hashes: dict) -> None:
        obj.update(self.db, **hashes)

    def file_hasher(self, file: core.File, targets: list[str]) -> dict[str, str]:
        hashes = {}
        for target in targets:
            try:
                hashes[target] = self.hash_functions[target](Path(file.path))
            except KeyError as e:
                raise ValueError(f"Invalid hashing target: {target}") from e
            
        return hashes
