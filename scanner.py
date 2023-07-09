from pathlib import Path
from typing import NoReturn
from collections.abc import Generator

import db
import core

class SymlinkFound(Exception):
    pass

class UnexpectedPathType(Exception):
    pass

def dir_dfs(path) -> Generator[tuple[str, Path | None], None, None]:
    '''Generator for all files and directories in a directory, directory path and comes before the children are iterated and "None" comes after everything.'''

    for p in Path(path).iterdir():
        if p.is_symlink():
            yield ('symlink', p)

        if p.is_file():
            yield ('file', p)
            
        if p.is_dir():
            yield ('dir', p)
            yield from dir_dfs(p)
            try:
                yield ('dir', None)
            except IndexError:
                # Reached the end of the stack of directories
                pass

class Scanner():
    def __init__(self, db_path: Path | str, overwrite_db: bool = False) -> None:
        self.db = db.Database(db_path, overwrite_db)
        self.dir_stack: list[tuple[int, Path]] = []

    @property
    def current_dir_id(self) -> int | None:
        try:
            id, _ = self.dir_stack[-1]
        except IndexError:
            id = None

        return id

    def file_handler(self, path: Path) -> None:
        file_dict = {
            "path": str(path),
            "size": path.stat().st_size,
            "parent_dir": self.current_dir_id
        }
        core.File.insert(file_dict, self.db)

    def dir_handler(self, path: Path) -> None:
        if path is None:
            self.dir_stack.pop()
            
        else:
            dir_dict = {
                "path": str(path),
                "parent_dir": self.current_dir_id
            }
            dir = core.Dir.insert(dir_dict, self.db)
            self.dir_stack.append((dir.id, path))

    @staticmethod
    def symlink_handler(path: Path) -> NoReturn:
        raise SymlinkFound(f'Symlink "{path} found in directory, unable to handle it')

    def handlerSwitch(self, type, path) -> None:
        if type == 'dir':
            self.dir_handler(path)
        elif type == 'file':
            self.file_handler(path)
        elif type == 'symlink':
            self.symlink_handler(path)
        else:
            raise UnexpectedPathType

    def scan(self, path: Path) -> None:
        self.dir_handler(path)
        for type, p in dir_dfs(path):
            self.handlerSwitch(type, p)
