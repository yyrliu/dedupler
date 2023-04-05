import hashlib
from pathlib import Path
from typing import NoReturn

import fs_utlis
import db as DB
import hashers


class SymlinkFound(Exception):
    pass

class UnexpectedPathType(Exception):
    pass

# set of extensions that are supported by PIL.Image
img_extensions = ('.jpg', '.png', '.gif', '.tiff', '.jpeg', '.bmp')

class Scanner():
    def __init__(self, db_path: Path | str) -> None:
        self.db = DB.Database(db_path)
        self.db.initialize()
        self.dir_stack = []

    @property
    def current_dir_id(self) -> int:
        try:
            id, _ = self.dir_stack[-1]
        except IndexError:
            id = self.db.rootDirID

        return id

    def file_handler(self, path: Path) -> None:
        size = path.stat().st_size
        fileID = self.db.insertFile(str(path), size, self.current_dir_id)

        if path.suffix.casefold() in img_extensions:
            image_hash = hashers.image_hasher(path)
            self.db.updateFileHash(fileID, image_hash, image_hash)

        else:
            partial_hash = hashers.partial_hasher(path, size)
            try:
                self.db.updateFileHash(fileID, partial_hash, partial_hash)
            # Catch exception if identical partial hash exists
            except DB.PartialHashCollisionException as e:
                # Add complete hash to collided file if not exists
                if not e.has_hash_complete:
                    e_full_hash = hashers.full_hasher(e.path)
                    self.db.updateFileCompleteHash(e.id, e_full_hash)
                    self.dir_hash_update(e.dir_id)

                # Resummit insertion request
                full_hash = hashers.full_hasher(path)
                self.db.updateFileHash(fileID, partial_hash, partial_hash, full_hash)

    def dir_handler(self, path: Path) -> None:
        if path is None:
            id, path = self.dir_stack.pop()
            self.dir_hash_update(id)
            
        else:
            id = self.db.insertDir(str(path), self.current_dir_id)
            self.dir_stack.append((id, path))

    def dir_hasher(self, id: int) -> str:
        hashes = self.db.getChildrenHashes(id)
        hash_str = "\n".join(hashes)
        return hashlib.md5(hash_str.encode("ascii")).hexdigest()

    def dir_hash_update(self, id: int) -> None:
        if id != self.db.rootDirID:
            dir_hash = self.dir_hasher(id)
            self.db.updateDirHash(id, dir_hash)
            parent = self.db.getDirParentID(id)
            self.dir_hash_update(parent)

    @staticmethod
    def symlink_handler(path: Path) -> NoReturn:
        raise SymlinkFound(f'Symlink "{path} found in directory, unable to handle it')

    def handlerSwitch(self, type, path) -> None:
        if type == 'dir':
            self.dir_handler(path)
        elif type == 'file':
            self.file_handler(path)
        elif type == 'symlink':
            Scanner.symlink_handler(path)
        else:
            raise UnexpectedPathType

    def scan(self, path: Path) -> None:
        self.db.setRootDir(str(Path(path)))
        for type, p in fs_utlis.dir_dfs(path):
            self.handlerSwitch(type, p)

    def dumpResults(self) -> None:
        self.db.dumpTable("dirs")
        # self.db.dumpTable("files")
        # self.db.dumpTable("duplicates")
        for i in self.db.getDirs():
            print(f"---{i}---")
            if not i[0] == self.db.rootDirID:
                for j in self.db.getFilesInDir(i[0]):
                    print(j)
