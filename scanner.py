import hashlib
from wand.image import Image
from pathlib import Path

import fs_utlis
import db as DB


class SymlinkFound(Exception):
    pass

class UnexpactedPathType(Exception):
    pass

def partial_hasher(path, size):
    with open(path, 'rb') as f:
        if size < 1024:
            chunk = f.read()
        else:
            chunk = f.read(1024)
    return hashlib.md5(chunk).hexdigest()

def image_hasher(path):
    with Image(filename=path) as img:
        return img.signature

def full_hasher(path, block_size=2**20):
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()

img_extensions = ['.jpg', '.png', '.gif', '.tiff', '.jpeg']

class Scanner():
    def __init__(self, db_path):
        self.db = DB.Database(db_path)
        self.db.initialize()

    def file_handler(self, path):
        size = path.stat().st_size

        if path.suffix in img_extensions:
            image_hash = image_hasher(path)
            self.db.insertFile(str(path), size, image_hash, image_hash)

        else:
            partial_hash = partial_hasher(path, size)
            try:
                self.db.insertFile(str(path), size, partial_hash)
            # Catch exception if identical partial hash exists
            except DB.PartialHashCollisionException as e:
                # Add complete hash to collided file if not exists
                if not e.has_hash_complete:
                    e_full_hash = full_hasher(e.path)
                    self.db.updateFileCompleteHash(e.id, e_full_hash)
                # Resummit insertion request
                full_hash = full_hasher(path)
                self.db.insertFile(str(path), size, partial_hash, full_hash)

    def dir_handler(self, path, stack=[]):
        '''Use default parameter values as stack for saving directory states'''

        if path is None:
            path = stack.pop()  
            return f'<--- "{path}" ends there --->'

        stack.append(path)
        return f'<-- "{path}" starts from there --->'

    @staticmethod
    def symlink_handler(path):
        raise SymlinkFound(f'Symlink "{path} found in directory, unable to handle it')

    def switcher(self, type, *args):
        if type == 'dir':
            self.dir_handler(*args)
        elif type == 'file':
            self.file_handler(*args)
        elif type == 'symlink':
            Scanner.symlink_handler(*args)
        else:
            raise UnexpactedPathType

    def scan(self, path: Path):
        for type, p in fs_utlis.dir_dfs(path):
            self.switcher(type, p)
        
        self.db.commit()

    def dumpResults(self):
        self.db.dumpTable("files")
        self.db.dumpTable("duplicates")