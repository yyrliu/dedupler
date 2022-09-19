import chunk
import os
from pathlib import Path
from contextlib import contextmanager
import hashlib
from wand.image import Image

import fs_utlis
import db as DB


__db__ = None

class SymlinkFound(Exception):
    pass

def partial_hasher(path, size):
    with open(path, 'rb') as f:
        if size < 1024:
            chunk = f.read()
        else:
            chunk = f.read(1024)
    return hashlib.md5(chunk).hexdigest()

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

def file_handler(path):
    size = path.stat().st_size

    if path.suffix in img_extensions:
        with Image(filename=path) as img:
            sha256 = img.signature
            __db__.insertFile(str(path), size, sha256, sha256)

    else:
        partial_hash = partial_hasher(path, size)
        try:
            __db__.insertFile(str(path), size, partial_hash)
        # Catch exception if identical partial hash exists
        except DB.PartialHashCollisionException as e:
            # Add complete hash to collided file if not exists
            if not e.has_md5_complete:
                e_full_hash = full_hasher(e.path)
                __db__.updateFileCompleteHash(e.id, e_full_hash)
            # Resummit insertion request
            full_hash = full_hasher(path)
            __db__.insertFile(str(path), size, partial_hash, full_hash)

def dir_handler(path, stack=[]):
    '''Use default parameter values as stack for saving directory states'''

    if path is None:
        path = stack.pop()  
        return f'<--- "{path}" ends there --->'

    stack.append(path)
    return f'<-- "{path}" starts from there --->'

def symlink_handler(path):
    raise SymlinkFound(f'Symlink "{path} found in directory, unable to handle it')

def switcher(type, *args):
    return {
        'file': file_handler,
        'dir': dir_handler,
        'symlink': symlink_handler,
    }[type](*args)

def scan(path: Path, db: DB.Database):
    global __db__
    if __db__ is None:
        __db__ = db
    for type, p in fs_utlis.dir_dfs(path):
        switcher(type, p)
        __db__.commit()