import chunk
import os
from pathlib import Path
from contextlib import contextmanager
import hashlib


from fs_ops import cd
from db import Database as DB
import prep_mock


class SymlinkFound(Exception):
    pass

def dir_dfs(path):
    '''Generator for all files and directories in a directory, directory path and comes before the children are iterated and "None" comes after everything.'''

    for p in Path(path).iterdir():
        if p.is_symlink():
            yield ('symlink', p)

        if p.is_file():
            yield ('file', p)
            
        if p.is_dir():
            yield ('dir', p)
            yield from dir_dfs(p)
            yield ('dir', None)

def file_handler(path):
    size = path.stat().st_size

    with open(path, 'rb') as f:
        if size < 1024:
            chunk = f.read()
        else:
            chunk = f.read(1024)

    hash = hashlib.md5(chunk).hexdigest()
    
    db.insertFile(str(path), size, hash) 

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

def file_scanner(path):
    for type, p in dir_dfs(path):
        switcher(type, p)

def main():
    global db
    db = DB(':memory:')
    db.initialize()

    file_scanner("./test/mock_data")

    db.commit()
    db.dumpTable("files")
    db.dumpTable("duplicates")

if __name__ == "__main__":
    prep_mock.create_mock_data("./test/mock_data_file_tree.json", "./test/mock_data")
    # prep_mock.remove_mock_data("./test/mock_data")
    main()

