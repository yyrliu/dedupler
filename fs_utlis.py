import os
from pathlib import Path
from contextlib import contextmanager
from collections.abc import Generator


@contextmanager
def cd(newdir):
    prevdir = Path.cwd()
    os.chdir(newdir)
    try:
        yield
    finally:
        os.chdir(prevdir)

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
            yield ('dir', None)
