import os
from pathlib import Path
from contextlib import contextmanager


@contextmanager
def cd(newdir):
    prevdir = Path.cwd()
    os.chdir(newdir)
    try:
        yield
    finally:
        os.chdir(prevdir)
        