import hashlib
from wand.image import Image

def partial_hasher(path, size) -> str:
    with open(path, 'rb') as f:
        if size < 1024:
            chunk = f.read()
        else:
            chunk = f.read(1024)
    return hashlib.md5(chunk).hexdigest()

def image_hasher(path) -> str:
    with Image(filename=path) as img:
        return img.signature

def full_hasher(path, block_size=2**20) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()
