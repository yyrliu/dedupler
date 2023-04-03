import hashlib
from PIL import Image
import imagehash

def partial_hasher(path, size) -> str:
    with open(path, 'rb') as f:
        if size < 1024:
            chunk = f.read()
        else:
            chunk = f.read(1024)
    return hashlib.md5(chunk).hexdigest()

def image_hasher(path) -> str:
    with Image.open(path) as img:
        # TODO: Find best hash function
        # https://github.com/JohannesBuchner/imagehash
        hash = imagehash.average_hash(img)
        return str(hash)

def full_hasher(path, block_size=2**20) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()
