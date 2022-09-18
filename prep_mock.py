from pathlib import Path
import json
import shutil
import requests

from fs_ops import cd


class UnexpactedFileTree(Exception):
    pass

class UnexpactedFileType(Exception):
    pass

def dict_dfs(tree, path=Path()):
    for k, v in tree.items():
        if isinstance(v, dict):
            yield from dict_dfs(v, path.joinpath(k))
            continue

        if k[0] == ".":
            if not isinstance(v, list):
                raise UnexpactedFileTree(f"{k}: {v} in {path} should be a list")
            for item in v:
                yield (path, k[1:], item["name"], item)
            continue

        raise UnexpactedFileTree(f'In "{path}": "{k}": {v} cannot be parsed')

def create_txt(file, options):
    with open(file, 'w') as f:
        f.write(options["content"])

def create_jpg(file, options):
    print(options)
    data = requests.get(options["url"]).content
    with open(file, 'wb') as f:
        f.write(data)

def parse_mock(file):
    with open(file, 'r') as f:
        file_tree = json.load(f)

    return dict_dfs(file_tree)

def switcher(type, *args):
    return {
        "txt": create_txt,
        "jpg": create_jpg
    }[type](*args)

def create_mock_data(file_tree, base_dir="./test/mock_data"):
    base_dir = Path(base_dir)
    remove_mock_data(base_dir)

    base_dir.mkdir()

    files = parse_mock(file_tree)
    with cd(base_dir):
        for path, type, name, options in files:
            if not path.exists():
                path.mkdir()
            try:
                switcher(type, path.joinpath(f"{name}.{type}"), options)
            except KeyError as e:
                raise UnexpactedFileType(f'Unknown file type "{type}" in "{path.joinpath(name)}"') from e
        

def remove_mock_data(base_dir):
    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)

    if base_dir.exists():
        shutil.rmtree(base_dir)
