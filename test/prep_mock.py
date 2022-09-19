from pathlib import Path
import json
import shutil
import requests
import asyncio


import fs_utlis

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

def create_jpg_async(file, options, tasks=[], loop=None):
    """Tasks are stored in tasks[] and executed asynchronously when file parameter is None. """

    # Create event loop at first run
    if not loop:
        loop = asyncio.get_event_loop()

    # Execute event loop when file parameter is None
    if not file:
        loop.run_until_complete(asyncio.wait(tasks))
        return
    
    async def task(file, options):
        print(options)
        res = await loop.run_in_executor(None, requests.get, options["url"])
        with open(file, 'wb') as f:
            f.write(res.content)

    # Normal behavior, append task to tasks[] 
    tasks.append(loop.create_task(task(file, options)))

def parse_mock(file):
    with open(file, 'r') as f:
        file_tree = json.load(f)

    return dict_dfs(file_tree)

def switcher(type, *args):
    return {
        "txt": create_txt,
        "jpg": create_jpg_async
    }[type](*args)

def create_mock_data(file_tree, base_dir="./test/mock_data"):
    base_dir = Path(base_dir)
    remove_mock_data(base_dir)

    base_dir.mkdir()

    files = parse_mock(file_tree)
    with fs_utlis.cd(base_dir):
        for path, type, name, options in files:
            if not path.exists():
                path.mkdir()
            try:
                switcher(type, path.joinpath(f"{name}.{type}"), options)
            except KeyError as e:
                raise UnexpactedFileType(f'Unknown file type "{type}" in "{path.joinpath(name)}"') from e

        # TODO: find a more elegant way to start event loop execution
        create_jpg_async(None, None)
        

def remove_mock_data(base_dir):
    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)

    if base_dir.exists():
        shutil.rmtree(base_dir)
