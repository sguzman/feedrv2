from __future__ import annotations

import functools
import os
import pathlib
import tomllib
from typing import Any, Dict

# ---------- Config ----------
# Location of project toml files
root_dir: pathlib.Path = pathlib.Path(".")


# Load settings.toml


@functools.cache
def read_toml(path: str | os.PathLike) -> Dict[str, Any]:
    with open(path, "rb") as f:
        return tomllib.load(f)


@functools.cache
def config(root: pathlib.Path = root_dir) -> Dict[str, Any]:
    path = root / "settings.toml"
    if not path.is_file():
        raise FileNotFoundError("Missing settings.toml")
    return read_toml(path)


settings = config(root_dir)
