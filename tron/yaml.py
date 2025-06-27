from typing import Any
from typing import Iterator
from typing import Optional

import yaml


def dump(*args: Any, **kwargs: Any) -> Optional[str]:
    kwargs["Dumper"] = yaml.CSafeDumper
    return yaml.dump(*args, **kwargs)


def load(*args: Any, **kwargs: Any) -> Any:
    kwargs["Loader"] = yaml.CSafeLoader
    return yaml.load(*args, **kwargs)


def load_all(*args: Any, **kwargs: Any) -> Iterator[Any]:
    kwargs["Loader"] = yaml.CSafeLoader
    return yaml.load_all(*args, **kwargs)


safe_dump = dump
safe_load = load
safe_load_all = load_all
