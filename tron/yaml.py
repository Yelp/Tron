from typing import Any
from typing import Iterator
from typing import Optional
from typing import Union

import yaml


# these are basically the same signatures as in the pyyaml typeshed stubs
def dump(data: Any, stream: Optional[Any] = None, **kwargs: Any) -> Union[str, bytes, None]:
    kwargs["Dumper"] = yaml.CSafeDumper
    return yaml.dump(data, stream, **kwargs)  # type: ignore[no-any-return]  # not quite sure why this isn't picking up the right types


def load(stream: Any, **kwargs: Any) -> Any:
    kwargs["Loader"] = yaml.CSafeLoader
    return yaml.load(stream, **kwargs)


def load_all(stream: Any, **kwargs: Any) -> Iterator[Any]:
    kwargs["Loader"] = yaml.CSafeLoader
    return yaml.load_all(stream, **kwargs)


safe_dump = dump
safe_load = load
safe_load_all = load_all
