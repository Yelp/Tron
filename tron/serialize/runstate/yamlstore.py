"""Store state in a local YAML file.

WARNING: Using this store is NOT recommended.  It will be far too slow for
anything but the most trivial setups.  It should only be used with a high
buffer size (10+), and a low run_limit (< 10).
"""
import operator
import os
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import NamedTuple
from typing import Tuple

from tron import yaml
from tron.serialize import runstate


class YamlKey(NamedTuple):
    type: str
    iden: str


TYPE_MAPPING: Dict[str, str] = {
    runstate.JOB_STATE: "jobs",
}


class YamlStateStore:
    def __init__(self, filename: str) -> None:
        self.filename: str = filename
        self.buffer: Dict[str, Any] = {}

    def build_key(self, type: str, iden: str) -> YamlKey:
        return YamlKey(TYPE_MAPPING[type], iden)

    def restore(self, keys: List[YamlKey]) -> Dict[YamlKey, Any]:
        if not os.path.exists(self.filename):
            return {}

        with open(self.filename) as fh:
            self.buffer = yaml.load(fh)

        items = (self.buffer.get(key.type, {}).get(key.iden) for key in keys)
        key_item_pairs = zip(keys, items)
        return dict(filter(operator.itemgetter(1), key_item_pairs))

    def save(self, key_value_pairs: Iterable[Tuple[YamlKey, Any]]) -> None:
        for key, state_data in key_value_pairs:
            if state_data is None:
                self._delete_from_buffer(key)
            else:
                self.buffer.setdefault(key.type, {})[key.iden] = state_data
        self._write_buffer()

    def _delete_from_buffer(self, key: YamlKey) -> None:
        data_for_type = self.buffer.get(key.type, {})
        if data_for_type.get(key.iden):
            del data_for_type[key.iden]
        if not data_for_type:  # No remaining data for this type
            del self.buffer[key.type]

    def _write_buffer(self) -> None:
        with open(self.filename, "w") as fh:
            yaml.dump(self.buffer, fh)

    def cleanup(self) -> None:
        pass

    def __repr__(self) -> str:
        return f"YamlStateStore('{self.filename}')"
