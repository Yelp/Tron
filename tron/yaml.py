from __future__ import absolute_import
from __future__ import unicode_literals

import yaml


def dump(*args, **kwargs):
    kwargs['Dumper'] = yaml.CSafeDumper
    return yaml.dump(*args, **kwargs)


def load(*args, **kwargs):
    kwargs['Loader'] = yaml.CSafeLoader
    return yaml.load(*args, **kwargs)


def load_all(*args, **kwargs):
    kwargs['Loader'] = yaml.CSafeLoader
    return yaml.load_all(*args, **kwargs)
