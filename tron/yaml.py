from __future__ import absolute_import
from __future__ import unicode_literals

import yaml

SafeDumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
Dumper = SafeDumper
if SafeDumper is yaml.SafeDumper:
    print "YAML: slow sumper detected, missing libyaml-dev?"

SafeLoader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
Loader = SafeLoader
if SafeLoader is yaml.SafeLoader:
    print "YAML: slow loader detected, missing libyaml-dev?"


def dump(*args, **kwargs):
    kwargs['Dumper'] = SafeDumper
    return yaml.dump(*args, **kwargs)


def load(*args, **kwargs):
    kwargs['Loader'] = SafeLoader
    return yaml.load(*args, **kwargs)
