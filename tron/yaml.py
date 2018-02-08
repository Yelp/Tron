from __future__ import absolute_import
from __future__ import unicode_literals

import yaml

SafeDumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
Dumper = SafeDumper
if SafeDumper is yaml.SafeDumper:
    print "YAML: slow dumper detected, built without libyaml-dev?"

SafeLoader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
Loader = SafeLoader
if SafeLoader is yaml.SafeLoader:
    print "YAML: slow loader detected, built without libyaml-dev?"


def dump(*args, **kwargs):
    kwargs['Dumper'] = SafeDumper
    return yaml.dump(*args, **kwargs)


def load(*args, **kwargs):
    kwargs['Loader'] = SafeLoader
    return yaml.load(*args, **kwargs)
