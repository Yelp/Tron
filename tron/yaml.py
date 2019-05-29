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


safe_dump = dump
safe_load = load
safe_load_all = load_all
