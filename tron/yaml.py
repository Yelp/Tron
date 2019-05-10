import yaml

safe_dump = yaml.safe_dump
safe_load = yaml.safe_load
safe_load_all = yaml.safe_load_all


def dump(*args, **kwargs):
    kwargs['Dumper'] = yaml.CDumper
    return yaml.safe_dump(*args, **kwargs)


def load(*args, **kwargs):
    kwargs['Loader'] = yaml.CLoader
    return yaml.safe_load(*args, **kwargs)


def load_all(*args, **kwargs):
    kwargs['Loader'] = yaml.CLoader
    return yaml.safe_load_all(*args, **kwargs)
