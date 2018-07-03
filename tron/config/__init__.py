from pyrsistent import PClass


class ConfigError(Exception):
    """Generic exception class for errors with config validation"""
    pass


class ConfigRecord(PClass):
    @classmethod
    def from_config(kls, config, *_):
        return kls.create(config)
