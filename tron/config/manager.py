import logging
import os.path
import yaml

from tron.config import schema, config_parse


log = logging.getLogger(__name__)


def write(path, content):
    with open(path, 'w') as fh:
        yaml.dump(content, fh)

def read(path):
    with open(path, 'r') as fh:
        return yaml.load(fh)


class ManifestFile(object):
    """Manage the manifest file, which tracks name to filename."""

    MANIFEST_FILENAME = '_manifest.yaml'

    def __init__(self, path):
        self.filename = os.path.join(path, self.MANIFEST_FILENAME)

    def create(self):
        if os.path.isfile(self.filename):
            msg = "Refusing to create manifest. File %s exists."
            log.info(msg % self.filename)
            return

        write(self.filename, {})

    def add(self, name, filename):
        manifest = read(self.filename)
        manifest[name] = filename
        write(self.filename, manifest)

    def get_file_mapping(self):
        return read(self.filename)

    def get_file_name(self, name):
        return self.get_file_mapping().get(name)

    def __contains__(self, name):
        return name in self.get_file_mapping()


class ConfigManager(object):
    """Read, load and write configuration."""

    def __init__(self, config_path):
        self.config_path = config_path
        self.manifest = ManifestFile(config_path)

    def build_file_path(self, name):
        name = name.replace('.', '_').replace(os.path.sep, '_')
        return os.path.join(self.config_path, name)

    def read_config(self, name=schema.MASTER_NAMESPACE):
        filename = self.manifest.get_file_name(name)
        return read(filename)

    def write_config(self, name, content):
        self.validate_fragment(name, content)
        filename = self.manifest.get_file_name(name)
        if not filename:
            filename = self.build_file_path(name)
            self.manifest.add(name, filename)

        write(filename, content)

    def validate_fragment(self, name, content):
        container = self.load()
        container.add(name, content)
        container.validate()

    def load(self):
        """Return the fully constructed configuration."""
        log.info("Loading full config from %s" % self.config_path)
        seq = self.manifest.get_file_mapping().iteritems()
        name_mapping = dict((name, read(filename)) for name, filename in seq)
        return config_parse.ConfigContainer.create(name_mapping)