import hashlib
import logging
import os
from copy import deepcopy

from tron import yaml
from tron.config import config_parse
from tron.config import ConfigError
from tron.config import schema
from tron.core.jobgraph import JobGraph
from tron.utils import maybe_decode
from tron.utils import maybe_encode

log = logging.getLogger(__name__)


def from_string(content):
    try:
        return yaml.safe_load(content)
    except yaml.yaml.error.YAMLError as e:
        raise ConfigError("Invalid config format: %s" % str(e))


def write(path, content):
    with open(path, "w") as fh:
        yaml.dump(content, fh)


def read(path):
    with open(path) as fh:
        return from_string(fh)


def write_raw(path, content):
    with open(path, "w") as fh:
        fh.write(
            maybe_decode(content)
        )  # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.


def read_raw(path) -> str:
    with open(path) as fh:
        return fh.read()


def hash_digest(content):
    return hashlib.sha1(
        maybe_encode(content)
    ).hexdigest()  # TODO: TRON-2293 maybe_encode is a relic of Python2->Python3 migration. Remove it.


class ManifestFile:
    """Manage the manifest file, which tracks name to filename."""

    MANIFEST_FILENAME = "_manifest.yaml"

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

    def delete(self, name):
        manifest = read(self.filename)
        if name not in manifest:
            msg = "Namespace %s does not exist in manifest, cannot delete."
            log.info(msg % name)
            return

        del manifest[name]
        write(self.filename, manifest)

    def get_file_mapping(self):
        return read(self.filename)

    def get_file_name(self, name):
        return self.get_file_mapping().get(name)

    def __contains__(self, name):
        return name in self.get_file_mapping()


class ConfigManager:
    """Read, load and write configuration."""

    DEFAULT_HASH = hash_digest("")

    def __init__(self, config_path, manifest=None):
        self.config_path = config_path
        self.manifest = manifest or ManifestFile(config_path)
        self.name_mapping = None

    def build_file_path(self, name):
        name = name.replace(".", "_").replace(os.path.sep, "_")
        return os.path.join(self.config_path, "%s.yaml" % name)

    def read_raw_config(self, name=schema.MASTER_NAMESPACE) -> str:
        """Read the config file without converting to yaml."""
        filename = self.manifest.get_file_name(name)
        return read_raw(filename)

    def write_config(self, name: str, content: str) -> None:
        loaded_content = from_string(content)
        self.validate_with_fragment(
            name,
            content=loaded_content,
            # TODO: remove this constraint after tron triggers across clusters are supported.
            should_validate_missing_dependency=False,
        )
        # validate_with_fragment throws if the updated content is invalid - so if we get here
        # we know it's safe to reflect the update in our config store
        self.get_config_name_mapping()[name] = loaded_content

        # ...and then let's also persist the update to disk since memory is temporary, but disk is forever™
        filename = self.get_filename_from_manifest(name)
        write_raw(filename, content)

    def delete_config(self, name: str) -> None:
        filename = self.manifest.get_file_name(name)
        if not filename:
            msg = "Namespace %s does not exist in manifest, cannot delete."
            log.info(msg % name)
            return

        # to avoid needing to reload from disk on every config load - we need to ensure that
        # we also persist config deletions into our cache
        self.get_config_name_mapping().pop(name, None)
        self.manifest.delete(name)
        os.remove(filename)

    def get_filename_from_manifest(self, name):
        def create_filename():
            filename = self.build_file_path(name)
            self.manifest.add(name, filename)
            return filename

        return self.manifest.get_file_name(name) or create_filename()

    def validate_with_fragment(
        self,
        name,
        content,
        should_validate_missing_dependency=True,
    ):
        # NOTE: we deepcopy rather than swap values to keep this a pure function
        # get_config_name_mapping() returns a shared dict, so this would otherwise
        # actually update the mapping - which would be unwanted/need to be rolled-back
        # should validation fail.
        name_mapping = deepcopy(self.get_config_name_mapping())
        name_mapping[name] = content
        try:
            JobGraph(
                config_parse.ConfigContainer.create(name_mapping),
                should_validate_missing_dependency=should_validate_missing_dependency,
            )
        except ValueError as e:
            raise ConfigError(str(e))

    def get_config_name_mapping(self):
        if self.name_mapping is None:
            log.info("Creating config mapping cache...")
            seq = self.manifest.get_file_mapping().items()
            self.name_mapping = {name: read(filename) for name, filename in seq}
        return self.name_mapping

    def load(self):
        """Return the fully constructed configuration."""
        log.info("Loading full config from %s" % self.config_path)
        name_mapping = self.get_config_name_mapping()
        return config_parse.ConfigContainer.create(name_mapping)

    def get_hash(self, name) -> str:
        """Return a hash of the configuration contents for name."""
        if name not in self:
            return self.DEFAULT_HASH
        return hash_digest(self.read_raw_config(name))

    def __contains__(self, name):
        return name in self.manifest

    def get_namespaces(self) -> str:
        return self.manifest.get_file_mapping().keys()


def create_new_config(path, master_content):
    """Create a new configuration directory with master config."""
    os.makedirs(path)
    manager = ConfigManager(path)
    manager.manifest.create()
    filename = manager.get_filename_from_manifest(schema.MASTER_NAMESPACE)
    write_raw(filename, master_content)
