import logging
import time
import tron
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore

log = logging.getLogger(__name__)

class VersionMismatchError(ValueError):
    """Raised when the state has a newer version then tron.__version.__."""

class PersistenceStoreError(ValueError):
    """Raised if the store can not be created or fails a read or write."""


class PersistenceManagerFactory(object):
    """Create a PersistentStateManager."""

    @classmethod
    def from_config(cls, persistence_config):
        store_type          =  persistence_config.store_type
        name                = persistence_config.store_name
        connection_details  = persistence_config.connection_details
        buffer_size         = persistence_config.buffer_size
        store               = None

        if store_type == 'shelve':
            store = ShelveStateStore(name)

#        if store_type == 'sqlalchemy':
#            store = SQLAlchemyStore(name, connection_details)

#        if store_type == 'yaml':
#           store = YamlStateStore(name, buffer_size)

        if not store:
            raise PersistenceStoreError("Unknown store type: %s" % store_type)

        missing_imports = store.check_missing_imports()
        if missing_imports:
            raise PersistenceStoreError("Missing modules %s" % (
                    ','.join(missing_imports)))

        return PersistentStateManager(store)

class PersistentStateManager(object):
    """Provides an interface to persist the state of Tron."""

    MCP_IDEN = 'theoneandonly'

    def __init__(self, persistance_impl):
        self._impl = persistance_impl
        self.metadata_key = self._impl.build_key(
                runstate.MCP_STATE, self.MCP_IDEN)
        self.version = tron.__version__

    def restore(self, jobs, services):
        """Return the most recent serialized state."""
        self._validate_version()

        make_key = self._impl.build_key
        job_keys = (make_key(runstate.JOB_STATE, job.name) for job in jobs)
        service_keys = (make_key(runstate.SERVICE_STATE, service.name)
                for service in services)
        return self._restore_dict(job_keys), self._restore_dict(service_keys)

    def _validate_version(self):
        metadata, = self._impl.restore([self.metadata_key])
        if metadata['version'] > self.version:
            raise VersionMismatchError(
                "State for version %s, expected %s" % (
                    '.'.join(metadata['version']) ,
                    '.'.join(self.version)))

    def _restore_dict(self, keys):
        items = self._impl.restore(keys)
        return dict((item.name, item) for item in items)

    def save_job(self, job):
        key = self._impl.build_key(runstate.JOB_STATE, job.name)
        self._impl.save(key, job.state_data)

    def save_service(self, service):
        key = self._impl.build_key(runstate.SERVICE_STATE, service.name)
        self._impl.save(key, service.state_data)

    def save_metadata(self):
        state_data = {
            'version':              self.version,
            'create_time':          time.time(),
        }
        key = self._impl.build_key(runstate.MCP_STATE, self.MCP_IDEN)
        self._impl.save(key, state_data)

    def cleanup(self):
        self._impl.cleanup()
