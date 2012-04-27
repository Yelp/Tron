import logging
import time
import tron
from tron.core import job
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.utils import observer
from tron import service

log = logging.getLogger(__name__)

class VersionMismatchError(ValueError):
    """Raised when the state has a newer version then tron.__version.__."""

class PersistenceStoreError(ValueError):
    """Raised if the store can not be created or fails a read or write."""


class PersistenceManagerFactory(object):
    """Create a PersistentStateManager."""

    @classmethod
    def from_config(cls, persistence_config):
        store_type          = persistence_config.store_type
        name                = persistence_config.name
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

        return PersistentStateManager(store)


class StateMetadata(object):
    """A data object for saving state metadata. Conforms to the same
    RunState interface as Jobs and Services."""
    name = 'StateMetadata'

    def __init__(self, state_data):
        self.state_data = state_data


class PersistentStateManager(observer.Observer, observer.Observable):
    """Provides an interface to persist the state of Tron."""

    # TODO: pause state serialization

    def __init__(self, persistance_impl):
        super(PersistentStateManager, self).__init__()
        self._impl = persistance_impl
        self.metadata_key = self._impl.build_key(
                runstate.MCP_STATE, StateMetadata)
        self.version = tron.__version_info__

    def restore(self, jobs, services):
        """Return the most recent serialized state."""
        self._validate_version()

        job_keys = self._keys_for_items(runstate.JOB_STATE, jobs)
        service_keys = self._keys_for_items(runstate.SERVICE_STATE, services)
        return self._restore_dict(job_keys), self._restore_dict(service_keys)

    def _keys_for_items(self, item_type, items):
        make_key = self._impl.build_key
        return (make_key(item_type, item.name) for item in items)

    def _validate_version(self):
        metadata, = self._impl.restore([self.metadata_key])
        if metadata['version'] > self.version:
            msg = "State for version %s, expected %s"
            raise VersionMismatchError(
                msg % (metadata['version'] , self.version))

    def _restore_dict(self, keys):
        items = self._impl.restore(keys)
        return dict((item.name, item) for item in items)

    def _save(self, type_enum, item):
        key = self._impl.build_key(type_enum, item.name)
        try:
            self._impl.save(key, item.state_data)
        except Exception, e:
            msg = "Failed to save state for %s: %s" % (key, e)
            log.warn(msg)
            raise PersistenceStoreError(msg)

    def save_job(self, job):
        self._save(runstate.JOB_STATE, job)

    def save_service(self, service):
        self._save(runstate.SERVICE_STATE, service)

    def save_metadata(self):
        state_data = StateMetadata({
            'version':              self.version,
            'create_time':          time.time(),
        })
        self._save(runstate.MCP_STATE, state_data)

    def cleanup(self):
        self._impl.cleanup()

    def handler(self, observable, _event):
        if isinstance(observable, job.Job):
            self.save_job(observable)
        if isinstance(observable, service.Service):
            self.save_service(observable)
