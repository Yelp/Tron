from contextlib import contextmanager
import logging
import time
import itertools
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
        #connection_details  = persistence_config.connection_details
        #buffer_size         = persistence_config.buffer_size
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
    RunState interface as Jobs and Services.
    """
    name                    = 'StateMetadata'
    version                 = tron.__version_info__

    def __init__(self):
        self.state_data     = {
            'version':              self.version,
            'create_time':          time.time(),
        }

    @classmethod
    def validate_metadata(cls, metadata):
        """Raises an exception if the metadata version is newer then
        tron.__version__.
        """
        if not metadata:
            return

        metadata = metadata[0]
        if metadata['version'] > cls.version:
            msg = "State for version %s, expected %s"
            raise VersionMismatchError(
                msg % (metadata['version'] , cls.version))


# TODO: buffering
class PersistentStateManager(observer.Observer):
    """Provides an interface to persist the state of Tron."""

    def __init__(self, persistence_impl):
        self.enabled        = True
        self._impl          = persistence_impl
        self.metadata_key   = self._impl.build_key(
                                runstate.MCP_STATE, StateMetadata)

    def restore(self, jobs, services):
        """Return the most recent serialized state."""
        log.debug("Restoring state.")
        metadata = self._impl.restore([self.metadata_key])
        StateMetadata.validate_metadata(metadata)

        return (self._restore_dicts(runstate.JOB_STATE, jobs),
                self._restore_dicts(runstate.SERVICE_STATE, services))

    def _keys_for_items(self, item_type, items):
        """Returns a dict of item to the key for that item."""
        make_key = self._impl.build_key
        keys = (make_key(item_type, item.name) for item in items)
        return dict(itertools.izip(keys, items))

    def _restore_dicts(self, item_type, items):
        """Return a dict mapping of the items name to its state data."""
        key_to_item_map  = self._keys_for_items(item_type, items)
        key_to_state_map = self._impl.restore(key_to_item_map.keys())
        return dict(
                (key_to_item_map[key].name, state_data)
                for key, state_data in key_to_state_map.iteritems())

    def _save(self, type_enum, item):
        """Persist an items state."""
        if not self.enabled:
            return
        key = self._impl.build_key(type_enum, item.name)
        log.debug("Saving state for %s" % (key,))

        with self.timeit():
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
        self._save(runstate.MCP_STATE, StateMetadata())

    def cleanup(self):
        self._impl.cleanup()

    def handler(self, observable, _event):
        """Handle a state change in an observable by saving its state."""
        if isinstance(observable, job.Job):
            self.save_job(observable)
        if isinstance(observable, service.Service):
            self.save_service(observable)

    @contextmanager
    def timeit(self):
        """Log the time spent saving the state."""
        start_time = time.time()
        yield
        duration = time.time() - start_time
        log.info("State saved using %s in %0.2fs." % (self._impl, duration))

    @contextmanager
    def disabled(self):
        """Temporarily disable the state manager."""
        self.enabled = False
        try:
            yield
        finally:
            self.enabled = True
