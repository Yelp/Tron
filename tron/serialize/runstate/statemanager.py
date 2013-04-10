from contextlib import contextmanager
import logging
import time
import itertools
import tron
from tron.config import schema
from tron.core import job, service
from tron.serialize import runstate
from tron.serialize.runstate.mongostore import MongoStateStore
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.sqlalchemystore import SQLAlchemyStateStore
from tron.serialize.runstate.yamlstore import YamlStateStore
from tron.utils import observer

log = logging.getLogger(__name__)


class VersionMismatchError(ValueError):
    """Raised when the state has a newer version then tron.__version.__."""

class PersistenceStoreError(ValueError):
    """Raised if the store can not be created or fails a read or write."""


class PersistenceManagerFactory(object):
    """Create a PersistentStateManager."""

    @classmethod
    def from_config(cls, persistence_config):
        store_type              = persistence_config.store_type
        name                    = persistence_config.name
        connection_details      = persistence_config.connection_details
        buffer_size             = persistence_config.buffer_size
        store                   = None

        if store_type not in schema.StatePersistenceTypes:
            raise PersistenceStoreError("Unknown store type: %s" % store_type)

        if store_type == schema.StatePersistenceTypes.shelve:
            store = ShelveStateStore(name)

        if store_type == schema.StatePersistenceTypes.sql:
            store = SQLAlchemyStateStore(name, connection_details)

        if store_type == schema.StatePersistenceTypes.mongo:
            store = MongoStateStore(name, connection_details)

        if store_type == schema.StatePersistenceTypes.yaml:
            store = YamlStateStore(name)

        buffer = StateSaveBuffer(buffer_size)
        return PersistentStateManager(store, buffer)


class StateMetadata(object):
    """A data object for saving state metadata. Conforms to the same
    RunState interface as Jobs and Services.
    """
    name                        = 'StateMetadata'
    version                     = tron.__version_info__

    def __init__(self):
        self.state_data         = {
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

        version = metadata['version']
        # Names (and state keys) changed in 0.5.2, requires migration
        # see tools/migration/migrate_state_to_namespace
        if version > cls.version or version < (0, 5, 2):
            msg = "State for version %s, expected %s"
            raise VersionMismatchError(
                msg % (metadata['version'] , cls.version))


class StateSaveBuffer(object):
    """Buffer calls to save, and perform the saves when buffer reaches
    buffer size. This buffer will only store one state_data for each key.
    """

    def __init__(self, buffer_size):
        self.buffer_size        = buffer_size
        self.buffer             = {}
        self.counter            = itertools.cycle(xrange(buffer_size))

    def save(self, key, state_data):
        """Save the state_data indexed by key and return True if the buffer
        is full.
        """
        self.buffer[key] = state_data
        return not self.counter.next()

    def __iter__(self):
        """Return all buffered data and clear the buffer."""
        for key, item in self.buffer.iteritems():
            yield key, item
        self.buffer.clear()


class PersistentStateManager(object):
    """Provides an interface to persist the state of Tron.

    The implementation of persisting and restoring the state from disk is
    handled by a class which supports the StateStore interface:

    class IStateStore(object):

        def build_key(self, type, identifier):
            return <a key>

        def restore(self, keys):
            return <dict of key to states>

        def save(self, key, state_data):
            pass

        def cleanup(self):
            pass

    """

    def __init__(self, persistence_impl, buffer):
        self.enabled            = True
        self._buffer            = buffer
        self._impl              = persistence_impl
        self.metadata_key       = self._impl.build_key(
                                    runstate.MCP_STATE, StateMetadata.name)

    def restore(self, job_names, service_names, skip_validation=False):
        """Return the most recent serialized state."""
        log.debug("Restoring state.")
        if not skip_validation:
            self._restore_metadata()

        return (self._restore_dicts(runstate.JOB_STATE, job_names),
                self._restore_dicts(runstate.SERVICE_STATE, service_names))

    def _restore_metadata(self):
        metadata = self._impl.restore([self.metadata_key])
        StateMetadata.validate_metadata(metadata.get(self.metadata_key))

    def _keys_for_items(self, item_type, names):
        """Returns a dict of item to the key for that item."""
        keys = (self._impl.build_key(item_type, name) for name in names)
        return dict(itertools.izip(keys, names))

    def _restore_dicts(self, item_type, items):
        """Return a dict mapping of the items name to its state data."""
        key_to_item_map  = self._keys_for_items(item_type, items)
        key_to_state_map = self._impl.restore(key_to_item_map.keys())
        return dict((key_to_item_map[key], state_data)
                    for key, state_data in key_to_state_map.iteritems())

    def save(self, type_enum, name, state_data):
        """Persist an items state."""
        key = self._impl.build_key(type_enum, name)
        log.info("Buffering state save for: %s", key)
        if self._buffer.save(key, state_data) and self.enabled:
            self._save_from_buffer()

    def _save_from_buffer(self):
        key_state_pairs = list(self._buffer)
        if not key_state_pairs:
            return

        keys = ','.join(str(key) for key, _ in key_state_pairs)
        log.info("Saving state for %s" % keys)

        with self._timeit():
            try:
                self._impl.save(key_state_pairs)
            except Exception, e:
                msg = "Failed to save state for %s: %s" % (keys, e)
                log.warn(msg)
                raise PersistenceStoreError(msg)

    def cleanup(self):
        self._save_from_buffer()
        self._impl.cleanup()

    @contextmanager
    def _timeit(self):
        """Log the time spent saving the state."""
        start_time = time.time()
        yield
        duration = time.time() - start_time
        log.info("State saved using %s in %0.3fs." % (self._impl, duration))

    @contextmanager
    def disabled(self):
        """Temporarily disable the state manager."""
        self.enabled, prev_enabled = False, self.enabled
        try:
            yield
        finally:
            self.enabled = prev_enabled


class NullStateManager(object):
    enabled = False

    @staticmethod
    def cleanup():
        pass

    @classmethod
    def disabled(cls):
        return cls()

    def __enter__(self):
        return

    def __exit__(self, *args):
        return


class StateChangeWatcher(observer.Observer):
    """Observer of stateful objects."""

    def __init__(self):
        self.state_manager = NullStateManager
        self.config        = None

    def update_from_config(self, state_config):
        if self.config == state_config:
            return False

        self.shutdown()
        self.state_manager = PersistenceManagerFactory.from_config(state_config)
        self.config = state_config
        return True

    def handler(self, observable, _event):
        """Handle a state change in an observable by saving its state."""
        if isinstance(observable, job.Job):
            self.save_job(observable)
        if isinstance(observable, service.Service):
            self.save_service(observable)

    def save_job(self, job):
        self._save_object(runstate.JOB_STATE, job)

    def save_service(self, service):
        self._save_object(runstate.SERVICE_STATE, service)

    def save_metadata(self):
        self._save_object(runstate.MCP_STATE, StateMetadata())

    def _save_object(self, state_type, obj):
        self.state_manager.save(state_type, obj.name, obj.state_data)

    def shutdown(self):
        self.state_manager.enabled = False
        self.state_manager.cleanup()

    def disabled(self):
        return self.state_manager.disabled()

    def restore(self, jobs, services):
        return self.state_manager.restore(jobs, services)
