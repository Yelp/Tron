import concurrent.futures
import copy
import itertools
import logging
import sys
import time
from contextlib import contextmanager
from typing import Any
from typing import cast
from typing import Dict
from typing import List

from tron.config import schema
from tron.core import job
from tron.core import jobrun
from tron.mesos import MesosClusterRepository
from tron.serialize import runstate
from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.yamlstore import YamlStateStore
from tron.utils import observer

log = logging.getLogger(__name__)


class VersionMismatchError(ValueError):
    """Raised when the state has a newer version then tron.__version.__."""


class PersistenceStoreError(ValueError):
    """Raised if the store can not be created or fails a read or write."""


class PersistenceManagerFactory:
    """Create a PersistentStateManager."""

    @classmethod
    def from_config(cls, persistence_config):
        store_type = schema.StatePersistenceTypes(persistence_config.store_type)
        name = persistence_config.name
        buffer_size = persistence_config.buffer_size
        store = None

        if store_type == schema.StatePersistenceTypes.shelve:
            store = ShelveStateStore(name)

        if store_type == schema.StatePersistenceTypes.yaml:
            store = YamlStateStore(name)

        if store_type == schema.StatePersistenceTypes.dynamodb:
            table_name = persistence_config.table_name
            dynamodb_region = persistence_config.dynamodb_region
            max_transact_write_items = persistence_config.max_transact_write_items
            store = DynamoDBStateStore(table_name, dynamodb_region, max_transact_write_items=max_transact_write_items)

        buffer = StateSaveBuffer(buffer_size)
        return PersistentStateManager(store, buffer)


class StateSaveBuffer:
    """Buffer calls to save, and perform the saves when buffer reaches
    buffer size. This buffer will only store one state_data for each key.
    """

    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.buffer = {}
        self.counter = itertools.cycle(range(buffer_size))

    def save(self, key, state_data):
        """Save the state_data indexed by key and return True if the buffer
        is full.
        """
        self.buffer[key] = state_data
        return not next(self.counter)

    def __iter__(self):
        """Return all buffered data and clear the buffer."""
        yield from self.buffer.items()
        self.buffer.clear()


class PersistentStateManager:
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
        self.enabled = True
        self._buffer = buffer
        self._impl = persistence_impl

    # TODO: get rid of the Any here - hopefully with a TypedDict
    def restore(self, job_names: List[str], read_json: bool = False) -> Dict[str, Any]:
        """Return the most recent serialized state."""
        log.debug("Restoring state.")

        # First, restore the jobs themselves
        jobs = self._restore_dicts(runstate.JOB_STATE, job_names, read_json)
        # jobs should be a dictionary that contains  job name and number of runs
        # {'MASTER.k8s': {'run_nums':[0], 'enabled': True}, 'MASTER.cits_test_frequent_1': {'run_nums': [1,0], 'enabled': True}}

        # Second, restore the runs for each of the jobs restored above
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # start the threads and mark each future with it's job name
            # this is useful so that we can index the job name later to add the runs to the jobs dictionary
            results = {
                executor.submit(self._restore_runs_for_job, job_name, job_state, read_json): job_name
                for job_name, job_state in jobs.items()
            }
            for result in concurrent.futures.as_completed(results):
                try:
                    jobs[results[result]]["runs"] = result.result()
                except Exception:
                    log.exception(f"Unable to restore state for {results[result]} - exiting to avoid corrupting data.")
                    sys.exit(1)

        state = {
            runstate.JOB_STATE: jobs,
        }
        return state

    # TODO: get rid of the Any here - hopefully with a TypedDict
    def _restore_runs_for_job(
        self, job_name: str, job_state: Dict[str, Any], read_json: bool = False
    ) -> List[Dict[str, Any]]:
        """Restore the state for the runs of each job"""
        run_nums = job_state["run_nums"]
        keys = [jobrun.get_job_run_id(job_name, run_num) for run_num in run_nums]
        job_runs_restored_states = self._restore_dicts(runstate.JOB_RUN_STATE, keys, read_json)
        all_job_runs = copy.copy(job_runs_restored_states)
        for run_id, state in all_job_runs.items():
            if state == {}:
                log.error(f"Failed to restore {run_id}, no state found for it!")
                job_runs_restored_states.pop(run_id)

        runs = list(job_runs_restored_states.values())
        # We need to sort below otherwise the runs will not be in order
        runs.sort(key=lambda x: x["run_num"], reverse=True)
        return runs

    def _keys_for_items(self, item_type, names):
        """Returns a dict of item to the key for that item."""
        keys = (self._impl.build_key(item_type, name) for name in names)
        return dict(zip(keys, names))

    # TODO: get rid of the Any here - hopefully with a TypedDict
    def _restore_dicts(self, item_type: str, items: List[str], read_json: bool = False) -> Dict[str, Any]:
        """Return a dict mapping of the items name to its state data."""
        key_to_item_map = self._keys_for_items(item_type, items)
        key_to_state_map = self._impl.restore(key_to_item_map.keys(), read_json)
        return {key_to_item_map[key]: state_data for key, state_data in key_to_state_map.items()}

    def delete(self, type_enum, name):
        # A hack to use the save buffer, implementations of save
        # need to delete if data is None.
        self.save(type_enum, name, None)

    def save(self, type_enum, name, state_data):
        """Persist an items state."""
        key = self._impl.build_key(type_enum, name)
        log.debug("Buffering state save for: %s", key)
        if self._buffer.save(key, state_data):
            if not self.enabled:
                log.debug(f"State manager disabled, not persisting {key}")
                return
            self._save_from_buffer()

    def _save_from_buffer(self):
        key_state_pairs = list(self._buffer)
        if not key_state_pairs:
            return

        with self._timeit():
            try:
                self._impl.save(key_state_pairs)
            except Exception as e:
                msg = f"Error while saving: {repr(e)}"
                log.warning(msg)
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
        log.info(f"State saved using {self._impl} in {duration:0.3f}s.")

    @contextmanager
    def disabled(self):
        """Temporarily disable the state manager."""
        self.enabled, prev_enabled = False, self.enabled
        try:
            yield
        finally:
            self.enabled = prev_enabled


class NullStateManager:
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
        self.config = None

    def update_from_config(self, state_config):
        if self.config == state_config:
            return False

        self.shutdown()
        # NOTE: this will spin up a thread that will constantly persist data into dynamodb
        self.state_manager = PersistenceManagerFactory.from_config(
            state_config,
        )
        self.config = state_config
        return True

    def handler(self, observable, event, event_data=None):
        """Handle a state change in an observable by saving its state."""
        if observable == MesosClusterRepository:
            self.save_frameworks(observable)
        elif isinstance(observable, job.Job):
            if event == job.Job.NOTIFY_NEW_RUN:
                if event_data is None or not isinstance(event_data, jobrun.JobRun):
                    log.warning(f"Notified of new run, but no run to watch. Got {event_data}")
                else:
                    log.debug(f"Watching new run {event_data}")
                    self.watch(event_data)
            else:
                self.save_job(observable)
        elif isinstance(observable, jobrun.JobRun):
            if event == jobrun.JobRun.NOTIFY_REMOVED:
                self.delete_job_run(observable)
            else:
                self.save_job_run(observable)

    def save_job(self, job):
        self._save_object(runstate.JOB_STATE, job)

    def save_job_run(self, job_run):
        self._save_object(runstate.JOB_RUN_STATE, job_run)

    def delete_job_run(self, job_run):
        # HACK: this cast is nasty, but we should probably refactor things so that the default self.state_manager
        # in not a NullStateManager
        cast(PersistentStateManager, self.state_manager).delete(runstate.JOB_RUN_STATE, job_run.name)

    def save_frameworks(self, clusters):
        self._save_object(runstate.MESOS_STATE, clusters)

    def _save_object(self, state_type, obj):
        # HACK: this cast is nasty, but we should probably refactor things so that the default self.state_manager
        # in not a NullStateManager
        cast(PersistentStateManager, self.state_manager).save(state_type, obj.name, obj.state_data)

    def shutdown(self):
        self.state_manager.enabled = False
        self.state_manager.cleanup()

    def disabled(self):
        return self.state_manager.disabled()

    def restore(self, jobs: List[str], read_json: bool = False) -> Dict[str, Any]:
        # HACK: this cast is nasty, but we should probably refactor things so that the default self.state_manager
        # in not a NullStateManager
        return cast(PersistentStateManager, self.state_manager).restore(jobs, read_json)
