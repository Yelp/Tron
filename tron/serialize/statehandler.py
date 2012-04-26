import shutil
import time
import logging
import os

import yaml
from twisted.internet import reactor

import tron
from tron import event
from tron.utils import observer, timeutils


log = logging.getLogger(__name__)


class StateFileVersionError(Exception):
    pass


class UnsupportedVersionError(Exception):
    pass


STATE_FILE = 'tron_state.yaml'
STATE_SLEEP_SECS = 1
WRITE_DURATION_WARNING_SECS = 30


class StateHandler(observer.Observer, observer.Observable):
    """StateHandler is responsible for serializing state changes to disk.
    It is an Observer of Jobs and Services and an Observable for an
    EventRecorder.
    """

    EVENT_WRITE_FAILED   = event.EventType(event.LEVEL_CRITICAL, "write_failed")
    EVENT_WRITE_COMPLETE = event.EventType(event.LEVEL_OK, "write_complete")
    EVENT_WRITE_DELAYED  = event.EventType(event.LEVEL_NOTICE, "write_delayed")
    EVENT_RESTORING      = event.EventType(event.LEVEL_NOTICE, "restoring")
    EVENT_STORING        = event.EventType(event.LEVEL_INFO, "storing")

    def __init__(self, mcp, working_dir, writing=False):
        super(StateHandler, self).__init__()
        self.mcp = mcp
        self.working_dir = working_dir
        self.write_pid = None
        self.write_start = None
        self.writing_enabled = writing
        self.store_delayed = False

    def restore_service(self, service, data):
        service.set_context(self.mcp.context)
        service.restore(data)

    def delay_store(self):
        self.store_delayed = False
        self.store_state()

    def check_write_child(self):
        if self.write_pid:
            pid, status = os.waitpid(self.write_pid, os.WNOHANG)
            if pid != 0:
                log.info("State writing completed in in %d seconds",
                    timeutils.current_timestamp() - self.write_start)
                if status != 0:
                    log.warning("State writing process failed with status %d",
                        status)
                    self.notify(self.EVENT_WRITE_FAILED)
                else:
                    self.notify(self.EVENT_WRITE_COMPLETE)
                self.write_pid = None
                self.write_start = None
            else:
                # Process hasn't exited
                write_duration = (timeutils.current_timestamp() -
                                  self.write_start)
                if write_duration > WRITE_DURATION_WARNING_SECS:
                    log.warning("State writing hasn't completed in %d secs",
                        write_duration)
                    self.notify(self.EVENT_WRITE_DELAYED)

                reactor.callLater(STATE_SLEEP_SECS, self.check_write_child)

    def handle_state_changes(self, _observable, _event):
        self.store_state()
    handler = handle_state_changes

    def store_state(self):
        """Stores the state of tron"""
        log.debug("store_state called: %r, %r",
            self.write_pid, self.writing_enabled)

        # If tron is already storing data, don't start again till it's done
        if self.write_pid or not self.writing_enabled:
            # If a child is writing, we don't want to ignore this change, so
            # lets try it later
            if not self.store_delayed:
                self.store_delayed = True
                reactor.callLater(STATE_SLEEP_SECS, self.delay_store)
            return

        tmp_path = os.path.join(self.working_dir, '.tmp.' + STATE_FILE)
        file_path = os.path.join(self.working_dir, STATE_FILE)
        log.info("Storing state in %s", file_path)

        self.notify(self.EVENT_STORING)

        self.write_start = timeutils.current_timestamp()
        pid = os.fork()
        if pid:
            self.write_pid = pid
            reactor.callLater(STATE_SLEEP_SECS, self.check_write_child)
        else:
            exit_status = os.EX_SOFTWARE
            try:
                with open(tmp_path, 'w') as data_file:
                    yaml.dump(self.state_data, data_file,
                        default_flow_style=False, indent=4)
                    data_file.flush()
                    os.fsync(data_file.fileno())
                shutil.move(tmp_path, file_path)
                exit_status = os.EX_OK
            except:
                log.exception("Failure while writing state")
            finally:
                os._exit(exit_status)

    def get_state_file_path(self):
        return os.path.join(self.working_dir, STATE_FILE)

    def load_data(self):
        log.info('Restoring state from %s', self.get_state_file_path())
        self.notify(self.EVENT_RESTORING)

        if not os.path.isfile(self.get_state_file_path()):
            log.info("No state data found")
            return

        with open(self.get_state_file_path()) as data_file:
            return self._load_data_file(data_file)

    def _load_data_file(self, data_file):
        data = yaml.load(data_file)

        if 'version' not in data:
            # Pre-versioned state files need to be reformatted a bit
            data = {
                'version': [0, 1, 9],
                'jobs': data
            }

        # For properly comparing version, we need to convert this guy to a
        # tuple
        data['version'] = tuple(data['version'])

        # By default we assume backwards compatability.
        if data['version'] == tron.__version_info__:
            return data
        elif data['version'] > tron.__version_info__:
            raise StateFileVersionError("State file has new version: %r",
                data['version'])
        elif data['version'] < (0, 2, 0):
            raise UnsupportedVersionError("State file has version %r" %
                                          (data['version'],))
        else:
            # Potential version conversions
            return data

    @property
    def state_data(self):
        data = {
            'version': tron.__version_info__,
            'create_time': int(time.time()),
            'jobs': {},
            'services': {},
            }

        for name, job_sched in self.mcp.jobs.iteritems():
            data['jobs'][name] = job_sched.job.state_data

        for s in self.mcp.services.itervalues():
            data['services'][s.name] = s.state_data

        return data

    def __str__(self):
        return "STATE_HANDLER"