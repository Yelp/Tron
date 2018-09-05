"""
 Daemonize trond.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import logging.config
import os
import signal
import threading
import time

import ipdb
import lockfile
import pkg_resources
from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import log as twisted_log

import tron
from tron.manhole import make_manhole
from tron.mesos import MesosClusterRepository
from tron.utils import flockfile

log = logging.getLogger(__name__)


class PIDFile(object):
    """Create and check for a PID file for the daemon."""

    def __init__(self, filename):
        self.lock = flockfile.FlockFile(filename)
        self.check_if_pidfile_exists()

    @property
    def filename(self):
        return self.lock.path

    def check_if_pidfile_exists(self):
        self.lock.acquire()

        try:
            with open(self.filename, 'r') as fh:
                pid = int(fh.read().strip())
        except (IOError, ValueError):
            pid = None

        if self.is_process_running(pid):
            self._try_unlock()
            raise SystemExit(f"Daemon running as {pid}")

        if pid:
            self._try_unlock()
            raise SystemExit(
                f"A tron pidfile is already present at {self.filename} using "
                f"PID {pid}. The existing pidfile must be removed before "
                "starting another tron daemon."
            )

    def is_process_running(self, pid):
        """Return True if the process is still running."""
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def __enter__(self):
        print(os.getpid(), file=self.lock.file)
        self.lock.file.flush()

    def _try_unlock(self):
        try:
            self.lock.release()
        except lockfile.NotLocked:
            log.warning("Lockfile was already unlocked.")

    def __exit__(self, *args):
        self._try_unlock()
        try:
            os.unlink(self.filename)
            log.info(f"Removed pidfile: {self.filename}")
        except OSError:
            log.warning(f"Failed to remove pidfile: {self.filename}")


def setup_logging(options):
    default = pkg_resources.resource_filename(tron.__name__, 'logging.conf')
    logfile = options.log_conf or default

    level = twist_level = None
    if options.verbose > 0:
        level = logging.INFO
        twist_level = logging.WARNING
    if options.verbose > 1:
        level = logging.DEBUG
        twist_level = logging.INFO
    if options.verbose > 2:
        twist_level = logging.DEBUG

    tron_logger = logging.getLogger('tron')
    twisted_logger = logging.getLogger('twisted')

    logging.config.fileConfig(logfile)
    if level is not None:
        tron_logger.setLevel(level)
    if twist_level is not None:
        twisted_logger.setLevel(twist_level)

    # Hookup twisted to standard logging
    twisted_log.PythonLoggingObserver().start()

    # Show stack traces for errors in twisted deferreds.
    if options.debug:
        defer.setDebugging(True)


class NoDaemonContext(object):
    """A mock DaemonContext for running trond without being a daemon."""

    def __init__(self, **kwargs):
        self.pidfile = kwargs.pop('pidfile', None)
        self.working_dir = kwargs.pop('working_directory', '.')

    def __enter__(self):
        os.chdir(self.working_dir)
        if self.pidfile:
            self.pidfile.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.info("NoDaemonContext exit")
        if self.pidfile:
            self.pidfile.__exit__(exc_type, exc_val, exc_tb)

    def terminate(self, signal_number, *_):
        raise SystemExit(f"Terminating on signal {signal_number!r}")


class TronDaemon(object):
    """Daemonize and run the tron daemon."""

    def __init__(self, options):
        self.options = options
        self.mcp = None
        self.context = self._build_context(options)
        self.manhole_sock = f"{self.options.working_dir}/manhole.sock"

    def _build_context(self, options):
        pidfile = PIDFile(options.pid_file)
        return NoDaemonContext(
            working_directory=options.working_dir,
            umask=0o022,
            pidfile=pidfile,
            signal_map={},
            files_preserve=[pidfile.lock.file],
        )

    def run(self):
        with self.context:
            setup_logging(self.options)
            self._run_mcp()
            self._run_www_api()
            self._run_manhole()
            self._run_reactor()

    def _run_manhole(self):
        self.manhole = make_manhole(dict(trond=self, mcp=self.mcp))
        reactor.listenUNIX(self.manhole_sock, self.manhole)
        log.info(f"manhole started on {self.manhole_sock}")

    def _run_www_api(self):
        # Local import required because of reactor import in server and www
        from tron.api import resource
        site = resource.TronSite.create(self.mcp, self.options.web_path)
        port = self.options.listen_port
        reactor.listenTCP(port, site, interface=self.options.listen_host)

    def _run_mcp(self):
        # Local import required because of reactor import in mcp
        from tron import mcp
        working_dir = self.options.working_dir
        config_path = self.options.config_path
        self.mcp = mcp.MasterControlProgram(working_dir, config_path)

        try:
            self.mcp.initial_setup()
        except Exception as e:
            msg = "Error in configuration %s: %s"
            log.exception(msg % (config_path, e))
            raise

    def _run_reactor(self):
        """Run the twisted reactor."""
        threading.Thread(
            target=reactor.run,
            daemon=True,
            kwargs=dict(installSignalHandlers=0)
        ).start()
        signal_map = {
            signal.SIGHUP: self._handle_reconfigure,
            signal.SIGINT: self._handle_shutdown,
            signal.SIGTERM: self._handle_shutdown,
            signal.SIGQUIT: self._handle_shutdown,
            signal.SIGUSR1: self._handle_debug,
        }
        while True:
            signal.pthread_sigmask(signal.SIG_BLOCK, signal_map.keys())
            signum = signal.sigwait(set(signal_map.keys()))
            logging.info("Got signal %s" % signum)
            if signum in signal_map:
                signal_map[signum](signum, None)

    def _handle_shutdown(self, sig_num, stack_frame):
        log.info("Shutdown requested via %s" % sig_num)
        reactor.callLater(0, reactor.stop)
        waited = 0
        while reactor.running:
            if waited > 5:
                log.error("timed out waiting for reactor shutdown")
                break
            time.sleep(0.1)
            waited += 0.1
        if self.mcp:
            self.mcp.shutdown()
        MesosClusterRepository.shutdown()
        self.context.terminate(sig_num, stack_frame)

    def _handle_reconfigure(self, _signal_number, _stack_frame):
        log.info("Reconfigure requested by SIGHUP.")
        reactor.callLater(0, self.mcp.reconfigure)

    def _handle_debug(self, _signal_number, _stack_frame):
        ipdb.set_trace()
