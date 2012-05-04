"""
 Daemonize trond.
"""
import logging
import os
import daemon

import lockfile
import signal
from twisted.web import server
from twisted.internet import reactor
from tron import mcp

from tron.api import www


log = logging.getLogger(__name__)


class PIDFile(object):
    """Create and check for a PID file for the daemon."""

    def __init__(self, filename):
        self.filename = filename
        self.check_if_pidfile_exists()

    def check_if_pidfile_exists(self):
        self.lock = lockfile.FileLock(self.filename)
        self.lock.acquire(0)

        try:
            with open(self.filename) as fh:
                pid = int(fh.read().strip())
        except IOError:
            pid = None

        if self.is_process_running(pid):
            self._try_unlock()
            raise SystemExit("Daemon running as %s" % pid)

        if pid:
            self._try_unlock()
            raise SystemExit("Daemon was running as %s. Remove PID file." % pid)

    def is_process_running(self, pid):
        """Return True the process is still running."""
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def __enter__(self):
        with open(self.filename, 'w') as fh:
            fh.write('%s\n' % os.getpid())

    def _try_unlock(self):
        try:
            self.lock.release()
        except lockfile.NotLocked:
            log.warn("Lockfile was already unlocked.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._try_unlock()
        try:
            os.unlink(self.filename)
        except OSError:
            log.warn("Failed to remove pidfile: %s" % self.filename)


class NoDaemonContext(object):
    """A mock DaemonContext."""

    def __init__(self, **kwargs):
        self.signal_map     = kwargs.pop('signal_map', {})
        self.pidfile        = kwargs.pop('pidfile', None)

    def __enter__(self):
        for signum, handler in self.signal_map.iteritems():
            signal.signal(signum, handler)
        if self.pidfile:
            self.pidfile.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pidfile:
            self.pidfile.__exit__(exc_type, exc_val, exc_tb)

    def terminate(self, _sig_num, _frame):
        pass

class TronDaemon(object):
    """Daemonize and run the tron daemon."""

    def __init__(self, options):
        self.options    = options
        self.mcp        = None
        nodaemon        = self.options.nodaemon
        context_class   = NoDaemonContext if nodaemon else daemon.DaemonContext
        self.context    = self._build_context(options, context_class)

    def _build_context(self, options, context_class):
        signal_map = {
            signal.SIGHUP:  self._handle_reconfigure,
            signal.SIGINT:  self._handle_shutdown,
            signal.SIGTERM: self._handle_shutdown
        }
        return context_class(
            working_directory=options.working_dir,
            umask=0o022,
            pidfile=PIDFile(options.pid_file),
            signal_map=signal_map
        )

    def run(self):
        with self.context:
            self._run_mcp()
            self._run_www_api()
            self._run_reactor()

    def _run_www_api(self):
        site = server.Site(www.RootResource(self.mcp))
        port = self.options.listen_port
        reactor.listenTCP(port, site, interface=self.options.listen_host)

    def _run_mcp(self):
        working_dir         = self.options.working_dir
        config_file         = self.options.config_file
        self.mcp            = mcp.MasterControlProgram(working_dir, config_file)

        try:
            self.mcp.initial_setup()
        except Exception, e:
            msg = "Error in configuration file %s: %s"
            log.exception(msg % (self.options.config_file, e))
            raise SystemExit("Failed to configure MCP")

    def _run_reactor(self):
        """Run the twisted reactor."""
        reactor.run()

    def _handle_shutdown(self, signal_number, stack_frame):
        log.info("Shutdown requested: sig %s" % signal_number)
        if self.mcp:
            self.mcp.shutdown()
        reactor.stop()
        self.context.terminate(signal_number, stack_frame)

    def _handle_reconfigure(self, _signal_number, _stack_frame):
        log.info("Reconfigure requested by SIGHUP.")
        reactor.callLater(0, self.mcp.reconfigure)
        # TODO: reload config