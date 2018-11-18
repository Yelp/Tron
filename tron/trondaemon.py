"""
 Daemonize trond.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging.config
import os
import signal
import threading
import time

import ipdb
import pkg_resources
from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import log as twisted_log

import tron
from tron.manhole import make_manhole
from tron.mesos import MesosClusterRepository
from tron.utils import flockfile
from tron.utils import signalqueue

log = logging.getLogger(__name__)


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
        self.lockfile = kwargs.pop('lockfile', None)
        self.working_dir = kwargs.pop('working_directory', '.')

        self.signal_map = kwargs.pop('signal_map', {})
        self._set_signal_handlers(self.signal_map)

    def __enter__(self):
        os.chdir(self.working_dir)
        if self.lockfile:
            try:
                self.lockfile.__enter__()
            except OSError:
                error_msg = f"Tron lockfile already locked: {self.lockfile}"
                log.error(error_msg)
                raise SystemExit(f"error: {error_msg}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.info("NoDaemonContext exit")
        if self.lockfile:
            self.lockfile.__exit__(exc_type, exc_val, exc_tb)

    def _set_signal_handlers(self, signal_map):
        """ Sets signal handlers for the current thread using a signal map """
        for signum, handler in signal_map.items():
            signal.signal(signum, handler)

    def terminate(self, signal_number, *_):
        raise SystemExit(f"Terminating on signal {str(signal_number)}")


class TronDaemon(object):
    """Daemonize and run the tron daemon."""

    def __init__(self, options):
        self.options = options
        setup_logging(self.options)
        self.mcp = None

        self._sigint_handler = self._make_sigint_handler(
            signal.getsignal(signal.SIGINT)
        )
        self.sigqueue = signalqueue.SignalQueue()

        self.context = self._build_context(options)
        self.manhole_sock = f"{self.options.working_dir}/manhole.sock"

    def _build_context(self, options):
        return NoDaemonContext(
            lockfile=flockfile.FlockFile(options.lock_file),
            working_directory=options.working_dir,
            signal_map = {sig: self.sigqueue.handler for sig in [
                signal.SIGHUP,
                signal.SIGINT,
                signal.SIGTERM,
                signal.SIGQUIT,
                signal.SIGUSR1,
            ]},
        )

    def run(self):
        with self.context:
            self._run_mcp()
            self._run_www_api()
            self._run_manhole()
            self._run_reactor()

            # signal handlers we use to handle signals synchronously
            signal_map = {
                signal.SIGHUP: self._handle_reconfigure,
                signal.SIGINT: self._sigint_handler,
                signal.SIGTERM: self._handle_shutdown,
                signal.SIGQUIT: self._handle_shutdown,
                signal.SIGUSR1: self._handle_debug,
            }
            # wait for signals to become available then handle them one by one
            while True:
                signum = self.sigqueue.wait()  # does not block
                if signum in signal_map:
                    sig = signal.Signals(signum)  # int to proper Signal
                    log.info(f"Got signal {str(sig)}")
                    signal_map[signum](sig, None)

    def _run_manhole(self):
        # This condition is made with the assumption that no existing daemon
        # is running. If there is one, the following code could potentially
        # cause problems for the other daemon by removing its socket.
        if os.path.exists(self.manhole_sock):
            log.info('Removing orphaned manhole socket')
            os.remove(self.manhole_sock)

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
            kwargs=dict(installSignalHandlers=0),
        ).start()

    def _make_sigint_handler(self, prev_handler=None):
        """ Creates a SIGINT handler that takes into account a previous
        handler to differentiate between a user request to shutdown, versus
        another source we want to prevent from interrupting the reactor.

        :type prev_handler: function
        :param prev_handler: The previous SIGINT handler, set by another source.
                             We use it to verify whether or not a SIGINT we
                             received is a genuine shutdown request.
        """

        def handler(signum, frame):
            try:
                if prev_handler is not None:
                    prev_handler(signum, frame)
            except KeyboardInterrupt:
                # Previous signal handler didn't raise another exception,
                # so must be user requesting shutdown.
                pass
            except Exception as e:
                # We received a SIGINT, but was caused by another thread
                # aborting due to its own error. In this case, we don't want to
                # stop running.
                log.error(f"Non-reactor thread raised: {e}")
                return
            self._handle_shutdown(signum, frame)

        return handler

    def _handle_shutdown(self, sig_num, stack_frame):
        log.info(f"Shutdown requested via {str(sig_num)}")
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
