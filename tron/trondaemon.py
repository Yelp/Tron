"""
 Daemonize trond.
"""
import contextlib
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
from tron.kubernetes import KubernetesClusterRepository
from tron.manhole import make_manhole
from tron.utils import chdir
from tron.utils import flock
from tron.utils import signals

log = logging.getLogger(__name__)


def setup_logging(options):
    default = pkg_resources.resource_filename(tron.__name__, "logging.conf")
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

    tron_logger = logging.getLogger("tron")
    twisted_logger = logging.getLogger("twisted")

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


@contextlib.contextmanager
def no_daemon_context(workdir, lockfile=None, signal_map={}):
    with chdir(workdir), flock(lockfile), signals(signal_map):
        yield


class TronDaemon:
    """Daemonize and run the tron daemon."""

    def __init__(self, options):
        self.options = options
        setup_logging(self.options)

        self.mcp = None
        self.lock_file = self.options.lock_file
        self.working_dir = self.options.working_dir
        self.signals = {signal.SIGINT: signal.default_int_handler}
        self.manhole_sock = f"{self.options.working_dir}/manhole.sock"

    def run(self, boot_time):
        with no_daemon_context(self.working_dir, self.lock_file, self.signals):
            signal_map = {
                signal.SIGHUP: self._handle_reconfigure,
                signal.SIGINT: self._handle_shutdown,
                signal.SIGTERM: self._handle_shutdown,
                signal.SIGQUIT: self._handle_shutdown,
                signal.SIGUSR1: self._handle_debug,
            }
            signal.pthread_sigmask(signal.SIG_BLOCK, signal_map.keys())
            log.info("Starting setup processes...")
            self._run_mcp(boot_time=boot_time)
            log.info(
                f"Master Control Program (MCP) setup complete. Time elapsed since Tron started: {time.time() - boot_time}s"
            )
            self._run_www_api()
            log.info(f"Tron API setup complete. Time elapsed since Tron started: {time.time() - boot_time}s")
            self._run_manhole()
            log.info(f"Manhole setup complete. Time elapsed since Tron started: {time.time() - boot_time}s")
            self._run_reactor()
            log.info(
                f"Twisted reactor has started. The Tron API should be up and ready now to receive requests. Time elapsed since Tron started: {time.time() - boot_time}s"
            )
            log.info("Setup complete!")

            while True:
                signum = signal.sigwait(list(signal_map.keys()))
                if signum in signal_map:
                    logging.info(f"Got signal {str(signum)}")
                    signal_map[signum](signum, None)

    def _run_manhole(self):
        # This condition is made with the assumption that no existing daemon
        # is running. If there is one, the following code could potentially
        # cause problems for the other daemon by removing its socket.
        if os.path.exists(self.manhole_sock):
            log.info("Removing orphaned manhole socket")
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

    def _run_mcp(self, boot_time=None):
        # Local import required because of reactor import in mcp
        from tron import mcp

        working_dir = self.options.working_dir
        config_path = self.options.config_path
        self.mcp = mcp.MasterControlProgram(working_dir, config_path, boot_time)

        try:
            self.mcp.initial_setup()
        except Exception as e:
            msg = "Error in configuration %s: %s"
            log.exception(msg % (config_path, e))
            raise

    def _run_reactor(self):
        """Run the twisted reactor."""
        # This is what actually starts the Tron server by starting the Twisted event loop
        threading.Thread(
            target=reactor.run,
            daemon=True,
            kwargs=dict(installSignalHandlers=0),
        ).start()

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
        KubernetesClusterRepository.shutdown()
        raise SystemExit(f"Terminating on signal {str(sig_num)}")

    def _handle_reconfigure(self, _signal_number, _stack_frame):
        log.info("Reconfigure requested by SIGHUP.")
        reactor.callLater(0, self.mcp.reconfigure)

    def _handle_debug(self, _signal_number, _stack_frame):
        ipdb.set_trace()
