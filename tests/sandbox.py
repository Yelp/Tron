import logging
import os
import shutil
import signal
import socket
from subprocess import Popen, PIPE, CalledProcessError
import sys
import tempfile
import time
import contextlib
import functools

from testify import TestCase, setup, teardown, turtle

from tron.commands import client


# Used for getting the locations of the executable
_test_folder, _ = os.path.split(__file__)
_repo_root, _   = os.path.split(_test_folder)

log = logging.getLogger(__name__)


def wait_on_sandbox(func, delay=0.1, max_wait=5.0):
    """Poll for func() to return True. Sleeps `delay` seconds between polls
    up to a max of `max_wait` seconds.
    """
    start_time = time.time()
    while time.time() - start_time < max_wait:
        time.sleep(delay)
        if func():
            return
    raise TronSandboxException("Failed %s" % func.__name__)


def handle_output(cmd, (stdout, stderr), returncode):
    """Log process output before it is parsed. Raise exception if exit code
    is nonzero.
    """
    if stdout:
        log.info("%s: %s", cmd, stdout)
    if stderr:
        log.warning("%s: %s", cmd, stderr)
    if returncode:
        raise CalledProcessError(returncode, cmd)


def find_unused_port():
    """Return a port number that is not in use."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with contextlib.closing(sock) as sock:
        sock.bind(('localhost', 0))
        _, port = sock.getsockname()
    return port


class TronSandboxException(Exception):
    pass


class SandboxTestCase(TestCase):

    _suites = ['sandbox']

    @setup
    def make_sandbox(self):
        self.sandbox = TronSandbox()

    @teardown
    def delete_sandbox(self):
        self.sandbox.delete()
        self.sandbox = None


class ClientProxy(object):
    """Wrap calls to client and raise a TronSandboxException on connection
    failures.
    """

    def __init__(self, client, log_filename):
        self.client         = client
        self.log_filename   = log_filename

    def log_contents(self):
        """Return the contents of the log file."""
        with open(self.log_filename, 'r') as f:
            return f.read()

    def wrap(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (client.RequestError, ValueError), e:
            # ValueError for JSONDecode errors
            log.warn("%r, Log:\n%s" % (e, self.log_contents()))
            return False

    def __getattr__(self, name):
        attr = getattr(self.client, name)
        if not callable(attr):
            return attr

        return functools.partial(self.wrap, attr)


class TronSandbox(object):
    """A sandbox for running trond and tron commands in subprocesses."""

    def __init__(self):
        """Set up a temp directory and store paths to relevant binaries"""
        self.verify_environment()
        self.tmp_dir        = tempfile.mkdtemp(prefix='tron-')
        cmd_path_func       = functools.partial(os.path.join, _repo_root, 'bin')
        cmds                = 'tronctl', 'trond', 'tronfig', 'tronview'
        self.commands       = dict((cmd, cmd_path_func(cmd)) for cmd in cmds)
        self.log_file       = self.abs_path('tron.log')
        self.log_conf       = self.abs_path('logging.conf')
        self.pid_file       = self.abs_path('tron.pid')
        self.config_file    = self.abs_path('tron_config.yaml')
        self.port           = find_unused_port()
        self.host           = 'localhost'
        self.api_uri        = 'http://%s:%s' % (self.host, self.port)
        client_config       = turtle.Turtle(server=self.api_uri,
                                warn=False, num_displays=100)
        cclient             = client.Client(client_config)
        self.client         = ClientProxy(cclient, self.log_file)
        self.setup_logging_conf()

    def abs_path(self, filename):
        """Return the absolute path for a file in the sandbox."""
        return os.path.join(self.tmp_dir, filename)

    def setup_logging_conf(self):
        config_template = os.path.join(_repo_root, 'tests/data/logging.conf')
        with open(config_template, 'r') as fh:
            config = fh.read()

        with open(self.log_conf, 'w') as fh:
            fh.write(config.format(self.log_file))

    def verify_environment(self):
        ssh_sock = 'SSH_AUTH_SOCK'
        msg = "Missing $%s in test environment."
        if not os.environ.get(ssh_sock):
            raise TronSandboxException(msg % ssh_sock)

        path = 'PYTHONPATH'
        if not os.environ.get(path):
            raise TronSandboxException(msg % path)

    def delete(self):
        """Delete the temp directory and shutdown trond."""
        if os.path.exists(self.pid_file):
            with open(self.pid_file, 'r') as f:
                os.kill(int(f.read()), signal.SIGKILL)
        shutil.rmtree(self.tmp_dir)

    def save_config(self, config_text):
        """Save the initial tron configuration."""
        with open(self.config_file, 'w') as f:
            f.write(config_text)

    def run_command(self, command_name, args=None, stdin_lines=None):
        """Run the command by name and return (stdout, stderr)."""
        args        = args or []
        command     = [sys.executable, self.commands[command_name]] + args
        stdin       = PIPE if stdin_lines else None
        proc        = Popen(command, stdout=PIPE, stderr=PIPE, stdin=stdin)
        streams     = proc.communicate(stdin_lines)
        handle_output(command, streams, proc.returncode)
        return streams

    def tronctl(self, args=None):
        args = list(args) if args else []
        return self.run_command('tronctl', args + ['--server', self.api_uri])

    def tronview(self, args=None):
        args = list(args) if args else []
        args += ['--nocolor', '--server', self.api_uri]
        return self.run_command('tronview', args)

    def trond(self, args=None):
        args = list(args) if args else []
        args += ['--working-dir=%s' % self.tmp_dir,
                   '--pid-file=%s'  % self.pid_file,
                   '--port=%d'      % self.port,
                   '--host=%s'      % self.host,
                   '--config=%s'    % self.config_file,
                   '--log-conf=%s'  % self.log_conf]

        self.run_command('trond', args)
        wait_on_sandbox(lambda: bool(self.client.home()))

    def tronfig(self, config_content):
        args = ['--server', self.api_uri, '-']
        return self.run_command('tronfig', args, stdin_lines=config_content)
