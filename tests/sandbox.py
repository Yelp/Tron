from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import functools
import logging
import os
import shutil
import signal
import socket
import sys
import tempfile
import time
from subprocess import CalledProcessError
from subprocess import PIPE
from subprocess import Popen

import mock

from testifycompat import assert_not_equal
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tron.commands import client
from tron.config import manager
from tron.config import schema

# Used for getting the locations of the executable
test_dir, _ = os.path.split(__file__)
repo_root, _ = os.path.split(test_dir)

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


def wait_on_state(client_func, url, state, field='state'):
    """Use client_func(url) to wait until the resource changes to state."""

    def wait_func():
        return client_func(url)[field] == state

    wait_func.__name__ = '%s wait on %s' % (url, state)
    wait_on_sandbox(wait_func)


def wait_on_proc_terminate(pid):
    def wait_on_terminate():
        try:
            os.kill(pid, 0)
        except Exception:
            return True

    wait_on_terminate.__name__ = "Wait on %s to terminate" % pid
    wait_on_sandbox(wait_on_terminate)


def build_waiter_func(client_func, url):
    return functools.partial(wait_on_state, client_func, url)


def handle_output(cmd, out_err, returncode):
    """Log process output before it is parsed. Raise exception if exit code
    is nonzero.
    """
    stdout, stderr = out_err
    cmd = ' '.join(cmd)
    if stdout:
        log.warn("%s STDOUT: %s", cmd, stdout)
    if stderr:
        log.warn("%s STDERR: %s", cmd, stderr)
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

    sandbox = None

    @setup
    def make_sandbox(self):
        verify_environment()
        self.sandbox = TronSandbox()
        self.client = self.sandbox.client

    @teardown
    def delete_sandbox(self):
        if self.sandbox:
            self.sandbox.delete()
            self.sandbox = None

    def start_with_config(self, config):
        self.sandbox.save_config(config)
        self.sandbox.trond()

    def restart_trond(self):
        old_pid = self.sandbox.get_trond_pid()
        self.sandbox.shutdown_trond()
        wait_on_proc_terminate(self.sandbox.get_trond_pid())

        self.sandbox.trond()
        assert_not_equal(old_pid, self.sandbox.get_trond_pid())


class ClientProxy(object):
    """Wrap calls to client and raise a TronSandboxException on connection
    failures.
    """

    def __init__(self, client, log_filename):
        self.client = client
        self.log_filename = log_filename

    def log_contents(self):
        """Return the contents of the log file."""
        with open(self.log_filename, 'r') as f:
            return f.read()

    def wrap(self, func, *args, **kwargs):
        with mock.patch('tron.commands.client.log', autospec=True):
            try:
                return func(*args, **kwargs)
            except (client.RequestError, ValueError) as e:
                # ValueError for JSONDecode errors
                log_contents = self.log_contents()
                if log_contents:
                    log.warn("%r, Log:\n%s" % (e, log_contents))
                return False

    def __getattr__(self, name):
        attr = getattr(self.client, name)
        if not callable(attr):
            return attr

        return functools.partial(self.wrap, attr)


def verify_environment():
    for env_var in ['SSH_AUTH_SOCK', 'PYTHONPATH']:
        if not os.environ.get(env_var):
            raise TronSandboxException(
                "Missing $%s in test environment." % env_var,
            )


class TronSandbox(object):
    """A sandbox for running trond and tron commands in subprocesses."""

    def __init__(self):
        """Set up a temp directory and store paths to relevant binaries"""
        self.tmp_dir = tempfile.mkdtemp(prefix='tron-')
        cmd_path_func = functools.partial(os.path.join, repo_root, 'bin')
        cmds = 'tronctl', 'trond', 'tronfig', 'tronview'
        self.commands = {cmd: cmd_path_func(cmd) for cmd in cmds}
        self.log_file = self.abs_path('tron.log')
        self.log_conf = self.abs_path('logging.conf')
        self.pid_file = self.abs_path('tron.pid')
        self.config_path = self.abs_path('configs/')
        self.port = find_unused_port()
        self.host = 'localhost'
        self.api_uri = 'http://%s:%s' % (self.host, self.port)
        cclient = client.Client(self.api_uri)
        self.client = ClientProxy(cclient, self.log_file)
        self.setup_logging_conf()

    def abs_path(self, filename):
        """Return the absolute path for a file in the sandbox."""
        return os.path.join(self.tmp_dir, filename)

    def setup_logging_conf(self):
        config_template = os.path.join(repo_root, 'tests/data/logging.conf')
        with open(config_template, 'r') as fh:
            config = fh.read()

        with open(self.log_conf, 'w') as fh:
            fh.write(config.format(self.log_file))

    def delete(self):
        """Delete the temp directory and shutdown trond."""
        self.shutdown_trond(sig_num=signal.SIGKILL)
        shutil.rmtree(self.tmp_dir)

    def save_config(self, config_text):
        """Save the initial tron configuration."""
        manager.create_new_config(self.config_path, config_text)

    def run_command(self, command_name, args=None, stdin_lines=None):
        """Run the command by name and return (stdout, stderr)."""
        args = args or []
        command = [sys.executable, self.commands[command_name]] + args
        stdin = PIPE if stdin_lines else None
        proc = Popen(command, stdout=PIPE, stderr=PIPE, stdin=stdin)
        streams = proc.communicate(stdin_lines)
        try:
            handle_output(command, streams, proc.returncode)
        except CalledProcessError:
            log.warn(self.client.log_contents())
            raise
        return streams

    def tronctl(self, *args):
        args = list(args) if args else []
        return self.run_command('tronctl', args + ['--server', self.api_uri])

    def tronview(self, *args):
        args = list(args) if args else []
        args += ['--nocolor', '--server', self.api_uri]
        return self.run_command('tronview', args)

    def trond(self, *args):
        args = list(args) if args else []
        args += [
            '--working-dir=%s' % self.tmp_dir,
            '--pid-file=%s' % self.pid_file,
            '--port=%d' % self.port,
            '--host=%s' % self.host,
            '--config-path=%s' % self.config_path,
            '--log-conf=%s' % self.log_conf,
        ]

        self.run_command('trond', args)
        wait_on_sandbox(lambda: bool(self.client.home()))

    def tronfig(
        self,
        config_content=None,
        name=schema.MASTER_NAMESPACE,
    ):
        args = ['--server', self.api_uri, name]
        args += ['-'] if config_content else ['-p']
        return self.run_command('tronfig', args, stdin_lines=config_content)

    def get_trond_pid(self):
        if not os.path.exists(self.pid_file):
            return None
        with open(self.pid_file, 'r') as f:
            return int(f.read())

    def shutdown_trond(self, sig_num=signal.SIGTERM):
        trond_pid = self.get_trond_pid()
        if trond_pid:
            os.kill(trond_pid, sig_num)
