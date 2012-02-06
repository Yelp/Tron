from __future__ import with_statement

import logging
import os
import shutil
import signal
from subprocess import Popen, PIPE, CalledProcessError
import sys
import tempfile
import time

from testify import *

from tron import cmd


# Used for getting the locations of the executables
_test_folder, _ = os.path.split(__file__)
_repo_root, _ = os.path.split(_test_folder)

log = logging.getLogger(__name__)


def wait_for_sandbox_success(func, start_delay=0.1, stop_at=5.0):
    """Call *func* repeatedly until it stops throwing TronSandboxException.
    Wait increasing amounts from *start_delay* but wait no more than a total
    of *stop_at* seconds
    """
    delay = 0.1
    total_time = 0.0
    last_exception = None
    while total_time < 5.0:
        time.sleep(delay)
        total_time += delay
        try:
            func()
            return
        except TronSandboxException, e:
            delay *= 2
            last_exception = e
    raise last_exception


def make_file_existence_sandbox_exception_thrower(path):
    def func():
        if not os.path.exists(path):
            raise TronSandboxException('File does not exist: %s' % path)
    return func

def wait_for_file_to_exist(path):
    func = make_file_existence_sandbox_exception_thrower(path)
    wait_for_sandbox_success(func)


def handle_output(cmd, (stdout, stderr), returncode):
    """Log process output before it is parsed. Raise exception if exit code
    is nonzero.
    """
    if stdout:
        log.info("%s: %s", cmd, stdout)
    if stderr:
        log.warning("%s: %s", cmd, stderr)
    if returncode != 0:
        raise CalledProcessError(returncode, cmd)


class TronSandboxException(Exception):
    pass


class MockConfigOptions(object):

    def __init__(self, server):
        self.server = server


class TronSandbox(object):

    def __init__(self):
        """Set up a temp directory and store paths to relevant binaries"""
        super(TronSandbox, self).__init__()

        self.tmp_dir = tempfile.mkdtemp(prefix='tron-')
        self.tron_bin = os.path.join(_repo_root, 'bin')
        self.tronctl_bin = os.path.join(self.tron_bin, 'tronctl')
        self.trond_bin = os.path.join(self.tron_bin, 'trond')
        self.tronfig_bin = os.path.join(self.tron_bin, 'tronfig')
        self.tronview_bin = os.path.join(self.tron_bin, 'tronview')

        self.log_file = os.path.join(self.tmp_dir, 'tron.log')
        self.pid_file = os.path.join(self.tmp_dir, 'tron.pid')
        self.config_file = os.path.join(self.tmp_dir, 'tron_config.yaml')

        self.port = 8089
        self.host = 'localhost'

        self.run_time = None

        self.trond_debug_args = ['--working-dir=%s' % self.tmp_dir,
                                 '--log-file=%s' % self.log_file,
                                 '--pid-file=%s' % self.pid_file,
                                 '--port=%d' % self.port,
                                 '--host=%s' % self.host,
                                 '--config=%s' % self.config_file,
                                 '--verbose']

        self.tron_server_address = '%s:%d' % (self.host, self.port)
        self.tron_server_uri = 'http://%s' % self.tron_server_address
        self.tron_server_arg = '--server=%s' % self.tron_server_address

        # mock a config object
        self.config_obj = MockConfigOptions(self.tron_server_uri)
        cmd.save_config(self.config_obj)

        self._last_trond_launch_args = []

    def log_contents(self):
        with open(self.log_file, 'r') as f:
            return f.read()

    def delete(self):
        """Delete the temp directory and its contents"""
        if os.path.exists(self.pid_file):
            self.stop_trond()
        shutil.rmtree(self.tmp_dir)
        self.tmp_dir = None
        self.tron_bin = None
        self.tronctl_bin = None
        self.trond_bin = None
        self.tronfig_bin = None
        self.tronview_bin = None
        self.tron_server_uri = None

    def save_config(self, config_text):
        """Save a tron configuration to tron_config.yaml. Mainly useful for
        setting trond's initial configuration.
        """
        with open(self.config_file, 'w') as f:
            f.write(config_text)
        return config_text

    ### trond control ###

    def start_trond(self, args=None):
        """Start trond"""
        args = args or []
        self._last_trond_launch_args = args
        command = [sys.executable, self.trond_bin] + self.trond_debug_args + args
        p = Popen(command, stdout=PIPE, stderr=PIPE)

        handle_output(command, p.communicate(), p.returncode)

        # make sure trond has actually launched
        wait_for_sandbox_success(self.list_all)

        # (but p.communicate() already waits for the process to exit... -Steve)
        return p.wait()

    def stop_trond(self):
        """Stop trond based on the tron.pid in the temp directory"""
        with open(self.pid_file, 'r') as f:
            os.kill(int(f.read()), signal.SIGKILL)

    def restart_trond(self, args=None):
        """Stop and start trond"""
        if args == None:
            args = self._last_trond_launch_args
        self.stop_tron()
        self.start_tron(args=args)

    ### www API ###

    def _check_call_api(self, uri, data=None):
        cmd.load_config(self.config_obj)
        status, content = cmd.request(self.tron_server_uri, uri, data=data)

        if status != cmd.OK or not content:
            raise TronSandboxException("Error connecting to tron server at %s%s" % (self.tron_server_uri, uri))

        return content

    def upload_config(self, config_text):
        """Upload a tron configuration to the server"""
        return self._check_call_api('/config', {'config': config_text})

    def get_config(self):
        """Get the text of the current configuration"""
        return self._check_call_api('/config')['config']

    def ctl(self, command, arg=None, run_time=None):
        """Call the www API like tronctl does. ``command`` can be one of
        ``(start, cancel, disable, enable, disableall, enableall, fail, succeed)``.
        ``run_time`` should be of the form ``YYYY-MM-DD HH:MM:SS``.
        """
        content = self._check_call_api('/')

        data = {'command': command}

        if run_time is not None:
            data['run_time'] = run_time

        if arg is not None:
            job_to_uri = cmd.make_job_to_uri(content)
            service_to_uri = cmd.make_service_to_uri(content)
            full_uri = cmd.obj_spec_to_uri(arg, job_to_uri, service_to_uri)
        else:
            full_uri = '/jobs'

        self._check_call_api(full_uri, data=data)

    def list_all(self):
        """Call the www API to list jobs and services."""
        return self._check_call_api('/')

    def list_events(self):
        """Call the www API to list all events."""
        return self._check_call_api('/events')

    def list_job(self, job_name):
        """Call the www API to list all runs of one job."""
        return self._check_call_api('/jobs/%s' % job_name)

    def list_job_events(self, job_name):
        """Call the www API to list all events of one job."""
        return self._check_call_api('/jobs/%s/_events' % job_name)

    def list_job_run(self, job_name, run_number):
        """Call the www API to list all actions of one job run."""
        return self._check_call_api('/jobs/%s/%d' % (job_name, run_number))

    def list_job_run_events(self, job_name, run_number):
        """Call the www API to list all actions of one job run."""
        return self._check_call_api('/jobs/%s/%d/_events' % (job_name, run_number))

    def list_action_run(self, job_name, run_number, action_name, num_lines=100):
        """Call the www API to display the results of an action."""
        return self._check_call_api('/jobs/%s/%d/%s?num_lines=%d' % (job_name, run_number, action_name, num_lines))

    def list_service(self, service_name):
        return self._check_call_api('/services/%s' % service_name)

    def list_service_events(self, service_name):
        return self._check_call_api('/services/%s/_events' % service_name)

    ### Basic subprocesses ###

    def tronctl(self, args=None):
        """Call tronctl with args and return ``(stdout, stderr)``"""
        args = args or []
        command = [sys.executable, self.tronctl_bin] + args
        p = Popen(command, stdout=PIPE, stderr=PIPE)
        retval = p.communicate()
        handle_output(command, retval, p.returncode)
        return retval

    def tronview(self, args=None):
        """Call tronview with args and return ``(stdout, stderr)``"""
        args = args or []
        command = [sys.executable, self.tronview_bin] + args
        p = Popen(command, stdout=PIPE, stderr=PIPE)
        retval = p.communicate()
        handle_output(command, retval, p.returncode)
        # TODO: Something with return value
        # return p.wait()
        # (but p.communicate() already waits for the process to exit... -Steve)
        return retval
