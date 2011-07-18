import os
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
from testify import *
import time

from tron import cmd
from tron.utils.binutils import make_job_to_uri, make_service_to_uri, obj_spec_to_uri


# Used for getting the locations of the executables
_test_folder, _ = os.path.split(__file__)
_repo_root, _ = os.path.split(_test_folder)


class TronSandboxException(Exception):
    pass


class MockConfigOptions(object):

    def __init__(self, server):
        self.server = server


class TronTestCase(TestCase):

    @setup
    def make_sandbox(self):
        """Set up a temp directory and storepaths to relevant binaries"""
        # I had a really hard time not calling this function make_sandwich()
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
                                 '--host=%s' % self.host]

        self.tron_server_address = '%s:%d' % (self.host, self.port)
        self.tron_server_uri = 'http://%s' % self.tron_server_address
        self.tron_server_arg = '--server=%s' % self.tron_server_address

        # mock a config object
        self.config_obj = MockConfigOptions(self.tron_server_uri)
        cmd.save_config(self.config_obj)

        self._last_trond_launch_args = []

    @teardown
    def delete_sandbox(self):
        """Delete the temp directory and its contents"""
        if os.path.exists(self.pid_file):
            self.stop_trond()
        shutil.rmtree(self.tmp_dir)

    ### Configuration ###

    def save_config(self, config_text):
        """Save a tron configuration to tron_config.yaml"""
        with open(self.config_file, 'w') as f:
            f.write(config_text)
        return config_text

    def upload_config(self, config_text):
        """Upload a tron configuration to the server"""
        cmd.load_config(self.config_obj)
        status, content = cmd.request(self.tron_server_uri,
                                      'config',
                                      {'config': config_text})
        if 'error' in content:
            raise TronSandboxException(content['error'])
        else:
            return status, content

    def get_config(self):
        """Get the text of the current configuration"""
        cmd.load_config(self.config_obj)
        status, content = cmd.request(self.tron_server_uri, '/config')
        if status != cmd.OK:
            raise TronSandboxException(content)
        else:
            return content['config']

    ### trond control ###

    def start_trond(self, args=None):
        """Start trond"""
        args = args or []
        self._last_trond_launch_args = args
        p = Popen([self.trond_bin] + self.trond_debug_args + args,
                  stdout=PIPE, stderr=PIPE)
        retval = p.communicate()
        time.sleep(0.1)
        return retval

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

    def ctl(self, command, arg=None, run_time=None):
        """Call the www API like tronctl does. ``command`` can be one of
        ``(start, cancel, disable, enable, disableall, enableall, fail, succeed)``.
        ``run_time`` should be of the
        form ``YYYY-MM-DD HH:MM:SS``.
        """
        content = self._check_call_api('/')

        data = {'command': command}

        if run_time is not None:
            data['run_time'] = run_time

        if arg is not None:
            job_to_uri = make_job_to_uri(content)
            service_to_uri = make_service_to_uri(content)
            full_uri = obj_spec_to_uri(arg, job_to_uri, service_to_uri)
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
        p = Popen([self.tronctl_bin] + args, stdout=PIPE, stderr=PIPE)
        return p.communicate()

    def tronview(self, args=None):
        """Call tronview with args and return ``(stdout, stderr)``"""
        args = args or []
        p = Popen([self.tronview_bin] + args, stdout=PIPE, stderr=PIPE)
        return p.communicate()
