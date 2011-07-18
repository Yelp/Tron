import os
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
from testify import *
import time

from tron import cmd


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
        self.troview_bin = os.path.join(self.tron_bin, 'tronview')

        self.log_file = os.path.join(self.tmp_dir, 'tron.log')
        self.pid_file = os.path.join(self.tmp_dir, 'tron.pid')
        self.config_file = os.path.join(self.tmp_dir, 'tron_config.yaml')

        self.port = 8089
        self.host = 'localhost'

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

    def send_command(self, command, arg):
        cmd.load_config(self.config_obj)
        status, content = cmd.request(options.server, "/")

        if status != cmd.OK or not content:
            raise TronSandboxException("Error connecting to tron server at %s" % options.server)

        job_to_uri = dict([(job['name'], job['href']) for job in content['jobs']])
        service_to_uri = dict([(service['name'], service['href']) for service in content['services']])

    ### Basic subprocesses ###

    def tronctl(self, args=None):
        """Call tronctl with args"""
        args = args or []
        p = Popen([self.tronctl_bin] + args, stdout=PIPE, stderr=PIPE)
        return p.communicate()

    def tronview(self, args=None):
        """Call tronview with args"""
        args = args or []
        p = Popen([self.tronview_bin] + args, stdout=PIPE, stderr=PIPE)
        return p.communicate()
