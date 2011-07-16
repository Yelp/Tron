import os
import shutil
import tempfile
from testify import TestCase

class TronTestCase(TestCase):

    @setup
    def make_sandbox(self):
        self.tmp_dir = tempfile.mkdtemp()
        test_folder, _ = os.path.split(__file__)
        repo_root, _ = os.path.split(test_folder)
        self.tron_bin = os.path.join(repo_root, 'bin')
        self.tronctl_bin = os.path.join(self.tron_bin, 'tronctl')
        self.trond_bin = os.path.join(self.tron_bin, 'trond')
        self.tronfig_bin = os.path.join(self.tron_bin, 'tronfig')
        self.troview_bin = os.path.join(self.tron_bin, 'tronview')

        self.trond_debug_args = ['--working-dir=%s' % self.tmp_dir,
                                 '--log-file=%s' % os.path.join(self.tmp_dir, 'tron.log'),
                                 '--pid-file=%s' % os.path.join(self.tmp_dir, 'tron.pid')]

    @teardown
    def delete_sandbox(self):
        shutil.rmtree(self.tmp_dir)

    def start_tron(self, args=None):
        args = args or None
        # call trond

    def stop_tron(self):
        pass

    def restart_tron(self):
        pass

    def do_some_job_action(self):
        pass

    def tronctl(self):
        pass

    def tronview(self):
        pass

    def exit_with_failure(self):
        pass

    def exit_with_success(self):
        pass

    def daemonize(self):
        pass

    def log_stream(self):
        pass
