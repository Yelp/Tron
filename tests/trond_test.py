import datetime
import os
import sys
from textwrap import dedent
import time

from testify import TestCase, setup, teardown, assert_equal, assert_in, suite
from testify import assert_gt

from tests.sandbox import TronSandbox, TronSandboxException
from tests.sandbox import wait_for_file_to_exist


BASIC_CONFIG = """
nodes:
  - name: local
    hostname: 'localhost'
"""

SINGLE_ECHO_CONFIG = BASIC_CONFIG + """
jobs:
  - name: "echo_job"
    node: local
    schedule: "interval 1 hour"
    actions:
      - name: "echo_action"
        command: "echo 'Echo!'" """

DOUBLE_ECHO_CONFIG = SINGLE_ECHO_CONFIG + """
      - name: "another_echo_action"
        command: "echo 'Today is %(shortdate)s, which is the same as %(year)s-%(month)s-%(day)s' && false" """

TOUCH_CLEANUP_FMT = """
    cleanup_action:
      command: "touch %s.%%(runid)s" """


class SandboxTestCase(TestCase):

    @setup
    def make_sandbox(self):
        self.sandbox = TronSandbox()

    @teardown
    def delete_sandbox(self):
        self.sandbox.delete()
        self.sandbox = None


class BasicTronTestCase(SandboxTestCase):

    @suite('sandbox')
    def test_end_to_end_basic(self):
        # start with a basic configuration
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.start_trond()
        # make sure it got in
        assert_equal(self.sandbox.get_config(), SINGLE_ECHO_CONFIG)

        # reconfigure and confirm results
        canary = os.path.join(self.sandbox.tmp_dir, 'end_to_end_done')
        second_config = DOUBLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT % canary
        self.sandbox.upload_config(second_config)
        assert_equal(self.sandbox.list_events()['data'][0]['name'], 'restoring')
        assert_equal(self.sandbox.list_events()['data'][1]['name'], 'run_created')
        assert_equal(self.sandbox.get_config(), second_config)

        expected = {'jobs': [
                {
                    'action_names': ['echo_action', 'cleanup', 'another_echo_action'],
                    'status': 'ENABLED',
                    'href': '/jobs/echo_job',
                    'last_success': None,
                    'name': 'echo_job',
                    'scheduler': 'INTERVAL:1:00:00',
                    'node_pool': ['localhost'],
                    'runs': None
                }
            ],
            'status_href': '/status',
            'jobs_href': '/jobs',
            'config_href': '/config',
            'services': [],
            'services_href': '/services'
        }
        result = self.sandbox.list_all()
        assert_equal(result, expected)

        # run the job and check its output
        self.sandbox.ctl('start', 'echo_job')

        try:
            wait_for_file_to_exist(canary + '.echo_job.1.cleanup')
        except TronSandboxException:
            print >> sys.stderr, "trond appears to have crashed. Log:"
            print >> sys.stderr, self.sandbox.log_contents()
            raise

        echo_action_run = self.sandbox.list_action_run(
            'echo_job', 1, 'echo_action')
        another_echo_action_run = self.sandbox.list_action_run(
            'echo_job', 1, 'another_echo_action')
        assert_equal(echo_action_run['state'], 'SUCC')
        assert_equal(echo_action_run['stdout'], ['Echo!'])
        assert_equal(another_echo_action_run['state'], 'FAIL')
        assert_equal(another_echo_action_run['stdout'],
                     [datetime.datetime.now().strftime(
                         'Today is %Y-%m-%d, which is the same as %Y-%m-%d')])
        assert_equal(self.sandbox.list_job_run('echo_job', 1)['state'],
                     'FAIL')

    @suite('sandbox')
    def test_tronview_basic(self):
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.start_trond()

        expected = """\nServices:\nNo Services\n\n\nJobs:
            Name       State       Scheduler           Last Success
            echo_job   ENABLED     INTERVAL:1:00:00    None
            """

        def remove_line_space(s):
            return [l.replace(' ', '') for l in s.split('\n')]

        assert_equal(
            remove_line_space(self.sandbox.tronview()[0]),
            remove_line_space(expected)
        )

    @suite('sandbox')
    def test_tronctl_basic(self):
        canary = os.path.join(self.sandbox.tmp_dir, 'tronctl_basic_done')
        self.sandbox.save_config(SINGLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT % canary)
        self.sandbox.start_trond()

        # run the job and check its output
        self.sandbox.tronctl(['start', 'echo_job'])
        wait_for_file_to_exist(canary + '.echo_job.1.cleanup')

        assert_equal(self.sandbox.list_action_run('echo_job', 1, 'echo_action')['state'], 'SUCC')
        assert_equal(self.sandbox.list_job_run('echo_job', 1)['state'], 'SUCC')

    @suite('sandbox')
    def test_tronctl_service_zap(self):
        SERVICE_CONFIG = dedent("""
        nodes:
          - name: local
            hostname: 'localhost'
        services:
          - name: "fake_service"
            node: local
            count: 1
            pid_file: "%%(name)s-%%(instance_number)s.pid"
            command: "echo %(pid)s > %%(pid_file)s"
            monitor_interval: 0.1
        """ % {'pid': os.getpid()})

        self.sandbox.start_trond()
        self.sandbox.upload_config(SERVICE_CONFIG)
        time.sleep(1)

        self.sandbox.ctl('start', 'fake_service')
        self.sandbox.tronctl(['start', 'fake_service'])

        time.sleep(1)
        self.sandbox.tronctl(['zap', 'fake_service'])
        assert_equal('DOWN', self.sandbox.list_service('fake_service')['state'])

    @suite('sandbox')
    def test_cleanup_on_failure(self):
        canary = os.path.join(self.sandbox.tmp_dir, 'end_to_end_done')

        FAIL_CONFIG = dedent("""
        nodes:
          - name: local
            hostname: 'localhost'
        jobs:
          - name: "failjob"
            node: local
            schedule: "interval 1 seconds"
            actions:
              - name: "failaction"
                command: "failplz"
        """) + TOUCH_CLEANUP_FMT % canary

        # start with a basic configuration
        self.sandbox.save_config(FAIL_CONFIG)
        self.sandbox.start_trond()

        time.sleep(3)

        assert os.path.exists(canary + '.failjob.0.cleanup')
        assert_gt(len(self.sandbox.list_job('failjob')['runs']), 1)

    @suite('sandbox')
    def test_skip_failed_actions(self):
        CONFIG = dedent("""
        nodes:
          - name: local
            hostname: 'localhost'
        jobs:
          - name: "multi_step_job"
            node: local
            schedule: "interval 1 seconds"
            actions:
              - name: "broken"
                command: "failingcommand"
              - name: "works"
                command: "echo ok"
                requires: broken
        """)

        self.sandbox.save_config(CONFIG)
        self.sandbox.start_trond()
        time.sleep(2)

        self.sandbox.tronctl(['skip', 'multi_step_job.0.broken'])
        action_run = self.sandbox.list_action_run('multi_step_job', 0, 'broken')
        assert_equal(action_run['state'], 'SKIP')

        action_run = self.sandbox.list_action_run('multi_step_job', 0, 'works')
        assert_equal(action_run['state'], 'SUCC')
        job_run = self.sandbox.list_job_run('multi_step_job', 0)
        assert_equal(job_run['state'], 'SUCC')

    @suite('sandbox')
    def test_failure_on_multi_step_job_doesnt_wedge_tron(self):
        # WARNING: This test may be flaky.
        FAIL_CONFIG = dedent("""
            ssh_options: !SSHOptions
              agent: true

            nodes:
              - &local
                  hostname: 'localhost'
            jobs:
              - &random_job
                name: "random_failure_job"
                node: *local
                queueing: true
                schedule: "interval 1 seconds"
                actions:
                  - &fa
                    name: "fa"
                    command: "sleep 1.1; failplz"
                  - &sa
                    name: "sa"
                    command: "echo 'you will never see this'"
                    requires: [*fa]
        """)

        with open(self.sandbox.config_file, 'w') as f:
            f.write(FAIL_CONFIG)
        self.sandbox.start_trond()

        # Wait a little to give things time to explode
        time.sleep(1.5)
        jerb = self.sandbox.list_job('random_failure_job')
        total_tries = 0
        while ((len(jerb['runs']) < 3 or
                jerb['runs'][-1][u'state'] not in [u'FAIL', u'SUCC']) and
                total_tries < 30):
            time.sleep(0.2)
            jerb = self.sandbox.list_job('random_failure_job')
            total_tries += 1

        assert_equal(jerb['runs'][-1][u'state'], u'FAIL')
        assert_in(jerb['runs'][-2][u'state'], [u'FAIL', u'RUNN'])
        assert_equal(jerb['runs'][0][u'state'], u'SCHE')
