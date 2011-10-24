import datetime
import os
from textwrap import dedent
import time
import yaml

from testify import *

from test.sandbox import TronSandbox, wait_for_file_to_exist


BASIC_CONFIG = """
--- !TronConfiguration
ssh_options:
        agent: true
nodes:
    - &local
        hostname: 'localhost'
"""

SINGLE_ECHO_CONFIG = BASIC_CONFIG + """
jobs:
    - &echo_job
        name: "echo_job"
        node: *local
        schedule: "interval 1 hour"
        actions:
            -
                name: "echo_action"
                command: "echo 'Echo!'" """

DOUBLE_ECHO_CONFIG = SINGLE_ECHO_CONFIG + """
            -
                name: "another_echo_action"
                command: "echo 'Today is %(shortdate)s, which is the same as %(year)s-%(month)s-%(day)s' && false" """

TOUCH_CLEANUP_FMT = """
        cleanup_action:
            command: "touch %s" """


class SandboxTestCase(TestCase):

    @setup
    def make_sandbox(self):
        self.sandbox = TronSandbox()

    @teardown
    def delete_sandbox(self):
        self.sandbox.delete()
        self.sandbox = None


class BasicTronTestCase(SandboxTestCase):

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
        assert_equal(self.sandbox.list_events()['data'][0]['name'], 'reconfig')
        assert_equal(self.sandbox.get_config(), second_config)
        assert_equal(self.sandbox.list_all(),
                     {'jobs': [{'status': 'ENABLED',
                                'href': '/jobs/echo_job',
                                'last_success': None,
                                'name': 'echo_job',
                                'scheduler': 'INTERVAL:1:00:00'}],
                      'status_href': '/status',
                      'jobs_href': '/jobs',
                      'config_href': '/config',
                      'services': [],
                      'services_href': '/services'})

        # run the job and check its output
        self.sandbox.ctl('start', 'echo_job')
        wait_for_file_to_exist(canary)
        assert_equal(self.sandbox.list_action_run('echo_job', 2, 'echo_action')['state'], 'SUCC')
        assert_equal(self.sandbox.list_action_run('echo_job', 2, 'echo_action')['stdout'], ['Echo!'])
        assert_equal(self.sandbox.list_action_run('echo_job', 2, 'another_echo_action')['state'], 'FAIL')
        assert_equal(self.sandbox.list_action_run('echo_job', 2, 'another_echo_action')['stdout'],
                     [datetime.datetime.now().strftime('Today is %Y-%m-%d, which is the same as %Y-%m-%d')])
        assert_equal(self.sandbox.list_job_run('echo_job', 2)['state'], 'FAIL')

    def test_tronview_basic(self):
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.start_trond()

        assert_equal(self.sandbox.tronview()[0], """Services:
No services

Jobs:
Name     State      Scheduler            Last Success        
echo_job ENABLED    INTERVAL:1:00:00     None                
""")

    def test_tronctl_basic(self):
        canary = os.path.join(self.sandbox.tmp_dir, 'tronctl_basic_done')
        self.sandbox.save_config(SINGLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT % canary)
        self.sandbox.start_trond()

        # run the job and check its output
        self.sandbox.tronctl(['start', 'echo_job'])
        wait_for_file_to_exist(canary)
        assert_equal(self.sandbox.list_action_run('echo_job', 1, 'echo_action')['state'], 'SUCC')
        assert_equal(self.sandbox.list_job_run('echo_job', 1)['state'], 'SUCC')


class SchedulerTestCase(SandboxTestCase):


    QUEUE_CONFIG = dedent("""
        --- !TronConfiguration
        ssh_options:
                agent: true
        nodes:
            - &local
                hostname: 'localhost'
        jobs:
            - &echo_job
                name: "delayed_echo_job"
                node: *local
                queueing: true
                schedule: "interval 1 second"
                actions:
                    -
                        name: "delayed_echo_action"
                        command: "sleep 2 && echo 'Echo'"
        """)

    EMPTY_CONFIG = dedent("""
        --- !TronConfiguration
        ssh_options:
                agent: true
        nodes:
            - &local
                hostname: 'localhost'
        jobs:
            - &echo_job
                name: "delayed_echo_job"
                node: *local
                queueing: true
                schedule: "interval 1 hour"
                actions:
                    -
                        name: "delayed_echo_action"
                        command: "sleep 2 && echo 'Echo'"
        """)

    def test_queue_on_overlap(self):
        job_output_dir = os.path.join(self.sandbox.tmp_dir, 'delayed_echo_job')
        self.sandbox.save_config(SchedulerTestCase.QUEUE_CONFIG)
        self.sandbox.start_trond()
        time.sleep(4)
        self.sandbox.upload_config(SchedulerTestCase.QUEUE_CONFIG)

        print self.sandbox.list_job('delayed_echo_job')

        # at this point, up to 5 jobs have been queued up, but only
        # up to 2 should have output

        output_dirs = sorted(os.listdir(job_output_dir))
        for d in output_dirs:
            print os.listdir(os.path.join(job_output_dir, d))

        time.sleep(5)
        print '-----'
        print self.sandbox.list_job('delayed_echo_job')
        output_dirs = sorted(os.listdir(job_output_dir))
        for d in output_dirs:
            print os.listdir(os.path.join(job_output_dir, d))
