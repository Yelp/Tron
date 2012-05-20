import datetime
import os
from textwrap import dedent

from testify import assert_equal
from testify import assert_gt
from tests import sandbox


BASIC_CONFIG = """
ssh_options:
    agent: true

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
      command: "echo 'at last'"
"""


class TrondTestCase(sandbox.SandboxTestCase):

    def test_end_to_end_basic(self):
        client = self.sandbox.client
        # start with a basic configuration
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.trond()
        # make sure it got in
        assert_equal(client.config(), SINGLE_ECHO_CONFIG)

        # reconfigure and confirm results
        second_config = DOUBLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT
        self.sandbox.tronfig(second_config)
        events = client.events()
        assert_equal(events[0]['name'], 'restoring')
        assert_equal(events[1]['name'], 'run_created')
        assert_equal(client.config(), second_config)

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
        result = self.sandbox.client.home()
        assert_equal(result, expected)

        # run the job and check its output
        self.sandbox.tronctl(['start', 'echo_job'])

        def wait_on_cleanup():
            return (len(client.job('echo_job')['runs']) >= 2 and
                    client.action('echo_job.1.echo_action')['state'] == 'SUCC')
        sandbox.wait_on_sandbox(wait_on_cleanup)

        echo_action_run = client.action('echo_job.1.echo_action')
        another_echo_action_run = client.action('echo_job.1.another_echo_action')
        assert_equal(echo_action_run['state'], 'SUCC')
        assert_equal(echo_action_run['stdout'], ['Echo!'])
        assert_equal(another_echo_action_run['state'], 'FAIL')
        assert_equal(another_echo_action_run['stdout'],
                     [datetime.datetime.now().strftime(
                         'Today is %Y-%m-%d, which is the same as %Y-%m-%d')])
        assert_equal(client.job_runs('echo_job.1')['state'], 'FAIL')

    def test_tronview_basic(self):
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.trond()

        expected = """\nServices:\nNo Services\n\n\nJobs:
            Name       State       Scheduler           Last Success
            echo_job   ENABLED     INTERVAL:1:00:00    None
            """

        def remove_line_space(s):
            return [l.replace(' ', '') for l in s.split('\n')]

        actual = self.sandbox.tronview()[0]
        assert_equal(remove_line_space(actual), remove_line_space(expected))

    def test_tronctl_basic(self):
        client = self.sandbox.client
        self.sandbox.save_config(SINGLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT)
        self.sandbox.trond()
        self.sandbox.tronctl(['start', 'echo_job'])

        def wait_on_cleanup():
            return client.action('echo_job.1.cleanup')['state'] == 'SUCC'
        sandbox.wait_on_sandbox(wait_on_cleanup)

        assert_equal(client.action('echo_job.1.echo_action')['state'], 'SUCC')
        assert_equal(client.job_runs('echo_job.1')['state'], 'SUCC')

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

        client = self.sandbox.client
        self.sandbox.trond()
        self.sandbox.tronfig(SERVICE_CONFIG)

        wait_on_config = lambda: 'fake_service' in client.config()
        sandbox.wait_on_sandbox(wait_on_config)

        self.sandbox.tronctl(['start', 'fake_service'])

        def wait_on_start():
            return client.service('fake_service')['state'] == 'STARTING'
        sandbox.wait_on_sandbox(wait_on_start)

        self.sandbox.tronctl(['zap', 'fake_service'])
        assert_equal('DOWN', client.service('fake_service')['state'])

    def test_cleanup_on_failure(self):
        FAIL_CONFIG = BASIC_CONFIG + dedent("""
        jobs:
          - name: "failjob"
            node: local
            schedule: "constant"
            actions:
              - name: "failaction"
                command: "failplz"
        """) + TOUCH_CLEANUP_FMT

        client = self.sandbox.client
        self.sandbox.save_config(FAIL_CONFIG)
        self.sandbox.trond()

        def wait_on_failaction():
            return client.action('failjob.0.failaction')['state'] == 'FAIL'
        sandbox.wait_on_sandbox(wait_on_failaction)

        def wait_on_cleanup():
            return client.action('failjob.1.cleanup')['state'] == 'SUCC'
        sandbox.wait_on_sandbox(wait_on_cleanup)

        assert_gt(len(client.job('failjob')['runs']), 1)

    def test_skip_failed_actions(self):
        CONFIG = BASIC_CONFIG + dedent("""
        jobs:
          - name: "multi_step_job"
            node: local
            schedule: "constant"
            actions:
              - name: "broken"
                command: "failingcommand"
              - name: "works"
                command: "echo ok"
                requires: broken
        """)

        client = self.sandbox.client
        self.sandbox.save_config(CONFIG)
        self.sandbox.trond()

        def build_wait_func(state):
            def wait_on_multi_step_job():
                action_name = 'multi_step_job.0.broken'
                return client.action(action_name)['state'] == state
            return wait_on_multi_step_job

        sandbox.wait_on_sandbox(build_wait_func('FAIL'))
        self.sandbox.tronctl(['skip', 'multi_step_job.0.broken'])
        assert_equal(client.action('multi_step_job.0.broken')['state'], 'SKIP')

        sandbox.wait_on_sandbox(build_wait_func('SKIP'))
        assert_equal(client.action('multi_step_job.0.works')['state'], 'SUCC')
        assert_equal(client.job_runs('multi_step_job.0')['state'], 'SUCC')

    def test_failure_on_multi_step_job_doesnt_wedge_tron(self):
        FAIL_CONFIG = BASIC_CONFIG + dedent("""
            jobs:
                -   name: "random_failure_job"
                    node: local
                    queueing: true
                    schedule: "constant"
                    actions:
                        -   name: "fa"
                            command: "sleep 0.1; failplz"
                        -   name: "sa"
                            command: "echo 'you will never see this'"
                            requires: [fa]
        """)

        client = self.sandbox.client
        self.sandbox.save_config(FAIL_CONFIG)
        self.sandbox.trond()

        def wait_on_random_failure_job():
            return len(client.job('random_failure_job')['runs']) >= 4
        sandbox.wait_on_sandbox(wait_on_random_failure_job)

        job_runs = client.job('random_failure_job')['runs']
        assert_equal([run['state'] for run in job_runs[-3:]], ['FAIL'] * 3)