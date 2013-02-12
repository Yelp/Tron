import datetime
import os
from textwrap import dedent

from testify import assert_equal
from testify import assert_gt
from testify.assertions import assert_in
from tests import sandbox
from tests.assertions import assert_length


BASIC_CONFIG = """
ssh_options:
    agent: true

nodes:
  - name: local
    hostname: 'localhost'

state_persistence:
    name: "state_data.shelve"
    store_type: shelve

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
        command: "echo 'Today is %(shortdate)s, which is the same
                    as %(year)s-%(month)s-%(day)s' && false" """

ALT_NAMESPACED_ECHO_CONFIG = """
jobs:
  - name: "echo_job"
    node: local
    schedule: "interval 1 hour"
    actions:
      - name: "echo_action"
        command: "echo 'Echo!'" """

TOUCH_CLEANUP_FMT = """
    cleanup_action:
      command: "echo 'at last'"
"""

def summarize_events(events):
    return [(event['entity'], event['name']) for event in events]


class TrondTestCase(sandbox.SandboxTestCase):

    def test_end_to_end_basic(self):
        client = self.sandbox.client
        # start with a basic configuration
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.trond()
        # make sure it got in
        assert_equal(client.config('MASTER')['config'], SINGLE_ECHO_CONFIG)

        # reconfigure and confirm results
        second_config = DOUBLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT
        self.sandbox.tronfig(second_config)
        events = summarize_events(client.events())
        assert_in(('', 'restoring'), events)
        assert_in(('MASTER.echo_job.0', 'created'), events)
        assert_equal(client.config('MASTER')['config'], second_config)

        # reconfigure, by uploading a third configuration
        self.sandbox.tronfig(ALT_NAMESPACED_ECHO_CONFIG, name='ohce')

        job1 = {
            'action_names': ['echo_action', 'cleanup', 'another_echo_action'],
            'status': 'ENABLED',
            'href': '/jobs/MASTER.echo_job',
            'last_success': None,
            'name': 'MASTER.echo_job',
            'scheduler': 'INTERVAL:1:00:00',
            'node_pool': ['localhost'],
            'runs': None
        }
        job2 = {
            'action_names': ['echo_action'],
            'status': 'ENABLED',
            'href': '/jobs/ohce.echo_job',
            'last_success': None,
            'name': 'ohce.echo_job',
            'scheduler': 'INTERVAL:1:00:00',
            'node_pool': ['localhost'],
            'runs': None
        }
        expected = {
            'jobs': [job1, job2],
            'status_href': '/status',
            'jobs_href': '/jobs',
            'config_href': '/config',
            'services': [],
            'services_href': '/services'
        }
        result = self.sandbox.client.home()
        assert_equal(result, expected)

        # run the job and check its output
        self.sandbox.tronctl(['start', 'MASTER.echo_job'])

        job_url = client.get_url('MASTER.echo_job')
        action_url = client.get_url('MASTER.echo_job.1.echo_action')
        another_action_url = client.get_url('MASTER.echo_job.1.another_echo_action')
        def wait_on_cleanup():
            return (len(client.job(job_url)['runs']) >= 2 and
                    client.action(action_url)['state'] == 'SUCC')
        sandbox.wait_on_sandbox(wait_on_cleanup)

        echo_action_run = client.action(action_url)
        other_act_run = client.action(another_action_url)
        assert_equal(echo_action_run['state'], 'SUCC')
        assert_equal(echo_action_run['stdout'], ['Echo!'])
        assert_equal(other_act_run['state'], 'FAIL')

        now = datetime.datetime.now()
        stdout = now.strftime('Today is %Y-%m-%d, which is the same as %Y-%m-%d')
        assert_equal(other_act_run['stdout'], [stdout])

        job_runs_url = client.get_url('MASTER.echo_job.1')
        assert_equal(client.job_runs(job_runs_url)['state'], 'FAIL')

    def test_tronview_basic(self):
        self.sandbox.save_config(SINGLE_ECHO_CONFIG)
        self.sandbox.trond()

        expected = """\nServices:\nNo Services\n\n\nJobs:
            Name       State       Scheduler           Last Success
            MASTER.echo_job   ENABLED     INTERVAL:1:00:00    None
            """

        def remove_line_space(s):
            return [l.replace(' ', '') for l in s.split('\n')]

        actual = self.sandbox.tronview()[0]
        assert_equal(remove_line_space(actual), remove_line_space(expected))

    def test_tronctl_basic(self):
        client = self.sandbox.client
        self.sandbox.save_config(SINGLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT)
        self.sandbox.trond()
        self.sandbox.tronctl(['start', 'MASTER.echo_job'])

        cleanup_url = client.get_url('MASTER.echo_job.1.cleanup')
        def wait_on_cleanup():
            return client.action(cleanup_url)['state'] == 'SUCC'
        sandbox.wait_on_sandbox(wait_on_cleanup)

        action_run_url = client.get_url('MASTER.echo_job.1.echo_action')
        assert_equal(client.action(action_run_url)['state'], 'SUCC')
        job_run_url = client.get_url('MASTER.echo_job.1')
        assert_equal(client.job_runs(job_run_url)['state'], 'SUCC')

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

        action_run_url = client.get_url('MASTER.failjob.0.failaction')
        def wait_on_failaction():
            return client.action(action_run_url)['state'] == 'FAIL'
        sandbox.wait_on_sandbox(wait_on_failaction)

        action_run_url = client.get_url('MASTER.failjob.1.cleanup')
        def wait_on_cleanup():
            return client.action(action_run_url)['state'] == 'SUCC'
        sandbox.wait_on_sandbox(wait_on_cleanup)

        assert_gt(len(client.job(client.get_url('MASTER.failjob'))['runs']), 1)

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
                requires: [broken]
        """)

        client = self.sandbox.client
        self.sandbox.save_config(CONFIG)
        self.sandbox.trond()
        action_run_url = client.get_url('MASTER.multi_step_job.0.broken')

        def build_wait_func(state):
            def wait_on_multi_step_job():
                return client.action(action_run_url)['state'] == state
            return wait_on_multi_step_job

        sandbox.wait_on_sandbox(build_wait_func('FAIL'))
        self.sandbox.tronctl(['skip', 'MASTER.multi_step_job.0.broken'])
        assert_equal(client.action(action_run_url)['state'], 'SKIP')

        sandbox.wait_on_sandbox(build_wait_func('SKIP'))
        action_run_url = client.get_url('MASTER.multi_step_job.0.works')
        assert_equal(client.action(action_run_url)['state'], 'SUCC')
        job_run_url = client.get_url('MASTER.multi_step_job.0')
        assert_equal(client.job_runs(job_run_url)['state'], 'SUCC')

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
        job_url = client.get_url('MASTER.random_failure_job')

        def wait_on_random_failure_job():
            return len(client.job(job_url)['runs']) >= 4
        sandbox.wait_on_sandbox(wait_on_random_failure_job)

        job_runs = client.job(job_url)['runs']
        assert_equal([run['state'] for run in job_runs[-3:]], ['FAIL'] * 3)

    def test_cancel_schedules_a_new_run(self):
        config = BASIC_CONFIG + dedent("""
            jobs:
                -   name: "a_job"
                    node: local
                    schedule: "daily 05:00:00"
                    actions:
                        -   name: "first_action"
                            command: "echo OK"
        """)

        client = self.sandbox.client
        self.sandbox.save_config(config)
        self.sandbox.trond()
        job_url = client.get_url('MASTER.a_job')

        self.sandbox.tronctl(['cancel', 'MASTER.a_job.0'])
        def wait_on_cancel():
            return len(client.job(job_url)['runs']) == 2
        sandbox.wait_on_sandbox(wait_on_cancel)

        job_runs = client.job(job_url)['runs']
        assert_length(job_runs, 2)
        run_states = [run['state'] for run in job_runs]
        assert_equal(run_states, ['SCHE', 'CANC'])

    def test_service_reconfigure(self):
        config_template = BASIC_CONFIG + dedent("""
            services:
                -   name: "a_service"
                    node: local
                    pid_file: "{wd}/%(name)s-%(instance_number)s.pid"
                    command: "{command}"
                    monitor_interval: {monitor_interval}
                    restart_interval: 2
        """)

        command = ("cd {path} && PYTHONPATH=. python "
                    "{path}/tests/mock_daemon.py %(pid_file)s")
        command = command.format(path=os.path.abspath('.'))
        config = config_template.format(
            command=command, monitor_interval=1, wd=self.sandbox.tmp_dir)
        client = self.sandbox.client
        self.sandbox.save_config(config)
        self.sandbox.trond()
        self.sandbox.tronctl(['start', 'MASTER.a_service'])
        service_url = client.get_url('MASTER.a_service')

        def wait_on_service_start():
            return client.service(service_url)['state'] == 'UP'
        sandbox.wait_on_sandbox(wait_on_service_start)

        new_config = config_template.format(
            command=command, monitor_interval=2, wd=self.sandbox.tmp_dir)
        self.sandbox.tronfig(new_config)

        self.sandbox.tronctl(['start', 'MASTER.a_service'])
        sandbox.wait_on_sandbox(wait_on_service_start)
        self.sandbox.tronctl(['stop', 'MASTER.a_service'])

        def wait_on_service_stop():
            return client.service(service_url)['state'] == 'DISABLED'
        sandbox.wait_on_sandbox(wait_on_service_stop)