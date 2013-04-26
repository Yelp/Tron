import datetime
import os
from textwrap import dedent

from testify import assert_equal
from testify import assert_gt
from testify.assertions import assert_in
from tests import sandbox
from tron.core import service, actionrun


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


class TrondEndToEndTestCase(sandbox.SandboxTestCase):

    def test_end_to_end_basic(self):
        self.start_with_config(SINGLE_ECHO_CONFIG)
        client = self.sandbox.client

        assert_equal(self.client.config('MASTER')['config'], SINGLE_ECHO_CONFIG)

        # reconfigure and confirm results
        second_config = DOUBLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT
        self.sandbox.tronfig(second_config)
        events = summarize_events(client.events())
        assert_in(('', 'restoring'), events)
        assert_in(('MASTER.echo_job.0', 'created'), events)
        assert_equal(client.config('MASTER')['config'], second_config)

        # reconfigure, by uploading a third configuration
        self.sandbox.tronfig(ALT_NAMESPACED_ECHO_CONFIG, name='ohce')
        self.sandbox.client.home()

        # run the job and check its output
        echo_job_name = 'MASTER.echo_job'
        job_url = client.get_url(echo_job_name)
        action_url = client.get_url('MASTER.echo_job.1.echo_action')

        self.sandbox.tronctl_start(echo_job_name)

        def wait_on_cleanup():
            return (len(client.job(job_url)['runs']) >= 2 and
                    client.action_runs(action_url)['state'] ==
                    actionrun.ActionRun.STATE_SUCCEEDED.name)
        sandbox.wait_on_sandbox(wait_on_cleanup)

        echo_action_run = client.action_runs(action_url)
        another_action_url = client.get_url('MASTER.echo_job.1.another_echo_action')
        other_act_run = client.action_runs(another_action_url)
        assert_equal(echo_action_run['state'],
            actionrun.ActionRun.STATE_SUCCEEDED.name)
        assert_equal(echo_action_run['stdout'], ['Echo!'])
        assert_equal(other_act_run['state'],
            actionrun.ActionRun.STATE_FAILED.name)

        now = datetime.datetime.now()
        stdout = now.strftime('Today is %Y-%m-%d, which is the same as %Y-%m-%d')
        assert_equal(other_act_run['stdout'], [stdout])

        job_runs_url = client.get_url('%s.1' % echo_job_name)
        assert_equal(client.job_runs(job_runs_url)['state'],
            actionrun.ActionRun.STATE_FAILED.name)

    def test_tronview_basic(self):
        self.start_with_config(SINGLE_ECHO_CONFIG)
        expected = """\nServices:\nNo Services\n\n\nJobs:
            Name       State       Scheduler           Last Success
            MASTER.echo_job   enabled     interval 1:00:00    None
            """

        def remove_line_space(s):
            return [l.replace(' ', '') for l in s.split('\n')]

        actual = self.sandbox.tronview()[0]
        assert_equal(remove_line_space(actual), remove_line_space(expected))

    def test_tronctl_basic(self):
        self.start_with_config(SINGLE_ECHO_CONFIG + TOUCH_CLEANUP_FMT)
        self.sandbox.tronctl_start('MASTER.echo_job')

        cleanup_url = self.client.get_url('MASTER.echo_job.1.cleanup')
        sandbox.wait_on_state(self.client.action_runs, cleanup_url,
            actionrun.ActionRun.STATE_SUCCEEDED.name)

        action_run_url = self.client.get_url('MASTER.echo_job.1.echo_action')
        assert_equal(self.client.action_runs(action_run_url)['state'],
            actionrun.ActionRun.STATE_SUCCEEDED.name)

        job_run_url = self.client.get_url('MASTER.echo_job.1')
        assert_equal(self.client.job_runs(job_run_url)['state'],
            actionrun.ActionRun.STATE_SUCCEEDED.name)

    def test_node_reconfig(self):
        job_service_config = dedent("""
            jobs:
                - name: a_job
                  node: local
                  schedule: "interval 1s"
                  actions:
                    - name: first_action
                      command: "echo something"

            services:
                - name: a_service
                  node: local
                  pid_file: /tmp/does_not_exist
                  command: "echo service start"
                  monitor_interval: 1
        """)
        second_config = dedent("""
            ssh_options:
                agent: true

            nodes:
              - name: local
                hostname: '127.0.0.1'

            state_persistence:
                name: "state_data.shelve"
                store_type: shelve

        """) + job_service_config
        self.start_with_config(BASIC_CONFIG + job_service_config)

        service_name = 'MASTER.a_service'
        service_url = self.client.get_url(service_name)
        self.sandbox.tronctl_start(service_name)
        sandbox.wait_on_state(self.client.service, service_url,
            service.ServiceState.FAILED)

        job_url = self.client.get_url('MASTER.a_job.0')
        sandbox.wait_on_state(self.client.job_runs, job_url,
            actionrun.ActionRun.STATE_SUCCEEDED.name)

        self.sandbox.tronfig(second_config)

        sandbox.wait_on_state(self.client.service, service_url,
            service.ServiceState.DISABLED)

        job_url = self.client.get_url('MASTER.a_job')
        def wait_on_next_run():
            last_run = self.client.job(job_url)['runs'][0]
            return last_run['node']['hostname'] == '127.0.0.1'

        sandbox.wait_on_sandbox(wait_on_next_run)


class JobEndToEndTestCase(sandbox.SandboxTestCase):

    def test_cleanup_on_failure(self):
        config = BASIC_CONFIG + dedent("""
        jobs:
          - name: "failjob"
            node: local
            schedule: "constant"
            actions:
              - name: "failaction"
                command: "failplz"
        """) + TOUCH_CLEANUP_FMT
        self.start_with_config(config)

        action_run_url = self.client.get_url('MASTER.failjob.0.failaction')
        sandbox.wait_on_state(self.client.action_runs, action_run_url,
            actionrun.ActionRun.STATE_FAILED.name)

        action_run_url = self.client.get_url('MASTER.failjob.1.cleanup')
        sandbox.wait_on_state(self.client.action_runs, action_run_url,
            actionrun.ActionRun.STATE_SUCCEEDED.name)
        job_runs = self.client.job(self.client.get_url('MASTER.failjob'))['runs']
        assert_gt(len(job_runs), 1)

    def test_skip_failed_actions(self):
        config = BASIC_CONFIG + dedent("""
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
        self.start_with_config(config)
        action_run_url = self.client.get_url('MASTER.multi_step_job.0.broken')
        waiter = sandbox.build_waiter_func(self.client.action_runs, action_run_url)

        waiter(actionrun.ActionRun.STATE_FAILED.name)
        self.sandbox.tronctl(['skip', 'MASTER.multi_step_job.0.broken'])
        waiter(actionrun.ActionRun.STATE_SKIPPED.name)

        action_run_url = self.client.get_url('MASTER.multi_step_job.0.works')
        sandbox.wait_on_state(self.client.action_runs, action_run_url,
            actionrun.ActionRun.STATE_SUCCEEDED.name)

        job_run_url = self.client.get_url('MASTER.multi_step_job.0')
        sandbox.wait_on_state(self.client.job_runs, job_run_url,
            actionrun.ActionRun.STATE_SUCCEEDED.name)

    def test_failure_on_multi_step_job_doesnt_wedge_tron(self):
        config = BASIC_CONFIG + dedent("""
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
        self.start_with_config(config)
        job_url = self.client.get_url('MASTER.random_failure_job')

        def wait_on_random_failure_job():
            return len(self.client.job(job_url)['runs']) >= 4
        sandbox.wait_on_sandbox(wait_on_random_failure_job)

        job_runs = self.client.job(job_url)['runs']
        expected = [actionrun.ActionRun.STATE_FAILED.name for _ in range(3)]
        assert_equal([run['state'] for run in job_runs[-3:]], expected)

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
        self.start_with_config(config)
        job_name = 'MASTER.a_job'
        job_url = self.client.get_url(job_name)

        self.sandbox.tronctl(['cancel', '%s.0' % job_name])
        def wait_on_cancel():
            return len(self.client.job(job_url)['runs']) == 2
        sandbox.wait_on_sandbox(wait_on_cancel)

        run_states = [run['state'] for run in self.client.job(job_url)['runs']]
        expected = [
            actionrun.ActionRun.STATE_SCHEDULED.name,
            actionrun.ActionRun.STATE_CANCELLED.name]
        assert_equal(run_states, expected)

    def test_job_queueing_false_with_overlap(self):
        """Test that a job that has queueing false properly cancels an
        overlapping job run.
        """
        config = BASIC_CONFIG + dedent("""
            jobs:
                -   name: "cancel_overlap"
                    schedule: "interval 1s"
                    queueing: False
                    node: local
                    actions:
                        -   name: "do_something"
                            command: "sleep 3s"
                        -   name: "do_other"
                            command: "sleep 3s"
                    cleanup_action:
                        command: "echo done"
        """)
        self.start_with_config(config)
        job_url = self.client.get_url('MASTER.cancel_overlap')
        job_run_url = self.client.get_url('MASTER.cancel_overlap.1')

        def wait_on_job_schedule():
            return len(self.client.job(job_url)['runs']) == 2
        sandbox.wait_on_sandbox(wait_on_job_schedule)

        sandbox.wait_on_state(self.client.job, job_run_url,
            actionrun.ActionRun.STATE_CANCELLED.name)

        action_run_states = [action_run['state'] for action_run in
                             self.client.job_runs(job_run_url)['runs']]
        expected = [actionrun.ActionRun.STATE_CANCELLED.name
                    for _ in xrange(len(action_run_states))]
        assert_equal(action_run_states, expected)


class ServiceEndToEndTestCase(sandbox.SandboxTestCase):

    def test_service_reconfigure(self):
        config_template = BASIC_CONFIG + dedent("""
            services:
                -   name: "a_service"
                    node: local
                    pid_file: "{wd}/%(name)s-%(instance_number)s.pid"
                    command: "{command}"
                    monitor_interval: {monitor_interval}
                    restart_delay: 2
        """)

        command = ("cd {path} && PYTHONPATH=. python "
                   "{path}/tests/mock_daemon.py %(pid_file)s")
        command = command.format(path=os.path.abspath('.'))
        config = config_template.format(
            command=command, monitor_interval=1, wd=self.sandbox.tmp_dir)

        self.start_with_config(config)
        service_name = 'MASTER.a_service'
        service_url = self.client.get_url(service_name)

        self.sandbox.tronctl_start(service_name)
        waiter = sandbox.build_waiter_func(self.client.service, service_url)
        waiter(service.ServiceState.UP)

        new_config = config_template.format(
            command=command, monitor_interval=2, wd=self.sandbox.tmp_dir)
        self.sandbox.tronfig(new_config)

        waiter(service.ServiceState.DISABLED)
        self.sandbox.tronctl_start(service_name)
        waiter(service.ServiceState.UP)
        self.sandbox.tronctl_stop(service_name)
        waiter(service.ServiceState.DISABLED)

    def test_service_failed_restart(self):
        config = BASIC_CONFIG + dedent("""
            services:
                -   name: service_restart
                    node: local
                    pid_file: "/tmp/file_dne"
                    command: "sleep 1; cat /bogus/file/DNE"
                    monitor_interval: 1
                    restart_delay: 2
        """)
        self.start_with_config(config)
        service_name = 'MASTER.service_restart'
        service_url = self.client.get_url(service_name)
        self.sandbox.tronctl_start(service_name)

        waiter = sandbox.build_waiter_func(self.client.service, service_url)
        waiter(service.ServiceState.FAILED)
        service_content = self.client.service(service_url)
        expected = 'cat: /bogus/file/DNE: No such file or directory'
        assert_in(service_content['instances'][0]['failures'][0], expected)
        waiter(service.ServiceState.STARTING)
