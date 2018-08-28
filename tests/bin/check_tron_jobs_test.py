from __future__ import absolute_import
from __future__ import unicode_literals

import time

import check_tron_jobs
from check_tron_jobs import State
from mock import patch
from mock import PropertyMock

from testifycompat import assert_equal
from testifycompat import TestCase


class TestCheckJobs(TestCase):
    @patch('check_tron_jobs.check_job_result', autospec=True)
    @patch('check_tron_jobs.Client', autospec=True)
    @patch('check_tron_jobs.cmd_utils', autospec=True)
    @patch('check_tron_jobs.parse_cli', autospec=True)
    def test_check_job_result_exception(
        self,
        mock_args,
        mock_cmd_utils,
        mock_client,
        mock_check_job_result,
    ):
        type(mock_args.return_value).job = PropertyMock(return_value=None)
        type(mock_args.return_value).run_interval = 300
        mock_client.return_value.jobs.return_value = [
            {
                'name': 'job1',
            },
            {
                'name': 'job2',
            },
            {
                'name': 'job3',
            },
        ]
        mock_check_job_result.side_effect = [
            KeyError('foo'),
            None,
            TypeError,
        ]
        error_code = check_tron_jobs.main()
        assert_equal(error_code, 1)
        assert_equal(mock_check_job_result.call_count, 3)

# These tests test job run succeeded scenarios

    def test_job_succeeded(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.SUCCEEDED)

    def test_job_running_and_action_succeeded(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                    'runs': [
                        {
                            'id': 'MASTER.test.2.action2',
                            'state': 'running',
                        },
                        {
                            'id': 'MASTER.test.1.action1',
                            'state': 'succeeded',
                        },
                    ],  # noqa: E122
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.SUCCEEDED)

    def test_get_relevant_action_picks_the_first_one_succeeded(self):
        action_runs = [
            {
                'id':
                    'MASTER.test.action1',
                'action_name':
                    'action1',
                'state':
                    'succeeded',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1200)
                    ),
                'duration':
                    '0:18:01.475067',
            },
            {
                'id':
                    'MASTER.test.action2',
                'action_name':
                    'action2',
                'state':
                    'succeeded',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '0:08:02.005783',
            },
            {
                'id':
                    'MASTER.test.action1',
                'action_name':
                    'action1',
                'state':
                    'succeeded',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time())
                    ),
                'duration':
                    '0:00:01.006305',
            },
        ]
        actual = check_tron_jobs.get_relevant_action(
            action_runs=action_runs,
            last_state=State.SUCCEEDED,
            actions_expected_runtime={
                'action1': 86400.0,
                'action2': 86400.0,
                'action3': 86400.0
            },
        )
        assert_equal(actual["id"], "MASTER.test.action1")

# These tests test job run failed scenarios

    def test_job_failed(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'failed',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.FAILED)

    def test_most_recent_end_time_job_failed(self):
        job_runs = {
            'status':
                'scheduled',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'failed',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 500),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.FAILED)

    def test_rerun_job_failed(self):
        job_runs = {
            'status':
                'scheduled',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.4',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'failed',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 100),
                        ),
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 500),
                        ),
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'failed',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.3')
        assert_equal(state, State.FAILED)

    def test_job_running_but_action_failed_already(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                    'runs': [
                        {
                            'id': 'MASTER.test.2.action2',
                            'state': 'running',
                        },
                        {
                            'id': 'MASTER.test.1.action1',
                            'state': 'failed',
                        },
                    ],  # noqa: E122
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.FAILED)

    def test_get_relevant_action_picks_the_one_that_failed(self):
        action_runs = [
            {
                'node': {
                    'username': 'batch',
                    'hostname': 'localhost',
                    'name': 'localhost',
                    'port': 22,
                },
                'raw_command': '/bin/false',
                'requirements': [],
                'run_num': '582',
                'exit_status': 1,
                'stdout': None,
                'start_time': '2018-02-05 17:40:00',
                'id': 'MASTER.kwatest.582.action1',
                'action_name': 'action1',
                'state': 'failed',
                'command': '/bin/false',
                'end_time': '2018-02-05 17:40:00',
                'stderr': None,
                'duration': '0:00:00.065018',
                'job_name': 'MASTER.kwatest',
            },
            {
                'node': {
                    'username': 'batch',
                    'hostname': 'localhost',
                    'name': 'localhost',
                    'port': 22,
                },
                'raw_command': '/bin/true',
                'requirements': [],
                'run_num': '582',
                'exit_status': 0,
                'stdout': None,
                'start_time': '2018-02-05 17:40:00',
                'id': 'MASTER.kwatest.582.action2',
                'action_name': 'action2',
                'state': 'succeeded',
                'command': '/bin/true',
                'end_time': '2018-02-05 17:40:00',
                'stderr': None,
                'duration': '0:00:00.046243',
                'job_name': 'MASTER.kwatest',
            },
        ]
        actual = check_tron_jobs.get_relevant_action(
            action_runs=action_runs,
            last_state=State.FAILED,
            actions_expected_runtime={}
        )
        assert_equal(actual["state"], "failed")

# These tests test job/action stuck scenarios

    def test_job_stuck(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'queued',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1200),
                        ),
                    'end_time':
                        None,
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.STUCK)

    def test_job_running_job_exceeds_expected_runtime(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'expected_runtime':
                480.0,
            'runs': [
                {
                    'id':
                        'MASTER.test.100',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                    'start_time':
                        None,
                    'duration':
                        '',
                },
                {
                    'id':
                        'MASTER.test.99',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                    'duration':
                        '0:10:01.883601',
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.99')
        assert_equal(state, State.STUCK)

    def test_job_running_action_exceeds_expected_runtime(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'actions_expected_runtime': {
                'action1': 720.0,
                'action2': 480.0
            },
            'runs': [
                dict(
                    id='MASTER.test.3',
                    state='scheduled',
                    run_time=time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() + 600),
                    ),
                    end_time=None,
                    duration='',
                ),
                dict(
                    id='MASTER.test.2',
                    state='running',
                    run_time=time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 600),
                    ),
                    end_time=None,
                    duration='0:10:01.883601',
                    runs=[
                        dict(
                            id='MASTER.test.2.action2',
                            state='running',
                            action_name='action2',
                            start_time=time.strftime(
                                '%Y-%m-%d %H:%M:%S',
                                time.localtime(time.time() - 600),
                            ),
                            duration='0:10:01.883601',
                        ),
                        dict(
                            id='MASTER.test.1.action1',
                            state='running',
                            action_name='action1',
                            start_time=time.strftime(
                                '%Y-%m-%d %H:%M:%S',
                                time.localtime(time.time() - 600),
                            ),
                            duration='0:10:01.885401',
                        ),
                    ],
                ),
                dict(
                    id='MASTER.test.1',
                    state='succeeded',
                    run_time=time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1800),
                    ),
                    end_time=time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1700),
                    ),
                    duration='0:15:00.453601',
                ),
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.STUCK)

    def test_job_stuck_when_runtime_not_sorted(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time()),
                        ),
                    'end_time':
                        None,
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.STUCK)

    def test_get_relevant_action_pick_the_one_stuck(self):
        action_runs = [
            {
                'id':
                    'MASTER.test.1.action3',
                'state':
                    'succeeded',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1200)
                    ),
                'duration':
                    '0:18:01.475067',
            },
            {
                'id':
                    'MASTER.test.1.action2',
                'state':
                    'running',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1100)
                    ),
                'duration':
                    '0:18:40.005783',
            },
            {
                'id':
                    'MASTER.test.1.action1',
                'state':
                    'succeeded',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1000)
                    ),
                'duration':
                    '0:00:01.006305',
            },
        ]
        actual = check_tron_jobs.get_relevant_action(
            action_runs=action_runs,
            last_state=State.STUCK,
            actions_expected_runtime={
                'action1': 86400.0,
                'action2': 86400.0,
                'action3': 86400.0,
            }
        )
        assert_equal(actual["id"], "MASTER.test.1.action2")

    def test_get_relevant_action_pick_the_one_exceeds_expected_runtime(self):
        action_runs = [
            {
                'id':
                    'MASTER.test.1.action3',
                'state':
                    'running',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '0:10:00.006305',
            },
            {
                'id':
                    'MASTER.test.1.action2',
                'state':
                    'running',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '0:10:00.006383',
            },
            {
                'id':
                    'MASTER.test.1.action1',
                'state':
                    'succeeded',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '0:10:00.006331',
            },
        ]
        actions_expected_runtime = {
            'action3': 480.0,
            'action2': 720.0,
            'action1': 900.0
        }
        actual = check_tron_jobs.get_relevant_action(
            action_runs=action_runs,
            last_state=State.STUCK,
            actions_expected_runtime=actions_expected_runtime
        )
        assert_equal(actual["id"], "MASTER.test.1.action3")

    def test_get_relevant_action_pick_the_one_exceeds_expected_runtime_with_long_duration(
        self
    ):
        action_runs = [
            {
                'id':
                    'MASTER.test.1.action3',
                'action_name':
                    'action3',
                'state':
                    'running',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '1 day, 0:10:00.006305',
            },
            {
                'id':
                    'MASTER.test.1.action2',
                'action_name':
                    'action2',
                'state':
                    'running',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '2 days, 0:10:00.006383',
            },
            {
                'id':
                    'MASTER.test.1.action1',
                'action_name':
                    'action1',
                'state':
                    'running',
                'start_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)
                    ),
                'duration':
                    '1 day, 0:10:00.006331',
            },
        ]
        actions_expected_runtime = {
            'action3': 100000.0,
            'action2': 100000.0,
            'action1': 100000.0
        }
        actual = check_tron_jobs.get_relevant_action(
            action_runs=action_runs,
            last_state=State.STUCK,
            actions_expected_runtime=actions_expected_runtime
        )
        assert_equal(actual["id"], "MASTER.test.1.action2")

# These tests test job has no scheduled run scenarios

    def test_no_job_scheduled_or_queuing(self):
        job_runs = {
            'status':
                'succeeded',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 300),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 900),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1200),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.NOT_SCHEDULED)

    def test_get_relevant_action_for_not_scheduled_state(self):
        action_runs = [
            {
                'id':
                    'MASTER.test.1.2',
                'state':
                    'succeeded',
                'end_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 600),
                    ),
            },
            {
                'id':
                    'MASTER.test.1.1',
                'state':
                    'succeeded',
                'end_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() - 1200),
                    ),
            },
        ]
        actual = check_tron_jobs.get_relevant_action(
            action_runs=action_runs,
            last_state=State.NOT_SCHEDULED,
            actions_expected_runtime={
                'action1': 86400.0,
                'action2': 86400.0,
                'action3': 86400.0,
            }
        )
        assert_equal(actual["id"], "MASTER.test.1.1")

# These tests test job without succeeded/failed run scenarios

    def test_job_waiting_for_first_run(self):
        job_runs = {
            'status':
                'scheduled',
            'next_run':
                None,
            'runs': [{
                'id':
                    'MASTER.test.1',
                'state':
                    'scheduled',
                'run_time':
                    time.strftime(
                        '%Y-%m-%d %H:%M:%S',
                        time.localtime(time.time() + 1200),
                    ),
                'end_time':
                    None,
            }, ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.WAITING_FOR_FIRST_RUN)

    def test_job_has_no_runs_at_all(self):
        job_runs = {
            'status': 'running',
            'next_run': None,
            'runs': [],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run, None)
        assert_equal(state, State.NO_RUN_YET)

# These tests test job/action unknown scenarios

    def test_job_unknown(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'unknown',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1200),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.UNKNOWN)

    def test_job_running_but_action_unknown_already(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id':
                        'MASTER.test.3',
                    'state':
                        'scheduled',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() + 600),
                        ),
                    'end_time':
                        None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                    'end_time':
                        None,
                    'runs': [
                        {
                            'id': 'MASTER.test.2.action2',
                            'state': 'running',
                        },
                        {
                            'id': 'MASTER.test.1.action1',
                            'state': 'unknown',
                        },
                    ],  # noqa: E122
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'run_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                    'end_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1700),
                        ),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.UNKNOWN)


# These tests test guess realert feature

    def test_guess_realert_every(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(time.time() + 600),
                ),
            'runs': [
                {
                    'id': 'MASTER.test.3',
                    'state': 'scheduled',
                    'start_time': None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'failed',
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                },
            ],  # noqa: E122
        }
        realert_every = check_tron_jobs.guess_realert_every(job_runs)
        assert_equal(realert_every, 4)

    def test_guess_realert_every_queue_job(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                None,
            'runs': [
                {
                    'id': 'MASTER.test.3',
                    'state': 'queued',
                    'start_time': None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'running',
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 600),
                        ),
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 1800),
                        ),
                },
            ],
        }
        realert_every = check_tron_jobs.guess_realert_every(job_runs)
        assert_equal(realert_every, -1)

    def test_guess_realert_every_frequent_run(self):
        job_runs = {
            'status':
                'running',
            'next_run':
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(time.time() + 10),
                ),
            'runs': [
                {
                    'id': 'MASTER.test.3',
                    'state': 'scheduled',
                    'start_time': None,
                },
                {
                    'id':
                        'MASTER.test.2',
                    'state':
                        'failed',
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 10),
                        ),
                },
                {
                    'id':
                        'MASTER.test.1',
                    'state':
                        'succeeded',
                    'start_time':
                        time.strftime(
                            '%Y-%m-%d %H:%M:%S',
                            time.localtime(time.time() - 20),
                        ),
                },
            ],  # noqa: E122
        }
        realert_every = check_tron_jobs.guess_realert_every(job_runs)
        assert_equal(realert_every, 1)

    def test_guess_realert_every_first_time_job(self):
        job_runs = {
            'status':
                'enabled',
            'next_run':
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(time.time() + 600),
                ),
            'runs': [{
                'id': 'MASTER.test.1',
                'state': 'scheduled',
                'start_time': None,
            }, ],
        }
        realert_every = check_tron_jobs.guess_realert_every(job_runs)
        assert_equal(realert_every, -1)
