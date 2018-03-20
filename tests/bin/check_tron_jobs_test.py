from __future__ import absolute_import
from __future__ import unicode_literals

import time

import check_tron_jobs
from check_tron_jobs import State
from mock import patch
from mock import PropertyMock
from testify import assert_equal
from testify import TestCase


class CheckJobsTestCase(TestCase):

    @patch('check_tron_jobs.check_job_result')
    @patch('check_tron_jobs.Client')
    @patch('check_tron_jobs.cmd_utils')
    @patch('check_tron_jobs.parse_cli')
    def test_check_job_result_exception(self, mock_args, mock_cmd_utils, mock_client, mock_check_job_result):
        type(mock_args.return_value).job = PropertyMock(return_value=None)
        mock_client.return_value.jobs.return_value = [
            'job1', 'job2', 'job3',
        ]
        mock_check_job_result.side_effect = [
            KeyError('foo'), None, TypeError,
        ]
        error_code = check_tron_jobs.main()
        assert_equal(error_code, 1)
        assert_equal(mock_check_job_result.call_count, 3)

    def test_get_relevant_action_picks_the_one_that_failed(self):
        action_runs = [
            {
                'node': {'username': 'batch', 'hostname': 'localhost', 'name': 'localhost', 'port': 22}, 'raw_command': '/bin/false', 'requirements': [], 'run_num': '582', 'exit_status': 1, 'stdout': None, 'start_time': '2018-02-05 17:40:00',
                'id': 'MASTER.kwatest.582.action1', 'action_name': 'action1', 'state': 'failed', 'command': '/bin/false', 'end_time': '2018-02-05 17:40:00', 'stderr': None, 'duration': '0:00:00.065018', 'job_name': 'MASTER.kwatest',
            },
            {
                'node': {'username': 'batch', 'hostname': 'localhost', 'name': 'localhost', 'port': 22}, 'raw_command': '/bin/true', 'requirements': [], 'run_num': '582', 'exit_status': 0, 'stdout': None, 'start_time': '2018-02-05 17:40:00',
                'id': 'MASTER.kwatest.582.action2', 'action_name': 'action2', 'state': 'succeeded', 'command': '/bin/true', 'end_time': '2018-02-05 17:40:00', 'stderr': None, 'duration': '0:00:00.046243', 'job_name': 'MASTER.kwatest',
            },
        ]
        actual = check_tron_jobs.get_relevant_action(action_runs, State.FAILED)
        assert_equal(actual["state"], "failed")

    def test_get_relevant_action_picks_the_first_one_succeeded(self):
        action_runs = [
            {
                'id': 'MASTER.test.action1', 'action_name': 'action1', 'state': 'succeeded', 'start_time': time.localtime(time.time() - 1200),
            },
            {
                'id': 'MASTER.test.action2', 'action_name': 'action2', 'state': 'succeeded', 'start_time': time.localtime(time.time() - 600),
            },
            {
                'id': 'MASTER.test.action1', 'action_name': 'action1', 'state': 'succeeded', 'start_time': time.localtime(time.time()),
            },
        ]
        actual = check_tron_jobs.get_relevant_action(
            action_runs, State.SUCCEEDED,
        )
        assert_equal(actual["id"], "MASTER.test.action1")

    def test_get_relevant_action_pick_the_one_stuck(self):
        action_runs = [
            {
                'id': 'MASTER.test.action1', 'action_name': 'action1', 'state': 'succeeded', 'start_time': time.localtime(time.time() - 1200),
            },
            {
                'id': 'MASTER.test.action2', 'action_name': 'action2', 'state': 'running', 'start_time': time.localtime(time.time() - 1100),
            },
            {
                'id': 'MASTER.test.action1', 'action_name': 'action1', 'state': 'succeeded', 'start_time': time.localtime(time.time() - 1000),
            },
        ]
        actual = check_tron_jobs.get_relevant_action(action_runs, State.STUCK)
        assert_equal(actual["id"], "MASTER.test.action2")

    def test_job_succeeded(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.3', 'state': 'scheduled', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 600)),
                },
                {
                    'id': 'MASTER.test.2', 'state': 'running', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
                {
                    'id': 'MASTER.test.1', 'state': 'succeeded', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 1800)),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.SUCCEEDED)

    def test_job_failed(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.3', 'state': 'scheduled', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 600)),
                },
                {
                    'id': 'MASTER.test.2', 'state': 'running', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
                {
                    'id': 'MASTER.test.1', 'state': 'failed', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 1800)),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.FAILED)

    def test_job_no_run_yet(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run, None)
        assert_equal(state, State.NO_RUN_YET)

    def test_job_stuck(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.2', 'state': 'queued', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
                {
                    'id': 'MASTER.test.1', 'state': 'running', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 1200)),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.STUCK)

    def test_job_stuck_when_runtime_not_sorted(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.2', 'state': 'running', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
                {
                    'id': 'MASTER.test.1', 'state': 'scheduled', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.STUCK)

    def test_no_job_scheduled_or_queuing(self):
        job_runs = {
            'status': 'succeeded', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.2', 'state': 'succeeded', 'end_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
                {
                    'id': 'MASTER.test.1', 'state': 'succeeded', 'end_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 1200)),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.2')
        assert_equal(state, State.NOT_SCHEDULED)

    def test_job_waiting_for_first_run(self):
        job_runs = {
            'status': 'scheduled', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.1', 'state': 'scheduled', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 1200)),
                },
            ],
        }
        run, state = check_tron_jobs.get_relevant_run_and_state(job_runs)
        assert_equal(run['id'], 'MASTER.test.1')
        assert_equal(state, State.WAITING_FOR_FIRST_RUN)
