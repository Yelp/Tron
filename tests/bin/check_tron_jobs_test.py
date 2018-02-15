from __future__ import absolute_import
from __future__ import unicode_literals

import check_tron_jobs
from testify import assert_equal
from testify import TestCase


class CheckJobsTestCase(TestCase):

    def test_check_relevant_action_runs_picks_the_one_that_failed(self):
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
        actual = check_tron_jobs.get_relevant_action(action_runs)
        print("%s", actual)
        assert_equal(actual["state"], "failed")

    def test_get_relevant_run_picks_the_one_that_queued(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'node': {'username': 'batch', 'hostname': 'localhost', 'name': 'localhost', 'port': 22}, 'raw_command': 'sleep 30m', 'requirements': [], 'run_num': '66', 'exit_status': None, 'stdout': None,
                    'start_time': None, 'id': 'MASTER.kwatest.66.purposestuck', 'action_name': 'purposestuck', 'state': 'queued', 'command': None, 'end_time': None, 'stderr': None, 'duration': '', 'job_name': 'MASTER.kwatest',
                },
                {
                    'node': {'username': 'batch', 'hostname': 'localhost', 'name': 'localhost', 'port': 22}, 'raw_command': 'sleep 30m', 'requirements': [], 'run_num': '65', 'exit_status': None, 'stdout': None, 'start_time': '2018-02-14 17:10:09',
                    'id': 'MASTER.kwatest.65.purposestuck', 'action_name': 'purposestuck', 'state': 'running', 'command': 'sleep 30m', 'end_time': None, 'stderr': None, 'duration': '0:04:34.912704', 'job_name': 'MASTER.kwatest',
                },
            ],
        }
        actual = check_tron_jobs.get_relevant_run(job_runs)
        print("%s", actual)
        assert_equal(actual["state"], "queued")

    def test_get_relevant_run_picks_the_one_that_cancelled(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'node': {'username': 'batch', 'hostname': 'localhost', 'name': 'localhost', 'port': 22}, 'raw_command': 'sleep 30m', 'requirements': [], 'run_num': '66', 'exit_status': None, 'stdout': None,
                    'start_time': None, 'id': 'MASTER.kwatest.66.purposestuck', 'action_name': 'purposestuck', 'state': 'cancelled', 'command': None, 'end_time': None, 'stderr': None, 'duration': '', 'job_name': 'MASTER.kwatest',
                },
                {
                    'node': {'username': 'batch', 'hostname': 'localhost', 'name': 'localhost', 'port': 22}, 'raw_command': 'sleep 30m', 'requirements': [], 'run_num': '65', 'exit_status': None, 'stdout': None, 'start_time': '2018-02-14 17:10:09',
                    'id': 'MASTER.kwatest.65.purposestuck', 'action_name': 'purposestuck', 'state': 'running', 'command': 'sleep 30m', 'end_time': None, 'stderr': None, 'duration': '0:04:34.912704', 'job_name': 'MASTER.kwatest',
                },
            ],
        }
        actual = check_tron_jobs.get_relevant_run(job_runs)
        print("%s", actual)
        assert_equal(actual["state"], "cancelled")
