from __future__ import absolute_import
from __future__ import unicode_literals

import time

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

    def test_is_job_no_stuck(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.2', 'state': 'scheduled', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 600)),
                },
                {
                    'id': 'MASTER.test.1', 'state': 'running', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
            ],
        }
        assert_equal(check_tron_jobs.is_job_stuck(job_runs), False)

    def test_is_job_stuck(self):
        job_runs = {
            'status': 'running', 'next_run': None, 'runs': [
                {
                    'id': 'MASTER.test.1', 'state': 'queued', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 600)),
                },
                {
                    'id': 'MASTER.test.2', 'state': 'running', 'run_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 1200)),
                },
            ],
        }
        assert_equal(check_tron_jobs.is_job_stuck(job_runs), True)
