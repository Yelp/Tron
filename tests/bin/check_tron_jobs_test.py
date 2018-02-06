from testify import assert_equal

def check_relevant_action_runs_picks_the_one_that_failed():
    action_runs = [
        {u'node': {u'username': u'batch', u'hostname': u'localhost', u'name': u'localhost', u'port': 22}, u'raw_command': u'/bin/false', u'requirements': [], u'run_num': u'582', u'exit_status': 1, u'stdout': None, u'start_time': u'2018-02-05 17:40:00', u'id': u'MASTER.kwatest.582.action1', u'action_name': u'action1', u'state': u'failed', u'command': u'/bin/false', u'end_time': u'2018-02-05 17:40:00', u'stderr': None, u'duration': u'0:00:00.065018', u'job_name': u'MASTER.kwatest'},
        {u'node': {u'username': u'batch', u'hostname': u'localhost', u'name': u'localhost', u'port': 22}, u'raw_command': u'/bin/true', u'requirements': [], u'run_num': u'582', u'exit_status': 0, u'stdout': None, u'start_time': u'2018-02-05 17:40:00', u'id': u'MASTER.kwatest.582.action2', u'action_name': u'action2', u'state': u'succeeded', u'command': u'/bin/true', u'end_time': u'2018-02-05 17:40:00', u'stderr': None, u'duration': u'0:00:00.046243', u'job_name': u'MASTER.kwatest'}
    ]
    actual = check_tron_jobs.get_relevant_action(action_runs)
    assert_equal(actual["status"],"failed")
