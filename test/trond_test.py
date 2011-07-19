import datetime
import os
from testify import *
import time
import yaml

from test.trontestcase import TronTestCase


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
                command: "echo 'Today is %(shortdate)s' && false" """


class BasicTronTestCase(TronTestCase):

    def test_end_to_end_basic(self):
        # start with a basic configuration
        self.save_config(SINGLE_ECHO_CONFIG)
        self.start_trond()
        # make sure it got in
        assert_equal(self.get_config(), SINGLE_ECHO_CONFIG)

        # reconfigure and confirm results
        self.upload_config(DOUBLE_ECHO_CONFIG)
        assert_equal(self.list_events()['data'][0]['name'], 'reconfig')
        assert_equal(self.get_config(), DOUBLE_ECHO_CONFIG)
        assert_equal(self.list_all(),
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
        self.ctl('start', 'echo_job')
        # no good way to ensure that it completes before it is checked
        time.sleep(2)
        assert_equal(self.list_action_run('echo_job', 2, 'echo_action')['state'], 'SUCC')
        assert_equal(self.list_action_run('echo_job', 2, 'echo_action')['stdout'], ['Echo!'])
        assert_equal(self.list_action_run('echo_job', 2, 'another_echo_action')['state'], 'FAIL')
        assert_equal(self.list_action_run('echo_job', 2, 'another_echo_action')['stdout'],
                     [datetime.datetime.now().strftime('Today is %Y-%m-%d')])
        assert_equal(self.list_job_run('echo_job', 2)['state'], 'FAIL')

    def test_tronview_basic(self):
        self.save_config(SINGLE_ECHO_CONFIG)
        self.start_trond()

        assert_equal(self.tronview()[0], """Services:
No services

Jobs:
Name     State      Scheduler            Last Success        
echo_job ENABLED    INTERVAL:1:00:00     None                
""")

    def test_tronctl_basic(self):
        self.save_config(SINGLE_ECHO_CONFIG)
        self.start_trond()

        # run the job and check its output
        self.tronctl(['start', 'echo_job'])
        # no good way to ensure that it completes before it is checked
        time.sleep(2)
        assert_equal(self.list_action_run('echo_job', 1, 'echo_action')['state'], 'SUCC')
        assert_equal(self.list_job_run('echo_job', 1)['state'], 'SUCC')
