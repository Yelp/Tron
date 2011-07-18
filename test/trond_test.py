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
                command: "echo 'Echo again!'" """


class BasicTronTestCase(TronTestCase):

    def test_end_to_end_basic(self):
        self.save_config(SINGLE_ECHO_CONFIG)
        self.start_trond()
        assert_equal(self.get_config(), SINGLE_ECHO_CONFIG)
        self.upload_config(DOUBLE_ECHO_CONFIG)
        assert_equal(self.get_config(), DOUBLE_ECHO_CONFIG)
