import os
import tempfile

from testify import teardown, setup, TestCase, run, assert_equal, assert_raises
from tests.assertions import assert_call
from tests.testingutils import Turtle

from tron.api.controller import JobController, ConfigController
from tron.config.config_parse import update_config, _initialize_original_config, ConfigError


class JobControllerTestCase(TestCase):

    @setup
    def setup_controller(self):
        self.jobs           = [Turtle(), Turtle(), Turtle()]
        self.mcp            = Turtle(get_jobs=lambda: self.jobs)
        self.controller     = JobController(self.mcp)

    def test_disable_all(self):
        self.controller.disable_all()
        for job in self.jobs:
            assert_call(job.disable, 0)

    def test_enable_all(self):
        self.controller.enable_all()
        for job in self.jobs:
            assert_call(job.enable, 0)


class ConfigControllerTestCase(TestCase):

    BASE_CONFIG = """
config_name: MASTER
nodes:
- {hostname: localhost, name: local}
ssh_options: {agent: true}
state_persistence: {name: state_data.shelve, store_type: shelve}
"""

    TEST_CONFIG_UPDATE = BASE_CONFIG + """
jobs:
- actions:
  - {command: echo 'Echo!', name: echo_action}
  - {command: 'echo ''Today is %(shortdate)s, which is the same as %(year)s-%(month)s-%(day)s''
      && false', name: another_echo_action}
  cleanup_action: {command: echo 'at last'}
  name: echo_job
  node: local
  schedule: interval 1 hour
"""

    TEST_CONFIG_RESULT = """MASTER:
  config_name: MASTER
  jobs:
  - actions:
    - {command: echo 'Echo!', name: echo_action}
    - {command: 'echo ''Today is %(shortdate)s, which is the same as %(year)s-%(month)s-%(day)s''
        && false', name: another_echo_action}
    cleanup_action: {command: echo 'at last'}
    name: echo_job
    node: local
    schedule: interval 1 hour
  nodes:
  - {hostname: localhost, name: local}
  ssh_options: {agent: true}
  state_persistence: {name: state_data.shelve, store_type: shelve}
"""

    @setup
    def setup_controller(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'test_config')
        self.controller = ConfigController(self.filename)

    @teardown
    def teardown_controller(self):
        try:
            os.unlink(self.filename)
        except OSError:
            pass

    def test_read_config(self):
        content = "12345"
        with open(self.filename, 'w') as fh:
            fh.write(content)

        assert_equal(self.controller.read_config(), content)

    def test_read_config_missing(self):
        self.controller.filepath = '/bogggusssss'
        assert not self.controller.read_config()

    def test_rewrite_config(self):
        assert self.controller.rewrite_config(self.TEST_CONFIG_UPDATE)
        assert_equal(self.controller.read_config(), self.TEST_CONFIG_RESULT)

    def test_rewrite_config_missing(self):
        self.controller.filepath = '/bogggusssss'
        assert not self.controller.rewrite_config(self.TEST_CONFIG_UPDATE)

    def test_missing_job_node(self):
        test_config = self.BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: bogussssss
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action:
            command: "test_command0.1"
            requires: [action0_0]
        """
        assert_raises(ConfigError, update_config, self.filename, test_config)

    def test_missing_service_node(self):
        test_config = self.BASE_CONFIG + """
services:
    -
        name: "test_job0"
        node: bogusssss
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action:
            command: "test_command0.1"
"""
        assert_raises(ConfigError, update_config, self.filename, test_config)


    def test_valid_original_config(self):
        test_config = self.BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
        """
        expected_result = {'MASTER':
                           {'nodes':
                            [{'hostname': 'localhost',
                              'name': 'local'}],
                            'config_name': 'MASTER',
                            'jobs':
                            [{'node': 'node0',
                              'name': 'test_job0',
                              'actions': None,
                              'schedule': 'interval 20s'}],
                            'ssh_options': {'agent': True},
                            'state_persistence': {'store_type': 'shelve',
                                                  'name': 'state_data.shelve'}}}
        fd = open(self.filename,'w')
        fd.write(test_config)
        fd.close()
        assert_equal(expected_result, _initialize_original_config(self.filename))

if __name__ == "__main__":
    run()
