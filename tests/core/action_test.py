from __future__ import absolute_import
from __future__ import unicode_literals

from testify import assert_equal
from testify import run
from testify import setup
from testify import TestCase

from tron.config import config_utils
from tron.core import action


class TestAction(TestCase):
    @setup
    def setup_action(self):
        self.config_context = config_utils.ConfigContext(
            'config',
            ['localhost'],
            ['cluster'],
            None,
            None,
        )
        self.node_pool = 'node_pool'
        self.action = action.Action.from_config(
            {
                'name': "my_action",
                'command': "doit",
                'node_pool': self.node_pool,
            },
            self.config_context,
        )

    def test_from_config(self):
        config = {
            'name': "ted",
            'command': "do something",
            'node': "first",
            'executor': "ssh",
            'cluster': "cluster",
            'pool': "default",
            'cpus': 1,
            'mem': 100,
            'service': "bar",
            'deploy_group': "test",
        }
        new_action = action.Action.from_config(config, self.config_context)
        assert_equal(new_action.name, config['name'])
        assert_equal(new_action.cpus, float(config['cpus']))
        assert_equal(list(new_action.required_actions), [])

    def test__eq__(self):
        new_action = action.Action.from_config(
            {
                'name': self.action.name,
                'command': self.action.command,
                'node_pool': self.node_pool,
            },
            self.config_context,
        )
        assert_equal(new_action, self.action)


if __name__ == '__main__':
    run()
