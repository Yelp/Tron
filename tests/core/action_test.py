from __future__ import absolute_import
from __future__ import unicode_literals

from testify import assert_equal
from testify import run
from testify import setup
from testify import TestCase

from tron.config import config_utils
from tron.core import action
from tron.core.action import Constraint
from tron.core.action import Volume


class TestAction(TestCase):
    @setup
    def setup_action(self):
        self.config_context = config_utils.ConfigContext(
            'config',
            ['localhost'],
            ['cluster'],
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

    def test_from_config_full(self):
        config = dict(
            name="ted",
            command="do something",
            node="first",
            executor="ssh",
            cpus=1,
            mem=100,
            constraints=[
                Constraint(attribute='pool', operator='LIKE', value='default'),
            ],
            docker_image='fake-docker.com:400/image',
            docker_parameters=[
                dict(key='test', value=123),
            ],
            env={'TESTING': 'true'},
            extra_volumes=[
                Volume(
                    host_path='/tmp', container_path='/nail/tmp', mode='RO'
                ),
            ],
            mesos_address='fake-mesos-master.com',
        )
        new_action = action.Action.from_config(config, self.config_context)
        assert_equal(new_action.name, config['name'])
        assert_equal(new_action.command, config['command'])
        assert_equal(new_action.node_pool, None)
        assert_equal(list(new_action.required_actions), [])
        assert_equal(
            new_action.executor, action.ExecutorTypes(config['executor'])
        )
        assert_equal(new_action.cpus, config['cpus'])
        assert_equal(new_action.mem, config['mem'])
        assert_equal(
            new_action.constraints, [
                Constraint(attribute='pool', operator='LIKE', value='default'),
            ]
        )
        assert_equal(new_action.docker_image, config['docker_image'])
        assert_equal(
            new_action.docker_parameters,
            [dict(key='test', value=123)],
        )
        assert_equal(new_action.env, config['env'])
        assert_equal(
            new_action.extra_volumes,
            [Volume(container_path='/nail/tmp', host_path='/tmp', mode='RO')],
        )
        assert_equal(new_action.mesos_address, config['mesos_address'])

    def test_from_config_none_values(self):
        config = dict(
            name="ted",
            command="do something",
            node="first",
            executor="ssh",
        )
        new_action = action.Action.from_config(config, self.config_context)
        assert_equal(new_action.name, config['name'])
        assert_equal(new_action.command, config['command'])
        assert_equal(list(new_action.required_actions), [])
        assert_equal(
            new_action.executor, action.ExecutorTypes(config['executor'])
        )
        assert_equal(new_action.constraints, [])
        assert_equal(new_action.docker_image, None)
        assert_equal(new_action.docker_parameters, [])
        assert_equal(new_action.env, {})
        assert_equal(new_action.extra_volumes, [])

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
