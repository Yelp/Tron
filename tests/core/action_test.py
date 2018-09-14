from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron import node
from tron.config.schema import ConfigAction
from tron.config.schema import ConfigConstraint
from tron.config.schema import ConfigParameter
from tron.config.schema import ConfigVolume
from tron.core import action


class TestAction(TestCase):
    @setup
    def setup_action(self):
        self.node_pool = mock.create_autospec(node.NodePool)
        self.action = action.Action("my_action", "doit", self.node_pool)

    def test_from_config_full(self):
        config = ConfigAction(
            name="ted",
            command="do something",
            node="first",
            executor="ssh",
            cpus=1,
            mem=100,
            constraints=[
                ConfigConstraint(
                    attribute='pool',
                    operator='LIKE',
                    value='default',
                ),
            ],
            docker_image='fake-docker.com:400/image',
            docker_parameters=[
                ConfigParameter(
                    key='test',
                    value=123,
                ),
            ],
            env={'TESTING': 'true'},
            extra_volumes=[
                ConfigVolume(
                    host_path='/tmp',
                    container_path='/nail/tmp',
                    mode='RO',
                ),
            ],
            trigger_downstreams=True,
            triggered_by=["foo.bar"],
        )
        new_action = action.Action.from_config(config)
        assert_equal(new_action.name, config.name)
        assert_equal(new_action.command, config.command)
        assert_equal(new_action.node_pool, None)
        assert_equal(new_action.required_actions, [])
        assert_equal(new_action.executor, config.executor)
        assert_equal(new_action.cpus, config.cpus)
        assert_equal(new_action.mem, config.mem)
        assert_equal(new_action.constraints, [['pool', 'LIKE', 'default']])
        assert_equal(new_action.docker_image, config.docker_image)
        assert_equal(
            new_action.docker_parameters,
            [{
                'key': 'test',
                'value': 123
            }],
        )
        assert_equal(new_action.env, config.env)
        assert_equal(
            new_action.extra_volumes,
            [{
                'container_path': '/nail/tmp',
                'host_path': '/tmp',
                'mode': 'RO'
            }],
        )
        assert new_action.trigger_downstreams is True
        assert new_action.triggered_by == ['foo.bar']

    def test_from_config_none_values(self):
        config = ConfigAction(
            name="ted",
            command="do something",
            node="first",
            executor="ssh",
        )
        new_action = action.Action.from_config(config)
        assert_equal(new_action.name, config.name)
        assert_equal(new_action.command, config.command)
        assert_equal(new_action.required_actions, [])
        assert_equal(new_action.executor, config.executor)
        assert_equal(new_action.constraints, [])
        assert_equal(new_action.docker_image, None)
        assert_equal(new_action.docker_parameters, [])
        assert_equal(new_action.env, {})
        assert_equal(new_action.extra_volumes, [])

    def test__eq__(self):
        new_action = action.Action(
            self.action.name,
            self.action.command,
            self.node_pool,
        )
        assert_equal(new_action, self.action)


if __name__ == '__main__':
    run()
