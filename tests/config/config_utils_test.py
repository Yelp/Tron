from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import mock
from testify import assert_equal
from testify import TestCase
from testify.assertions import assert_in

from tests.assertions import assert_raises
from tron.config import config_utils
from tron.config import ConfigError
from tron.config import schema


class ValidTimeTestCase(TestCase):
    def test_valid_time(self):
        time_spec = config_utils.valid_time("14:32")
        assert_equal(time_spec.hour, 14)
        assert_equal(time_spec.minute, 32)
        assert_equal(time_spec.second, 0)

    def test_valid_time_with_seconds(self):
        time_spec = config_utils.valid_time("14:32:12")
        assert_equal(time_spec.hour, 14)
        assert_equal(time_spec.minute, 32)
        assert_equal(time_spec.second, 12)

    def test_valid_time_invalid(self):
        assert_raises(
            ValueError,
            config_utils.valid_time,
            "14:32:12:34",
        )
        assert_raises(ValueError, config_utils.valid_time, None)


class ValidTimeDeltaTestCase(TestCase):
    def test_valid_time_delta_invalid(self):
        exception = assert_raises(
            ConfigError, config_utils.valid_time_delta, 'no time'
        )
        assert_in('not a valid time delta', str(exception))

    def test_valid_time_delta_valid_seconds(self):
        for jitter in [' 82s ', '82 s', '82 sec', '82seconds  ']:
            delta = datetime.timedelta(seconds=82)
            assert_equal(delta, config_utils.valid_time_delta(jitter))

    def test_valid_time_delta_valid_minutes(self):
        for jitter in ['10m', '10 m', '10   min', '  10minutes']:
            delta = datetime.timedelta(seconds=600)
            assert_equal(delta, config_utils.valid_time_delta(jitter))

    def test_valid_time_delta_invalid_unit(self):
        for jitter in ['1 year', '3 mo', '3 months']:
            assert_raises(ConfigError, config_utils.valid_time_delta, jitter)


class ConfigContextTestCase(TestCase):
    def test_build_config_context(self):
        path, nodes, namespace = 'path', {1, 2, 3}, 'namespace'
        command_context = mock.MagicMock()
        parent_context = config_utils.ConfigContext(
            path,
            nodes,
            command_context,
            namespace,
        )

        child = parent_context.build_child_context('child')
        assert_equal(child.path, '%s.child' % path)
        assert_equal(child.nodes, nodes)
        assert_equal(child.namespace, namespace)
        assert_equal(child.command_context, command_context)
        assert not child.partial


StubConfigObject = schema.config_object_factory(
    'StubConfigObject',
    ['req1', 'req2'],
    ['opt1', 'opt2'],
)
