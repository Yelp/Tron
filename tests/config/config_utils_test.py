from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import mock

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_raises
from tron.config import config_utils
from tron.config import ConfigError
from tron.config import schema
from tron.config.config_utils import build_list_of_type_validator
from tron.config.config_utils import ConfigContext
from tron.config.config_utils import valid_identifier


class TestUniqueNameDict(TestCase):
    @setup
    def setup_dict(self):
        self.msg = "The key %s was there."
        self.dict = config_utils.UniqueNameDict(self.msg)

    def test_set_item_no_conflict(self):
        self.dict['a'] = 'something'
        assert_in('a', self.dict)

    def test_set_item_conflict(self):
        self.dict['a'] = 'something'
        assert_raises(ConfigError, self.dict.__setitem__, 'a', 'next_thing')


class TestValidatorIdentifier(TestCase):
    def test_valid_identifier_too_long(self):
        assert_raises(ConfigError, valid_identifier, 'a' * 256, mock.Mock())

    def test_valid_identifier(self):
        name = 'avalidname'
        assert_equal(name, valid_identifier(name, mock.Mock()))

    def test_valid_identifier_invalid_character(self):
        for name in ['invalid space', '*name', '1numberstarted', 123, '']:
            assert_raises(ConfigError, valid_identifier, name, mock.Mock())


class TestBuildListOfTypeValidator(TestCase):
    @setup
    def setup_validator(self):
        self.item_validator = mock.Mock()
        self.validator = build_list_of_type_validator(self.item_validator)

    def test_validator_passes(self):
        items, context = ['one', 'two'], mock.create_autospec(ConfigContext)
        self.validator(items, context)
        expected = [mock.call(item, context) for item in items]
        assert_equal(self.item_validator.mock_calls, expected)

    def test_validator_fails(self):
        self.item_validator.side_effect = ConfigError
        items, context = ['one', 'two'], mock.create_autospec(ConfigContext)
        assert_raises(ConfigError, self.validator, items, context)


class TestBuildEnumValidator(TestCase):
    @setup
    def setup_enum_validator(self):
        self.enum = dict(a=1, b=2)
        self.validator = config_utils.build_enum_validator(self.enum)
        self.context = config_utils.NullConfigContext

    def test_validate(self):
        assert_equal(self.validator('a', self.context), 'a')
        assert_equal(self.validator('b', self.context), 'b')

    def test_invalid(self):
        exception = assert_raises(
            ConfigError,
            self.validator,
            'c',
            self.context,
        )
        assert_in(
            'Value at  is not in %s: ' % str(set(self.enum)),
            str(exception),
        )


class TestValidTime(TestCase):
    @setup
    def setup_config(self):
        self.context = config_utils.NullConfigContext

    def test_valid_time(self):
        time_spec = config_utils.valid_time("14:32", self.context)
        assert_equal(time_spec.hour, 14)
        assert_equal(time_spec.minute, 32)
        assert_equal(time_spec.second, 0)

    def test_valid_time_with_seconds(self):
        time_spec = config_utils.valid_time("14:32:12", self.context)
        assert_equal(time_spec.hour, 14)
        assert_equal(time_spec.minute, 32)
        assert_equal(time_spec.second, 12)

    def test_valid_time_invalid(self):
        assert_raises(
            ConfigError,
            config_utils.valid_time,
            "14:32:12:34",
            self.context,
        )
        assert_raises(ConfigError, config_utils.valid_time, None, self.context)


class TestValidTimeDelta(TestCase):
    @setup
    def setup_config(self):
        self.context = config_utils.NullConfigContext

    def test_valid_time_delta_invalid(self):
        exception = assert_raises(
            ConfigError,
            config_utils.valid_time_delta,
            'no time',
            self.context,
        )
        assert_in('not a valid time delta: no time', str(exception))

    def test_valid_time_delta_valid_seconds(self):
        for jitter in [' 82s ', '82 s', '82 sec', '82seconds  ']:
            delta = datetime.timedelta(seconds=82)
            assert_equal(
                delta,
                config_utils.valid_time_delta(
                    jitter,
                    self.context,
                ),
            )

    def test_valid_time_delta_valid_minutes(self):
        for jitter in ['10m', '10 m', '10   min', '  10minutes']:
            delta = datetime.timedelta(seconds=600)
            assert_equal(
                delta,
                config_utils.valid_time_delta(
                    jitter,
                    self.context,
                ),
            )

    def test_valid_time_delta_invalid_unit(self):
        for jitter in ['1 year', '3 mo', '3 months']:
            assert_raises(
                ConfigError,
                config_utils.valid_time_delta,
                jitter,
                self.context,
            )


class TestConfigContext(TestCase):
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


class StubValidator(config_utils.Validator):
    config_class = StubConfigObject


class TestValidator(TestCase):
    @setup
    def setup_validator(self):
        self.validator = StubValidator()

    def test_validate_with_none(self):
        expected_msg = "A StubObject is required"
        exception = assert_raises(
            ConfigError,
            self.validator.validate,
            None,
            config_utils.NullConfigContext,
        )
        assert_in(expected_msg, str(exception))

    def test_validate_optional_with_none(self):
        self.validator.optional = True
        config = self.validator.validate(None, config_utils.NullConfigContext)
        assert_equal(config, None)


if __name__ == "__main__":
    run()
