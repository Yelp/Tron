import mock
from testify import TestCase, run, assert_equal, setup
from testify.assertions import assert_in, assert_raises
from tron.config import config_utils, ConfigError
from tron.config.config_utils import build_list_of_type_validator, ConfigContext
from tron.config.config_utils import valid_identifier

class UniqueNameDictTestCase(TestCase):

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


class ValidatorIdentifierTestCase(TestCase):

    def test_valid_identifier_too_long(self):
        assert_raises(ConfigError, valid_identifier, 'a' * 256, mock.Mock())

    def test_valid_identifier(self):
        name = 'avalidname'
        assert_equal(name, valid_identifier(name, mock.Mock()))

    def test_valid_identifier_invalid_character(self):
        for name in ['invalid space', '*name', '1numberstarted', 123, '']:
            assert_raises(ConfigError, valid_identifier, name, mock.Mock())


class BuildListOfTypeValidatorTestCase(TestCase):

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


class ValidTimeTestCase(TestCase):

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
        assert_raises(ConfigError, config_utils.valid_time,
            "14:32:12:34", self.context)
        assert_raises(ConfigError, config_utils.valid_time, None, self.context)


class ConfigContextTestCase(TestCase):

    def test_build_config_context(self):
        path, nodes, namespace = 'path', set([1,2,3]), 'namespace'
        command_context = mock.MagicMock()
        parent_context = config_utils.ConfigContext(
            path, nodes, command_context, namespace)

        child = parent_context.build_child_context('child')
        assert_equal(child.path, '%s.child' % path)
        assert_equal(child.nodes, nodes)
        assert_equal(child.namespace, namespace)
        assert_equal(child.command_context, command_context)
        assert not child.partial


if __name__ == "__main__":
    run()