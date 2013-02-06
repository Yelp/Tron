import mock
from testify import TestCase, run, assert_equal, setup
from testify.assertions import assert_in, assert_raises
from tron.config import config_utils, ConfigError

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


class ConfigContextTestCase(TestCase):

    def test_build_config_context(self):
        path, nodes, namespace = 'path', set([1,2,3]), 'namespace'
        command_context, local = mock.MagicMock(), True
        parent_context = config_utils.ConfigContext(
            path, nodes, command_context, namespace, local)

        child = parent_context.build_child_context('child')
        assert_equal(child.path, '%s.child' % path)
        assert_equal(child.nodes, nodes)
        assert_equal(child.namespace, namespace)
        assert_equal(child.command_context, command_context)
        assert_equal(child.local, local)


if __name__ == "__main__":
    run()