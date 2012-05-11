from testify import TestCase, setup, assert_raises, assert_equal, run

from tron import command_context

class EmptyContextTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = command_context.CommandContext(None)

    def test(self):
        assert_raises(KeyError, self.context.__getitem__, 'foo')

    def test_get(self):
        assert not self.context.get('foo')


class SimpleContextTestCaseBase(TestCase):
    __test__ = False

    def test_hit(self):
        assert_equal(self.context['foo'], 'bar')

    def test_miss(self):
        assert_raises(KeyError, self.context.__getitem__, 'your_mom')

    def test_get_hit(self):
        assert_equal(self.context.get('foo'), 'bar')

    def test_get_miss(self):
        assert not self.context.get('your_mom')


class SimpleDictContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.context = command_context.CommandContext(dict(foo='bar'))


class SimpleObjectContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        class MyObject(object):
            pass
        obj = MyObject()
        obj.foo = 'bar'
        self.context = command_context.CommandContext(obj)


class ChainedDictContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.next_context = command_context.CommandContext(
                dict(foo='bar', next_foo='next_bar'))
        self.context = command_context.CommandContext(dict(), self.next_context)

    def test_chain_get(self):
        assert_equal(self.context['next_foo'], 'next_bar')


class ChainedDictOverrideContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.next_context = command_context.CommandContext(
                dict(foo='your mom', next_foo='next_bar'))
        self.context = command_context.CommandContext(
                dict(foo='bar'), self.next_context)

    def test_chain_get(self):
        assert_equal(self.context['next_foo'], 'next_bar')

class ChainedObjectOverrideContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        class MyObject(object):
            pass
        obj = MyObject()
        obj.foo = 'bar'

        self.next_context = command_context.CommandContext(
                dict(foo='your mom', next_foo='next_bar'))
        self.context = command_context.CommandContext(obj, self.next_context)

    def test_chain_get(self):
        assert_equal(self.context['next_foo'], 'next_bar')


if __name__ == '__main__':
    run()