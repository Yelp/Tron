from testify import *

from tron import command_context

class EmptyContextTestCase(TestCase):
    @setup
    def build_context(self):
        self.context = command_context.CommandContext(None)

    def test(self):
        assert_raises(KeyError, self.context.__getitem__, 'foo')


class SimpleContextTestCaseBase(TestCase):
    __test__ = False

    def test_hit(self):
        assert_equal(self.context['foo'], 'bar')

    def test_miss(self):
        assert_raises(KeyError, self.context.__getitem__, 'your_mom')


class SimpleDictContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.context = command_context.CommandContext(dict(foo="bar"))


class SimpleObjectContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        class MyObject(object):
            pass
        obj = MyObject()
        obj.foo = "bar"
        self.context = command_context.CommandContext(obj)


class ChainedDictContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.next_context = command_context.CommandContext(dict(foo="bar"))
        self.context = command_context.CommandContext(dict(), self.next_context)


class ChainedDictOverrideContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.next_context = command_context.CommandContext(dict(foo="your mom"))
        self.context = command_context.CommandContext(dict(foo="bar"), self.next_context)


class ChainedObjectOverrideContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        class MyObject(object):
            pass
        obj = MyObject()
        obj.foo = "bar"

        self.next_context = command_context.CommandContext(dict(foo="your mom"))
        self.context = command_context.CommandContext(obj, self.next_context)


if __name__ == '__main__':
    run()