from __future__ import absolute_import
from __future__ import unicode_literals

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import assert_raises
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron.utils.proxy import AttributeProxy
from tron.utils.proxy import CollectionProxy


class DummyTarget(object):
    def __init__(self, v):
        self.v = v

    def foo(self):
        return self.v

    @property
    def not_foo(self):
        return not self.v

    def equals(self, b, sometimes=False):
        if sometimes:
            return 'sometimes'
        return self.v == b


class DummyObject(object):
    def __init__(self, proxy):
        self.proxy = proxy

    def __getattr__(self, item):
        return self.proxy.perform(item)


class TestCollectionProxy(TestCase):
    @setup
    def setup_proxy(self):
        self.target_list = [DummyTarget(1), DummyTarget(2), DummyTarget(0)]
        self.proxy = CollectionProxy(
            lambda: self.target_list,
            [
                ('foo', any, True),
                ('not_foo', all, False),
                ('equals', lambda a: list(a), True),
            ],
        )
        self.dummy = DummyObject(self.proxy)

    def test_add(self):
        self.proxy.add('foo', any, True)
        assert_equal(self.proxy._defs['foo'], (any, True))

    def test_perform(self):
        assert self.dummy.foo()
        assert not self.dummy.not_foo

    def test_perform_not_defined(self):
        assert_raises(AttributeError, self.dummy.proxy.perform, 'bar')

    def test_perform_with_params(self):
        assert_equal(self.proxy.perform('equals')(2), [False, True, False])
        sometimes = ['sometimes'] * 3
        assert_equal(
            self.proxy.perform('equals')(3, sometimes=True),
            sometimes,
        )


class TestAttributeProxy(TestCase):
    @setup
    def setup_proxy(self):
        self.target = DummyTarget(1)
        self.proxy = AttributeProxy(self.target, ['foo', 'not_foo'])
        self.dummy = DummyObject(self.proxy)

    def test_add(self):
        self.proxy.add('bar')
        assert_in('bar', self.proxy._attributes)

    def test_perform(self):
        assert_equal(self.dummy.foo(), 1)
        assert_equal(self.dummy.not_foo, False)

    def test_perform_not_defined(self):
        assert_raises(AttributeError, self.dummy.proxy.perform, 'zzz')


if __name__ == "__main__":
    run()
