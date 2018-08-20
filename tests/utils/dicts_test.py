from __future__ import absolute_import
from __future__ import unicode_literals

from testifycompat import assert_equal
from testifycompat import TestCase
from tron.utils import dicts


class TestInvertDictList(TestCase):
    def test_invert_dict_list(self):
        expected = {
            'a': 1,
            'b': 1,
            'c': 1,
            'd': 2,
            'e': 3,
            'f': 3,
        }
        original = {
            1: ['a', 'b', 'c'],
            2: ['d'],
            3: ['e', 'f'],
        }
        assert_equal(dicts.invert_dict_list(original), expected)


class TestGetDeep(TestCase):

    data = {'foo': 23, 'bar': {'car': 'hello'}}

    def test_get_deep_one_key(self):
        assert_equal(
            dicts.get_deep(self.data, 'foo'),
            23,
        )

    def test_get_deep_two_keys(self):
        assert_equal(
            dicts.get_deep(self.data, 'bar.car'),
            'hello',
        )

    def test_get_deep_missing(self):
        assert_equal(
            dicts.get_deep(self.data, 'bar.baz'),
            None,
        )

    def test_get_deep_missing_first_key(self):
        assert_equal(
            dicts.get_deep(self.data, 'other.car'),
            None,
        )

    def test_get_deep_default(self):
        assert_equal(
            dicts.get_deep(self.data, 'other', 'custom_default'),
            'custom_default',
        )
