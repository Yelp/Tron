from testify import TestCase
from testify import assert_equal
from tron.utils import dicts


class InvertDictListTestCase(TestCase):

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
            3: ['e', 'f']
        }
        assert_equal(dicts.invert_dict_list(original), expected)