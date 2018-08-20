from __future__ import absolute_import
from __future__ import unicode_literals

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_raises
from tron.utils.iteration import list_all
from tron.utils.iteration import max_filter
from tron.utils.iteration import min_filter


class TestFilterFunc(TestCase):

    __test__ = False

    @setup
    def setup_seq(self):
        self.test_func = None

    def test_filter_empty_seq(self):
        assert_equal(self.test_func([]), None)

    def test_filter_all_nones(self):
        assert_equal(self.test_func([None, None, None]), None)

    def test_filter_none(self):
        assert_equal(self.test_func(None), None)

    def test_filter_single_item(self):
        assert_equal(self.test_func([1]), 1)

    def test_filter_single_item_with_nones(self):
        assert_equal(self.test_func([None, 4, None, None]), 4)


class TestFilteredMin(TestFilterFunc):
    @setup
    def setup_func(self):
        self.test_func = min_filter

    def test_min_filter(self):
        seq = [None, 2, None, 7, None, 9, 10, 12, 1]
        assert_equal(min_filter(seq), 1)


class TestFilteredMax(TestFilterFunc):
    @setup
    def setup_func(self):
        self.test_func = max_filter

    def test_max_filter(self):
        seq = [None, 2, None, 7, None, 9, 10, 12, 1]
        assert_equal(max_filter(seq), 12)


class TestListAll(TestCase):
    def test_all_true(self):
        assert list_all(range(1, 5))

    def test_all_false(self):
        assert not list_all(0 for _ in range(7))

    def test_full_iteration(self):
        seq = iter([1, 0, 3, 0, 5])
        assert not list_all(seq)
        assert_raises(StopIteration, lambda: next(seq))


if __name__ == "__main__":
    run()
