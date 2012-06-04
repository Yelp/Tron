from testify import TestCase, assert_equal, setup, run
from tests.assertions import assert_raises
from tron.utils.iteration import min_filter, max_filter, list_all

class FilterFuncTestCase(TestCase):

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


class FilteredMinTestCase(FilterFuncTestCase):

    @setup
    def setup_func(self):
        self.test_func = min_filter

    def test_min_filter(self):
        seq = [None, 2, None, 7, None, 9, 10, 12, 1]
        assert_equal(min_filter(seq), 1)


class FilteredMaxTestCase(FilterFuncTestCase):

    @setup
    def setup_func(self):
        self.test_func = max_filter

    def test_max_filter(self):
        seq = [None, 2, None, 7, None, 9, 10, 12, 1]
        assert_equal(max_filter(seq), 12)


class ListAllTestCase(TestCase):

    def test_all_true(self):
        assert list_all(range(1,5))

    def test_all_false(self):
        assert not list_all(0 for _ in xrange(7))

    def test_full_iteration(self):
        seq = iter([1, 0, 3, 0, 5])
        assert not list_all(seq)
        assert_raises(StopIteration, seq.next)

if __name__ == "__main__":
    run()