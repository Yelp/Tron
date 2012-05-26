from testify import TestCase, assert_equal, setup, run
from tron.utils.iteration import min_filter, max_filter

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


if __name__ == "__main__":
    run()