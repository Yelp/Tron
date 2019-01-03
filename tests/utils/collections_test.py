import mock

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import assert_not_in
from testifycompat import assert_raises
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_mock_calls
from tests.testingutils import autospec_method
from tron.utils import collections


class TestMappingCollections(TestCase):
    @setup
    def setup_collection(self):
        self.name = 'some_name'
        self.collection = collections.MappingCollection(self.name)

    def test_filter_by_name(self):
        autospec_method(self.collection.remove)
        self.collection.update(dict.fromkeys(['c', 'd', 'e']))
        self.collection.filter_by_name(['a', 'c'])
        expected = [mock.call(name) for name in ['d', 'e']]
        assert_mock_calls(expected, self.collection.remove.mock_calls)

    def test_remove_missing(self):
        assert_raises(ValueError, self.collection.remove, 'name')

    def test_remove(self):
        name = 'the_name'
        self.collection[name] = item = mock.Mock()
        self.collection.remove(name)
        assert_not_in(name, self.collection)
        item.disable.assert_called_with()

    def test_contains_item_false(self):
        mock_item, mock_func = mock.Mock(), mock.Mock()
        assert not self.collection.contains_item(mock_item, mock_func)
        assert not mock_func.mock_calls

    def test_contains_item_not_equal(self):
        mock_item, mock_func = mock.Mock(), mock.Mock()
        self.collection[mock_item.get_name()] = 'other item'
        result = self.collection.contains_item(mock_item, mock_func)
        assert_equal(result, mock_func.return_value)
        mock_func.assert_called_with(mock_item)

    def test_contains_item_true(self):
        mock_item, mock_func = mock.Mock(), mock.Mock()
        self.collection[mock_item.get_name()] = mock_item
        assert self.collection.contains_item(mock_item, mock_func)

    def test_add_contains(self):
        autospec_method(self.collection.contains_item)
        item, update_func = mock.Mock(), mock.Mock()
        assert not self.collection.add(item, update_func)
        assert_not_in(item.get_name(), self.collection)

    def test_add_new(self):
        autospec_method(self.collection.contains_item, return_value=False)
        item, update_func = mock.Mock(), mock.Mock()
        assert self.collection.add(item, update_func)
        assert_in(item.get_name(), self.collection)

    def test_replace(self):
        autospec_method(self.collection.add)
        item = mock.Mock()
        self.collection.replace(item)
        self.collection.add.assert_called_with(
            item,
            self.collection.remove_item,
        )
