from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
from unittest.mock import MagicMock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron.api.requestargs import get_bool
from tron.api.requestargs import get_datetime
from tron.api.requestargs import get_integer
from tron.api.requestargs import get_string


class TestRequestArgs(TestCase):
    @setup
    def setup_args(self):
        self.args = {
            b'number': [b'123'],
            b'string': [b'astring'],
            b'boolean': [b'1'],
            b'datetime': [b'2012-03-14 15:09:26'],
        }
        self.datetime = datetime.datetime(2012, 3, 14, 15, 9, 26)
        self.request = MagicMock(args=self.args)

    def _add_arg(self, name, value):
        name = name.encode()
        value = value.encode()
        if name not in self.args:
            self.args[name] = []
        self.args[name].append(value)

    def test_get_integer_valid_int(self):
        self._add_arg('number', '5')
        assert_equal(get_integer(self.request, 'number'), 123)

    def test_get_integer_invalid_int(self):
        self._add_arg('nan', 'beez')
        assert not get_integer(self.request, 'nan')

    def test_get_integer_missing(self):
        assert not get_integer(self.request, 'missing')

    def test_get_string(self):
        self._add_arg('string', 'bogus')
        assert_equal(get_string(self.request, 'string'), 'astring')

    def test_get_string_missing(self):
        assert not get_string(self.request, 'missing')

    def test_get_bool(self):
        assert get_bool(self.request, 'boolean')

    def test_get_bool_false(self):
        self._add_arg('false', '0')
        assert not get_bool(self.request, 'false')

    def test_get_bool_missing(self):
        assert not get_bool(self.request, 'missing')

    def test_get_datetime_valid(self):
        assert_equal(get_datetime(self.request, 'datetime'), self.datetime)

    def test_get_datetime_invalid(self):
        self._add_arg('nope', '2012-333-4')
        assert not get_datetime(self.request, 'nope')

    def test_get_datetime_missing(self):
        assert not get_datetime(self.request, 'missing')


if __name__ == "__main__":
    run()
