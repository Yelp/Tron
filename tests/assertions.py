"""
 Assertions for testify.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from testifycompat import assert_in
from testifycompat import assert_not_reached


def assert_raises(expected_exception_class, callable_obj, *args, **kwargs):
    """Returns the exception if the callable raises expected_exception_class"""
    try:
        callable_obj(*args, **kwargs)
    except expected_exception_class as e:
        # we got the expected exception
        return e
    assert_not_reached(
        "No exception was raised (expected %s)" % expected_exception_class,
    )


def assert_length(sequence, expected, msg=None):
    """Assert that a sequence or iterable has an expected length."""
    msg = msg or "%(sequence)s has length %(length)s expected %(expected)s"
    length = len(list(sequence))
    assert length == expected, msg % locals()


def assert_call(mock, call_idx, *args, **kwargs):
    """Assert that a function was called on mock with the correct args."""
    actual = mock.mock_calls[call_idx] if mock.mock_calls else None
    msg = "Call %s expected %s, was %s" % (call_idx, (args, kwargs), actual)
    assert actual == (args, kwargs), msg


def assert_mock_calls(expected, mock_calls):
    """Assert that all expected calls are in the list of mock_calls."""
    for expected_call in expected:
        assert_in(expected_call, mock_calls)
