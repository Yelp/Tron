"""
 Assertions for testify.
"""
from testify.assertions import assert_not_reached


def assert_raises(expected_exception_class, callable_obj, *args, **kwargs):
    """Returns the exception if the callable raises expected_exception_class"""
    try:
        callable_obj(*args, **kwargs)
    except expected_exception_class, e:
        # we got the expected exception
        return e
    assert_not_reached("No exception was raised (expected %s)" %
                       expected_exception_class)


def assert_length(sequence, expected, msg=None):
    """Assert that a sequence or iterable has an expected length."""
    msg = msg or "%(sequence)s has length %(length)s expected %(expected)s"
    length = len(list(sequence))
    assert length == expected, msg % locals()


def assert_call(turtle, call_idx, *args, **kwargs):
    """Assert that a function was called on turtle with the correct args."""
    if not turtle.calls:
        actual = None
    else:
        actual = turtle.calls[call_idx]
    msg = "Call %s expected %s, was %s" % (call_idx, (args, kwargs), actual)
    assert actual == (args, kwargs), msg