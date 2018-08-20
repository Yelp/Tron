"""Compatiblity functions for py.test to migrate code from testify.

This is not a complete list, but should hopefully cover most of the common
assertions.

These assertions should **not** be used in new code, and are only for migrating
old tests.
"""
import pytest


def assert_equal(left, right, *args):
    assert left == right


assert_sets_equal = assert_dicts_equal = assert_datetimes_equal = assert_equal
assert_equals = assert_equal


def assert_true(val):
    assert val


def assert_false(val):
    assert not val


def assert_raises_and_contains(exc, text, func, *args, **kwargs):
    with pytest.raises(exc) as excinfo:
        func(*args, **kwargs)

    text = text if isinstance(text, list) else [text]
    for item in text:
        assert item in str(excinfo.exconly())


def assert_raises(exc, func=None, *args, **kwargs):
    if func is None:
        return pytest.raises(exc)

    with pytest.raises(exc) as excinfo:
        func(*args, **kwargs)


def assert_in(item, container):
    assert item in container


def assert_not_in(item, container):
    assert item not in container


def assert_is(left, right):
    assert left is right


def assert_is_not(left, right):
    assert left is not right


def assert_not_equal(left, right):
    assert left != right


def assert_lt(left, right):
    assert left < right


def assert_lte(left, right):
    assert left <= right


def assert_gt(left, right):
    assert left > right


def assert_gte(left, right):
    assert left >= right


def assert_in_range(val, start, end):
    assert start < val < end


def assert_between(val, start, end):
    assert start <= val <= end


def assert_all_in(left, right):
    """Assert that everything in `left` is also in `right`
    Note: This is different than `assert_subset()` because python sets use
    `__hash__()` for comparision whereas `in` uses `__eq__()`.
    """
    for item in left:
        assert item in right


def assert_starts_with(val, prefix):
    assert val.startswith(prefix)


def assert_not_reached():
    assert False


def assert_empty(iterable):
    assert len(list(iterable)) == 0


def assert_not_empty(iterable):
    assert len(list(iterable)) > 0


def assert_length(sequence, expected):
    assert len(list(sequence)) == expected


def assert_sorted_equal(left, right):
    assert sorted(left) == sorted(right)


def assert_isinstance(object_, type_):
    assert isinstance(object_, type_)
