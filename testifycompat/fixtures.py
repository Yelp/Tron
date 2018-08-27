"""
Compatibility fixtures for migrating code from testify to py.test

.. note::

    ``class_`` fixtures must be applied to @classmethods. py.test will not run
    a class_* fixture that is not attached to a class-level method, so your
    tests will probably fail.
"""
import pytest


def setup(func):
    return pytest.fixture(autouse=True)(func)


def setup_teardown(func):
    return pytest.yield_fixture(autouse=True)(func)


def teardown(func):
    def teardown_(*args, **kwargs):
        yield
        func(*args, **kwargs)
    return pytest.yield_fixture(autouse=True)(teardown_)


def class_setup(func):
    return pytest.fixture(autouse=True, scope='class')(func)


def class_setup_teardown(func):
    return pytest.yield_fixture(autouse=True, scope='class')(func)


def class_teardown(func):
    def teardown_(*args, **kwargs):
        yield
        func(*args, **kwargs)
    return pytest.yield_fixture(autouse=True, scope='class')(teardown_)


def suite(name, reason=None):
    """Translate a :func:`testify.suite` decorator into the appropriate
    :mod:`pytest.mark` call. For the disabled suite this results in a
    skipped test. For other suites it will return a  `pytest.mark.<name>`
    decorator.
    """
    if name == 'disabled':
        return pytest.mark.skipif(True, reason=reason)

    return getattr(pytest.mark, name)
