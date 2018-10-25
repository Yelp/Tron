from unittest import TestCase  # noqa: F401

from testifycompat.assertions import *  # noqa: F401, F403
from testifycompat.fixtures import *  # noqa: F401, F403


version = '0.1.2'


def run():
    raise AssertionError(
        "Oops, you tried to use testifycompat.run(). This function doesn't "
        "do anything, it only exists as backwards compatibility with testify. "
        "You should remove it from your code.")
