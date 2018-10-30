from unittest import TestCase

from testifycompat.assertions import *
from testifycompat.fixtures import *


version = '0.1.2'


def run():
    raise AssertionError(
        "Oops, you tried to use testifycompat.run(). This function doesn't "
        "do anything, it only exists as backwards compatibility with testify. "
        "You should remove it from your code.")
