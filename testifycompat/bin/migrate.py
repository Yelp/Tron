#!/usr/bin/env python
"""

.. warning::

    This script is still very experimental. Use at your own risk. It will
    be replaced over time with lib2to3 fixers.


Usage:

    ``python -m testifycompat.bin.migrate <filenames>``

Example:

    ``find tests -name *.py | xargs python migrate.py``


"""
import functools
import re
import sys


def replace(pattern, repl):
    return functools.partial(re.sub, pattern, repl)


replaces = [
    # Replace imports
    replace(r'^from testify import ', 'from testifycompat import '),
    replace(r'^from testify.assertions import ', 'from testifycompat import '),
    replace(r'^import testify as T', 'import testifycompat as T'),

    # Replace test classes
    replace(r'^class (?:Test)?(\w+)(?:Test|TestCase)\((?:T\.)?TestCase\):$',
            'class Test\\1(object):'),
    replace(r'^class (?:Test)?(\w+)(?:Test|TestCase)(\(\w+TestCase\)):$',
            'class Test\\1\\2:'),

    # Replace some old assertions
    replace(r'self.assert_\((.*)\)', 'assert \\1')
]


def run_replacement(contents):
    for line in contents:
        for replacement in replaces:
            line = replacement(line)
        yield line


def strip_if_main_run(contents):
    if len(contents) < 2:
        return contents
    if 'run()' in contents[-1] and 'if __name__ == ' in contents[-2]:
        return contents[:-2]
    return contents


def run_migration_on_file(filename):
    with open(filename, 'r') as fh:
        lines = fh.read().split('\n')

    lines = list(run_replacement(lines))
    lines = strip_if_main_run(lines)

    with open(filename, 'w') as fh:
        fh.write('\n'.join(lines))


if __name__ == "__main__":
    for filename in sys.argv[1:]:
        run_migration_on_file(filename)
