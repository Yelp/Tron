from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import os


@contextlib.contextmanager
def working_dir(path):
    """Change the working directory and revert back to previous working
    directory.

    WARNING: This decorator manipulates global state (current directory) and
    should not be used in any code that is run in the tron daemon. This
    decorator should only be used in short lived scripts under tools/.
    """
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)
