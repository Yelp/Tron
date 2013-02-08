import contextlib
import os

@contextlib.contextmanager
def working_dir(path):
    """Change the working directory and revert back to previous working
    directory.
    """
    cwd = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(cwd)