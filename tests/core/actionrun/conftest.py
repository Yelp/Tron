import shutil
import tempfile

import pytest

from tron.serialize import filehandler


@pytest.fixture
def output_path():
    output_path = filehandler.OutputPath(tempfile.mkdtemp())
    yield output_path
    shutil.rmtree(output_path.base, ignore_errors=True)
