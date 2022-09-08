from unittest.mock import MagicMock
from unittest.mock import patch

from tron.utils.rookout import enable_rookout


@patch("rook.start")
@patch("tron.utils.rookout.ROOKOUT_ENABLE", True)
def test_enable_rookout_not_configured(rook_start: MagicMock):
    """Test without configured staticconf"""
    rook_start.return_value = True
    enable_rookout()
    assert rook_start.call_count == 0


@patch("rook.start")
@patch("tron.utils.rookout.ROOKOUT_ENABLE", True)
@patch("tron.utils.rookout.ROOKOUT_TOKEN", "not-a-real-token")
def test_enable_rookout(rook_start: MagicMock):
    """Test with configured staticconf"""
    rook_start.return_value = True
    enable_rookout()
    assert rook_start.call_count == 1
