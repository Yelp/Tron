from unittest.mock import MagicMock
from unittest.mock import patch

from tron.utils.rookout import enable_rookout


@patch("rook.start")
@patch("rook.stop")
def test_enable_rookout_not_configured(rook_stop: MagicMock, rook_start: MagicMock):
    """Test without configured staticconf"""
    rook_start.return_value = True
    rook_stop.return_value = True
    enable_rookout()

    assert rook_stop.call_count == 0
    assert rook_start.call_count == 0


@patch("rook.start")
@patch("rook.stop")
@patch("staticconf.read_bool")
@patch("tron.utils.rookout.ROOKOUT_TOKEN", "not-a-real-token")
def test_enable_rookout(get_bool: MagicMock, rook_stop: MagicMock, rook_start: MagicMock):
    """Test with configured staticconf"""
    rook_start.return_value = True
    rook_stop.return_value = True
    get_bool.return_value = True

    enable_rookout()

    assert rook_stop.call_count == 1
    assert rook_start.call_count == 1


@patch("rook.start")
@patch("rook.stop")
@patch("staticconf.read_bool")
@patch("staticconf.read")
@patch("tron.utils.rookout.ROOKOUT_TOKEN", "not-a-real-token")
def test_enable_rookout_reconfigure(get: MagicMock, get_bool: MagicMock, rook_stop: MagicMock, rook_start: MagicMock):
    """Test with configured staticconf, reconfigure"""
    rook_start.return_value = True
    rook_stop.return_value = True
    get_bool.return_value = True

    enable_rookout()

    assert rook_stop.call_count == 1
    assert rook_start.call_count == 1

    get.return_value = "127.0.0.1"

    enable_rookout()

    assert rook_stop.call_count == 2
    assert rook_start.call_count == 2
