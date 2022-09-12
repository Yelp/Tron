from unittest.mock import MagicMock
from unittest.mock import patch

from tron.utils.rookout import enable_rookout


@patch("rook.start")
@patch("tron.utils.rookout.ROOKOUT_ENABLE", True)
def test_enable_rookout_not_configured(rook_start: MagicMock):
    rook_start.return_value = True
    enable_rookout()
    assert rook_start.call_count == 0


@patch("rook.start")
@patch("tron.utils.rookout.prepare_rookout_token")
@patch("tron.utils.rookout.get_superregion")
@patch("tron.utils.rookout.get_ecosystem")
@patch("tron.utils.rookout.ROOKOUT_ENABLE", True)
def test_enable_rookout(
    get_ecosystem: MagicMock, get_superregion: MagicMock, prepare_rookout_token: MagicMock, rook_start: MagicMock
):
    rook_start.return_value = True
    get_ecosystem.return_value = "test_ecosystem"
    get_superregion.return_value = "test_superregion"
    prepare_rookout_token.return_value = "not-a-real-token"
    enable_rookout()
    assert rook_start.call_count == 1
