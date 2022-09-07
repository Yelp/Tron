from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import staticconf.testing

from tron.config.static_config import NAMESPACE
from tron.utils.rookout import enable_rookout


@pytest.fixture()
def setup_config():
    tron_config = {"rookout.enable": True}
    yield staticconf.testing.MockConfiguration(tron_config, namespace=NAMESPACE)


@patch("rook.start")
@patch("rook.stop")
@patch("tron.utils.rookout.get_config_watcher", autospec=True)
def test_enable_rookout_not_configured(
    mock_get_config_watcher: MagicMock, rook_stop: MagicMock, rook_start: MagicMock, setup_config
):
    """Test without configured staticconf"""
    rook_start.return_value = True
    rook_stop.return_value = True
    with setup_config:
        enable_rookout()

    assert rook_stop.call_count == 0
    assert rook_start.call_count == 0


@patch("rook.start")
@patch("rook.stop")
@patch("staticconf.read_bool")
@patch("tron.utils.rookout.ROOKOUT_TOKEN", "not-a-real-token")
@patch("tron.utils.rookout.get_config_watcher", autospec=True)
def test_enable_rookout(
    mock_get_config_watcher: MagicMock, get_bool: MagicMock, rook_stop: MagicMock, rook_start: MagicMock, setup_config
):
    """Test with configured staticconf"""
    rook_start.return_value = True
    rook_stop.return_value = True
    get_bool.return_value = True
    with setup_config:
        enable_rookout()

    assert rook_stop.call_count == 1
    assert rook_start.call_count == 1


@patch("rook.start")
@patch("rook.stop")
@patch("staticconf.read_bool")
@patch("staticconf.read")
@patch("tron.utils.rookout.ROOKOUT_TOKEN", "not-a-real-token")
@patch("tron.utils.rookout.get_config_watcher", autospec=True)
def test_enable_rookout_reconfigure(
    mock_get_config_watcher: MagicMock,
    get: MagicMock,
    get_bool: MagicMock,
    rook_stop: MagicMock,
    rook_start: MagicMock,
    setup_config,
):
    """Test with configured staticconf, reconfigure"""
    rook_start.return_value = True
    rook_stop.return_value = True
    get_bool.return_value = True
    with setup_config:
        enable_rookout()

    assert rook_stop.call_count == 1
    assert rook_start.call_count == 1

    get.return_value = "127.0.0.1"

    with setup_config:
        enable_rookout()

    assert rook_stop.call_count == 2
    assert rook_start.call_count == 2
