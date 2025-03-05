from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from twisted.web.server import Request

from tron.api.auth import AuthorizationFilter
from tron.api.auth import AuthorizationOutcome


@pytest.fixture
def mock_auth_filter():
    with patch("tron.api.auth.requests"):
        yield AuthorizationFilter("http://localhost:31337/whatever", True)


def mock_request(path: str, token: str, method: str):
    res = MagicMock(spec=Request, path=path.encode(), method=method.encode())
    res.getHeader.return_value = token
    return res


def test_is_request_authorized(mock_auth_filter):
    mock_auth_filter.session.post.return_value.json.return_value = {
        "result": {"allowed": True, "reason": "User allowed"}
    }
    assert mock_auth_filter.is_request_authorized(
        mock_request("/api/jobs/foobar.run.2", "aaa.bbb.ccc", "get")
    ) == AuthorizationOutcome(True, "User allowed")
    mock_auth_filter.session.post.assert_called_once_with(
        url="http://localhost:31337/whatever",
        json={
            "input": {
                "path": "/api/jobs/foobar.run.2",
                "backend": "tron",
                "token": "aaa.bbb.ccc",
                "method": "get",
                "service": "foobar",
            }
        },
        timeout=2,
    )


def test_is_request_authorized_fail(mock_auth_filter):
    mock_auth_filter.session.post.side_effect = Exception
    assert mock_auth_filter.is_request_authorized(
        mock_request("/allowed", "eee.ddd.fff", "get")
    ) == AuthorizationOutcome(False, "Auth backend error")


def test_is_request_authorized_malformed(mock_auth_filter):
    mock_auth_filter.session.post.return_value.json.return_value = {"foo": "bar"}
    assert mock_auth_filter.is_request_authorized(
        mock_request("/allowed", "eee.ddd.fff", "post")
    ) == AuthorizationOutcome(False, "Malformed auth response")


def test_is_request_authorized_no_enforce(mock_auth_filter):
    mock_auth_filter.session.post.return_value.json.return_value = {
        "result": {"allowed": False, "reason": "Missing token"}
    }
    with patch.object(mock_auth_filter, "enforce", False):
        assert mock_auth_filter.is_request_authorized(mock_request("/foobar", "", "post")) == AuthorizationOutcome(
            True, "Auth dry-run"
        )


def test_is_request_authorized_disabled(mock_auth_filter):
    mock_auth_filter.session.post.return_value.json.return_value = {
        "result": {"allowed": False, "reason": "Missing token"}
    }
    with patch.object(mock_auth_filter, "endpoint", None):
        assert mock_auth_filter.is_request_authorized(mock_request("/buzz", "", "post")) == AuthorizationOutcome(
            True, "Auth not enabled"
        )
