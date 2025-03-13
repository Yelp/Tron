import logging
import os
from functools import lru_cache
from typing import NamedTuple
from typing import Optional

import cachetools.func
import requests
from twisted.web.server import Request


logger = logging.getLogger(__name__)
AUTH_CACHE_SIZE = 50000
AUTH_CACHE_TTL = 30 * 60


class AuthorizationOutcome(NamedTuple):
    authorized: bool
    reason: str


class AuthorizationFilter:
    """API request authorization via external system"""

    def __init__(self, endpoint: str, enforce: bool):
        """Constructor

        :param str endpoint: HTTP endpoint of external authorization system
        :param bool enforce: whether to enforce authorization decisions
        """
        self.endpoint = endpoint
        self.enforce = enforce
        self.session = requests.Session()

    @classmethod
    @lru_cache(maxsize=1)
    def get_from_env(cls) -> "AuthorizationFilter":
        return cls(
            endpoint=os.getenv("API_AUTH_ENDPOINT", ""),
            enforce=bool(os.getenv("API_AUTH_ENFORCE", "")),
        )

    def is_request_authorized(self, request: Request) -> AuthorizationOutcome:
        """Check if API request is authorized

        :param Request request: API request object
        :return: auth outcome
        """
        if not self.endpoint:
            return AuthorizationOutcome(True, "Auth not enabled")
        token = (request.getHeader("Authorization") or "").strip()
        token = token.split()[-1] if token else ""  # removes "Bearer" prefix
        url_path = request.path.decode()
        service = url_path.split("/")[-1].split(".", 1)[0] if "/jobs/" in url_path else None
        auth_outcome = self._is_request_authorized_impl(
            # path and method are byte arrays in twisted
            path=url_path,
            token=token,
            method=request.method.decode(),
            service=service,
        )
        return auth_outcome if self.enforce else AuthorizationOutcome(True, "Auth dry-run")

    @cachetools.func.ttl_cache(maxsize=AUTH_CACHE_SIZE, ttl=AUTH_CACHE_TTL)
    def _is_request_authorized_impl(
        self,
        path: str,
        token: str,
        method: str,
        service: Optional[str],
    ) -> AuthorizationOutcome:
        """Check if API request is authorized

        :param str path: API path
        :param str token: authentication token
        :param str method: http method
        :return: auth outcome
        """
        try:
            response = self.session.post(
                url=self.endpoint,
                json={
                    "input": {
                        "path": path,
                        "backend": "tron",
                        "token": token,
                        "method": method.lower(),
                        "service": service,
                    },
                },
                timeout=2,
            ).json()
        except Exception as e:
            logger.exception(f"Issue communicating with auth endpoint: {e}")
            return AuthorizationOutcome(False, "Auth backend error")

        auth_result_allowed = response.get("result", {}).get("allowed")
        if auth_result_allowed is None:
            return AuthorizationOutcome(False, "Malformed auth response")

        if not auth_result_allowed:
            reason = response["result"].get("reason", "Denied")
            return AuthorizationOutcome(False, reason)

        reason = response["result"].get("reason", "Ok")
        return AuthorizationOutcome(True, reason)
