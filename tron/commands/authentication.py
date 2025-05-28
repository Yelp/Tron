import os
from typing import cast
from typing import Optional

from tron.commands.cmd_utils import get_client_config

try:
    from vault_tools.oidc import get_instance_oidc_identity_token  # type: ignore # library lacks py.typed marker
    from okta_auth import get_and_cache_jwt_default  # type: ignore # library lacks py.typed marker
except ImportError:

    def get_instance_oidc_identity_token(role: str, ecosystem: Optional[str] = None) -> str:
        return ""

    def get_and_cache_jwt_default(client_id: str) -> str:
        return ""


def get_sso_auth_token() -> str:
    """Generate an authentication token for the calling user from the Single Sign On provider, if configured"""
    client_id = get_client_config().get("auth_sso_oidc_client_id")
    return cast(str, get_and_cache_jwt_default(client_id, refreshable=True)) if client_id else ""


def get_vault_auth_token() -> str:
    """Generate an authentication token for the underlying instance via Vault"""
    vault_role = get_client_config().get("vault_api_auth_role", "service_authz")
    return cast(str, get_instance_oidc_identity_token(vault_role))


def get_auth_token() -> str:
    """Generate authentication token via Vault or Okta"""
    return get_vault_auth_token() if os.getenv("TRONCTL_VAULT_AUTH") else get_sso_auth_token()
