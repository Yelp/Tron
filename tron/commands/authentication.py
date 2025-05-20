import os
from typing import cast

from botocore.credentials import InstanceMetadataFetcher
from botocore.credentials import InstanceMetadataProvider

from tron.commands.cmd_utils import get_client_config

try:
    from vault_tools.paasta_secret import get_client as get_vault_client  # type: ignore
    from vault_tools.paasta_secret import get_vault_url
    from vault_tools.paasta_secret import get_vault_ca
    from okta_auth import get_and_cache_jwt_default  # type: ignore
except ImportError:

    def get_vault_client(url: str, capath: str) -> None:
        pass

    def get_vault_url(ecosystem: str) -> str:
        return ""

    def get_vault_ca(ecosystem: str) -> str:
        return ""

    def get_and_cache_jwt_default(client_id: str) -> str:
        return ""


def get_current_ecosystem() -> str:
    """Get current ecosystem from host configs, defaults to dev if no config is found"""
    try:
        with open("/nail/etc/ecosystem") as f:
            return f.read().strip()
    except OSError:
        pass
    return "devc"


def get_sso_auth_token() -> str:
    """Generate an authentication token for the calling user from the Single Sign On provider, if configured"""
    client_id = get_client_config().get("auth_sso_oidc_client_id")
    return get_and_cache_jwt_default(client_id, refreshable=True) if client_id else ""  # type: ignore


def get_vault_auth_token() -> str:
    """Generate an authentication token for the underlying instance via Vault"""
    ecosystem = get_current_ecosystem()
    vault_client = get_vault_client(get_vault_url(ecosystem), get_vault_ca(ecosystem))
    vault_role = get_client_config().get("vault_api_auth_role", "service_authz")
    metadata_provider = InstanceMetadataProvider(
        iam_role_fetcher=InstanceMetadataFetcher(),
    )
    instance_credentials = metadata_provider.load()
    assert instance_credentials, "No instance credentials found"  # make mypy happy
    static_instance_credentials = instance_credentials.get_frozen_credentials()
    vault_client.auth.aws.iam_login(
        static_instance_credentials.access_key,
        static_instance_credentials.secret_key,
        static_instance_credentials.token,
        mount_point="aws-iam",
        role=vault_role,
        use_token=True,
    )
    response = vault_client.secrets.identity.generate_signed_id_token(name=vault_role)
    return cast(str, response["data"]["token"])


def get_auth_token() -> str:
    """Generate authentication token via Vault or Okta"""
    return get_vault_auth_token() if os.getenv("TRONCTL_VAULT_AUTH") else get_sso_auth_token()
