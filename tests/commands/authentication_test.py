from unittest.mock import patch

from tron.commands.authentication import get_vault_auth_token


@patch("tron.commands.authentication.get_client_config", autospec=True)
@patch("tron.commands.authentication.get_current_ecosystem", autospec=True)
@patch("tron.commands.authentication.InstanceMetadataProvider", autospec=True)
@patch("tron.commands.authentication.InstanceMetadataFetcher", autospec=True)
@patch("tron.commands.authentication.get_vault_client", autospec=True)
@patch("tron.commands.authentication.get_vault_url", autospec=True)
@patch("tron.commands.authentication.get_vault_ca", autospec=True)
def test_get_service_auth_token(
    mock_vault_ca,
    mock_vault_url,
    mock_get_vault_client,
    mock_metadata_fetcher,
    mock_metadata_provider,
    mock_ecosystem,
    mock_config,
):
    mock_ecosystem.return_value = "dev"
    mock_config.return_value = {"vault_api_auth_role": "foobar"}
    mock_vault_client = mock_get_vault_client.return_value
    mock_vault_client.secrets.identity.generate_signed_id_token.return_value = {
        "data": {"token": "sometoken"},
    }
    assert get_vault_auth_token() == "sometoken"
    mock_instance_creds = mock_metadata_provider.return_value.load.return_value.get_frozen_credentials.return_value
    mock_metadata_provider.assert_called_once_with(iam_role_fetcher=mock_metadata_fetcher.return_value)
    mock_vault_url.assert_called_once_with("dev")
    mock_vault_ca.assert_called_once_with("dev")
    mock_get_vault_client.assert_called_once_with(mock_vault_url.return_value, mock_vault_ca.return_value)
    mock_vault_client.auth.aws.iam_login.assert_called_once_with(
        mock_instance_creds.access_key,
        mock_instance_creds.secret_key,
        mock_instance_creds.token,
        mount_point="aws-iam",
        role="foobar",
        use_token=True,
    )
    mock_vault_client.secrets.identity.generate_signed_id_token.assert_called_once_with(name="foobar")
