import pytest
import staticconf.testing

from tron.config.static_config import NAMESPACE
from tron.utils.authentication import get_projected_sa_volumes


@pytest.mark.parametrize(
    "service,expected",
    (
        ("service_a", []),
        ("service_b", [{"foo": "bar"}]),
    ),
)
def test_get_projected_sa_volumes(service, expected, tmpdir):
    with (tmpdir / "authenticating.yaml").open("w") as f:
        f.write("services:\n- service_b\n- service_c\n")
    with (tmpdir / "jwt_service_auth.json").open("w") as f:
        f.write(r'{"service_auth_token_settings": {"foo": "bar"}}')
    paths_config = {
        "soa_path": str(tmpdir),
        "paasta_config_path": str(tmpdir),
    }
    with staticconf.testing.MockConfiguration(paths_config, namespace=NAMESPACE):
        assert get_projected_sa_volumes(service) == expected
