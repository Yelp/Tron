import logging
import os
from functools import lru_cache
from functools import partial
from typing import cast
from typing import List
from typing import Optional
from typing import Set

import staticconf  # type: ignore
from task_processing.plugins.kubernetes.types import ProjectedSAVolume

from tron.config.static_config import NAMESPACE as TRON_NAMESPACE


log = logging.getLogger(__name__)
TOKEN_VOLUME_CONF_NAMESPACE = "service_account_token"
AUTH_SERVICES_CONF_NAMESPACE = "authenticating_services"
DEFAULT_SOA_PATH = "/nail/etc/services"
DEFAULT_PAASTA_CONFIG_PATH = "/etc/paasta"
CACHED_AUTHENTICATING_SERVICES = None


@lru_cache
def _get_config_watcher(filepath: str, namespace: str) -> Optional[staticconf.ConfigurationWatcher]:
    """Load configuration file and return a watcher for it

    :param str filepath: path to JSON/YAML configuration
    """
    watcher = None
    loader_class = staticconf.JSONConfiguration if filepath.endswith(".json") else staticconf.YamlConfiguration
    loader = partial(loader_class, filepath, namespace=namespace, flatten=False)
    reloader = (staticconf.config.ReloadCallbackChain(namespace),)
    try:
        loader()
        watcher = staticconf.ConfigurationWatcher(loader, filepath, min_interval=10, reloader=reloader)
    except Exception as e:
        # soft failing, as authz features are currently optional
        log.warning(f"Failed loading {filepath}: {e}")
    return watcher


def get_authenticating_services() -> Set[str]:
    """Load list of services participating in authenticated traffic

    :param str soa_dir: SOA configuration directory
    :return: set of service names
    """
    global CACHED_AUTHENTICATING_SERVICES
    soa_path = staticconf.read_string("soa_path", namespace=TRON_NAMESPACE, default=DEFAULT_SOA_PATH)
    authenticating_services_conf_path = os.path.join(soa_path, "authenticating.yaml")
    watcher = _get_config_watcher(authenticating_services_conf_path, AUTH_SERVICES_CONF_NAMESPACE)
    if (watcher and watcher.reload_if_changed()) or CACHED_AUTHENTICATING_SERVICES is None:
        CACHED_AUTHENTICATING_SERVICES = set(
            staticconf.read_list("services", namespace=AUTH_SERVICES_CONF_NAMESPACE, default=[])
        )
    return CACHED_AUTHENTICATING_SERVICES


def get_service_auth_token_volume_config() -> ProjectedSAVolume:
    """Get service authentication token mount configuration

    :param str paasta_config_dir: PaaSTA configuration directory
    :return: configuration as dictionary, if present
    """
    paasta_config_path = staticconf.read_string(
        "paasta_config_path", namespace=TRON_NAMESPACE, default=DEFAULT_PAASTA_CONFIG_PATH
    )
    config_path = os.path.join(paasta_config_path, "jwt_service_auth.json")
    watcher = _get_config_watcher(config_path, TOKEN_VOLUME_CONF_NAMESPACE)
    if watcher:
        watcher.reload_if_changed()
    return cast(
        ProjectedSAVolume,
        staticconf.read("service_auth_token_settings", namespace=TOKEN_VOLUME_CONF_NAMESPACE, default={}),
    )


def get_projected_sa_volumes(service: str) -> List[ProjectedSAVolume]:
    """Return projected service account volume, as a single elemenet list,
    if service participates in authenticated communications.

    :param str service: name of the service
    :return: list of volume config, empty if not needed
    """
    volume_config = get_service_auth_token_volume_config()
    return [volume_config] if volume_config and service in get_authenticating_services() else []
