from functools import partial

import staticconf  # type: ignore
from staticconf import config

FILENAME = "/nail/srv/configs/tron.yaml"
NAMESPACE = "tron"


def load_yaml_file() -> None:
    staticconf.YamlConfiguration(filename=FILENAME, namespace=NAMESPACE)


def build_configuration_watcher(filename: str, namespace: str) -> config.ConfigurationWatcher:
    config_loader = partial(staticconf.YamlConfiguration, filename, namespace=namespace)
    reloader = config.ReloadCallbackChain(namespace)
    return config.ConfigurationWatcher(config_loader, filename, min_interval=0, reloader=reloader)


# Load configuration from 'tron.yaml' into namespace 'tron'
def get_config_watcher() -> config.ConfigurationWatcher:
    load_yaml_file()
    return build_configuration_watcher(FILENAME, NAMESPACE)
