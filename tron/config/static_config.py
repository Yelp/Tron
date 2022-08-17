from functools import partial

import staticconf
from staticconf import config

FILENAME = "/nail/srv/configs/tron.yaml"
NAMESPACE = "tron"


def load_yaml_file():
    staticconf.YamlConfiguration(filename=FILENAME, namespace=NAMESPACE)


def build_configuration(filename, namespace):
    config_loader = partial(staticconf.YamlConfiguration, filename, namespace=namespace)
    reloader = config.ReloadCallbackChain(namespace)
    return config.ConfigurationWatcher(config_loader, filename, min_interval=0, reloader=reloader)


# Load configuration from 'tron.yaml' into namespace 'tron'
def get_config_watcher():
    load_yaml_file()
    return build_configuration(FILENAME, NAMESPACE)
