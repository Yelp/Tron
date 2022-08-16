from functools import partial

import staticconf
from staticconf import config

FILENAME = "/nail/srv/configs/tron.yaml"
NAMESPACE = "tron"

# Load configuration from 'tron.yaml' into namespace 'tron'
staticconf.YamlConfiguration(FILENAME, namespace=NAMESPACE)


def build_configuration(filename, namespace):
    config_loader = partial(staticconf.YamlConfiguration, filename, namespace=namespace)
    reloader = config.ReloadCallbackChain(namespace)
    return config.ConfigurationWatcher(config_loader, filename, min_interval=0, reloader=reloader)


config_watcher = build_configuration(FILENAME, NAMESPACE)
