import logging
import os
import platform
from typing import Optional

import rook
import staticconf  # type: ignore

import tron
from tron.config.static_config import get_config_watcher
from tron.config.static_config import NAMESPACE


ROOKOUT_TOKEN: Optional[str] = None
ROOKOUT_CONFIG: Optional[dict] = None
ROOKOUT_TOKEN_PATH = "/nail/etc/tron_rookout_key.txt"
# By default, try to connect to rookout controller via Mesh.
# Can be changed in srv-configs when something is broken and SDK reconnects
DEFAULT_ROOKOUT_HOST = "169.254.255.254"
DEFAULT_ROOKOUT_PORT = 20798

log = logging.getLogger(__name__)


def get_version_sha() -> Optional[str]:
    version_sha = None
    try:
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/../VERSION_SHA") as file:
            version_sha = file.read().rstrip()
    except OSError as exc:
        log.warning("Failed to read the file VERSION_SHA: %s. Loading master as default.", exc)
    return version_sha


def get_hostname() -> str:
    return platform.node()


def prepare_rookout_token() -> None:
    """Load rookout token into memory while we are still root"""
    global ROOKOUT_TOKEN
    try:
        with open(ROOKOUT_TOKEN_PATH, encoding="utf-8") as _rookout_token_file:
            ROOKOUT_TOKEN = _rookout_token_file.read().strip()
    except OSError as exc:
        log.warning("Failed to load rookout token: %s", exc)


def enable_rookout() -> None:
    """Enable rookout if srv_config enables it"""
    global ROOKOUT_TOKEN, ROOKOUT_CONFIG
    config_watcher = get_config_watcher()
    config_watcher.reload_if_changed()

    enable = staticconf.read_bool("rookout.enable", namespace=NAMESPACE, default=False)

    if not enable:
        rook.stop()
        if ROOKOUT_CONFIG:
            log.info("Stopping Rookout SDK (srv-configs not enabled)")
        ROOKOUT_CONFIG = None
        return
    if not ROOKOUT_TOKEN:
        log.info("Rookout token not loaded, not enabling SDK")
        return

    connection_config = {
        "host": staticconf.read("rookout.controller", namespace=NAMESPACE, default=DEFAULT_ROOKOUT_HOST,),
        "port": staticconf.read_int("rookout.controller_port", namespace=NAMESPACE, default=DEFAULT_ROOKOUT_PORT,),
    }

    if connection_config != ROOKOUT_CONFIG:
        log.info("Stopping Rookout SDK (reconfig)")
        rook.stop()
        log.info("Starting Rookout SDK")
        rook.start(
            token=ROOKOUT_TOKEN,
            labels={
                "app": "tron",
                "hostname": get_hostname(),
                "ecosystem": open("/nail/etc/ecosystem", encoding="utf-8").read(),
                "superregion": open("/nail/etc/superregion", encoding="utf-8").read(),
                "version": tron.__version__,
            },
            log_to_stderr=True,
            git_commit=get_version_sha(),
            git_origin="https://github.com/Yelp/Tron/",
            throw_errors=False,
            **connection_config,
        )
        log.info("Started Rookout SDK")
        ROOKOUT_CONFIG = connection_config
