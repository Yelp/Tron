import logging
import os
import platform
from typing import Optional

import rook

import tron


ROOKOUT_TOKEN: Optional[str] = None
ROOKOUT_TOKEN_PATH = os.getenv("ROOKOUT_TOKEN_PATH", "/nail/etc/tron_rookout_key.txt")
ROOKOUT_CONTROLLER_HOST = os.getenv("ROOKOUT_CONTROLLER_HOST", "169.254.255.254")
ROOKOUT_CONTROLLER_PORT = int(os.getenv("ROOKOUT_CONTROLLER_PORT", 20798))
ROOKOUT_ENABLE = os.getenv("ROOKOUT_ENABLE", False)

log = logging.getLogger(__name__)


def get_version_sha() -> Optional[str]:
    version_sha = None
    try:
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/../VERSION_SHA") as file:
            version_sha = file.read().rstrip()
    except OSError:
        log.exception("Failed to read the file VERSION_SHA. Using master as default.")
    return version_sha


def get_hostname() -> str:
    return platform.node()


def prepare_rookout_token() -> None:
    """Load rookout token into memory"""
    global ROOKOUT_TOKEN
    try:
        with open(ROOKOUT_TOKEN_PATH, encoding="utf-8") as _rookout_token_file:
            ROOKOUT_TOKEN = _rookout_token_file.read().strip()
    except OSError:
        log.exception("Failed to load rookout token")


def enable_rookout() -> None:
    """Enable rookout if srv_config enables it"""
    global ROOKOUT_TOKEN

    if not ROOKOUT_ENABLE:
        log.info("ROOKOUT_ENABLE set to False. Rookout SDK (Not enabled).")
        return
    prepare_rookout_token()
    if not ROOKOUT_TOKEN:
        log.info("Rookout token not loaded, not enabling SDK")
        return

    connection_config = {
        "host": ROOKOUT_CONTROLLER_HOST,
        "port": ROOKOUT_CONTROLLER_PORT,
    }

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
