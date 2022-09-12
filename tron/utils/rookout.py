import logging
import os
import platform
import sys
from distutils.util import strtobool
from typing import Optional

import rook

import tron
from tron.utils.habitat import get_ecosystem
from tron.utils.habitat import get_superregion

if sys.version_info >= (3, 7):  # pragma: no cover (PY37+)
    from importlib.resources import read_text
else:  # pragma: no cover (<PY37)
    from importlib_resources import read_text

ROOKOUT_TOKEN_PATH = os.getenv("ROOKOUT_TOKEN_PATH", "/nail/etc/tron_rookout_key.txt")
ROOKOUT_CONTROLLER_HOST: str = os.getenv("ROOKOUT_CONTROLLER_HOST", "169.254.255.254")
ROOKOUT_CONTROLLER_PORT: int = int(os.getenv("ROOKOUT_CONTROLLER_PORT", 20798))
ROOKOUT_ENABLE: bool = strtobool(os.getenv("ROOKOUT_ENABLE", "False"))

log = logging.getLogger(__name__)


def get_version_sha() -> Optional[str]:
    version_sha = None
    try:
        version_sha = read_text("tron", "VERSION_SHA").rstrip()
    except OSError:
        log.exception("Failed to read the file VERSION_SHA. Using master as default.")
    return version_sha


def get_hostname() -> str:
    return platform.node()


def prepare_rookout_token() -> Optional[str]:
    """Load rookout token into memory"""
    rookout_token: Optional[str] = None
    try:
        with open(ROOKOUT_TOKEN_PATH, encoding="utf-8") as _rookout_token_file:
            rookout_token = _rookout_token_file.read().strip()
    except OSError:
        log.exception("Failed to load rookout token")
    return rookout_token


def enable_rookout() -> None:
    """Enable rookout if configured"""
    if not ROOKOUT_ENABLE:
        log.info("ROOKOUT_ENABLE set to False. Rookout SDK (Not enabled).")
        return
    rookout_token = prepare_rookout_token()
    if not rookout_token:
        log.info("Rookout token not loaded, not enabling SDK")
        return

    connection_config = {
        "host": ROOKOUT_CONTROLLER_HOST,
        "port": ROOKOUT_CONTROLLER_PORT,
    }

    log.info("Starting Rookout SDK")
    rook.start(
        token=rookout_token,
        labels={
            "app": "tron",
            "hostname": get_hostname(),
            "ecosystem": get_ecosystem(),
            "superregion": get_superregion(),
            "version": tron.__version__,
        },
        log_to_stderr=True,
        git_commit=get_version_sha(),
        git_origin="https://github.com/Yelp/Tron/",
        throw_errors=False,
        **connection_config,
    )
    log.info("Started Rookout SDK")
