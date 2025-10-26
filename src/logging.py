from __future__ import annotations

import logging

from src.config import settings

# ---------- Logging levels ----------


LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

logging.basicConfig(
    level=LOG_LEVELS[settings["log"]["level"]],  # root logger level
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

logging.info(settings)
