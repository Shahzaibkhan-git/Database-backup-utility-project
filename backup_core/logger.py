from __future__ import annotations

import logging
from pathlib import Path

from django.conf import settings


def get_logger(name: str = "backup_core") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        logger.propagate = False
        return logger

    logger.setLevel(logging.INFO)

    default_log_file = Path(settings.BASE_DIR) / "logs" / "backup.log"
    log_file = Path(getattr(settings, "BACKUP_LOG_FILE", default_log_file))
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Prevent duplicate lines through root logger handlers (e.g., Celery worker logs).
    logger.propagate = False

    return logger
