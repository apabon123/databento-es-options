import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger with rotating file and console handlers.

    - File logs to logs/downloader.log with rotation (2 MB, 3 backups) at DEBUG level.
    - Console logs to stdout at INFO level.
    - Both handlers share the same formatter.
    - Ensures the logs/ directory exists.
    """
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file_path = logs_dir / "downloader.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    file_handler = RotatingFileHandler(
        filename=str(log_file_path),
        maxBytes=2 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


