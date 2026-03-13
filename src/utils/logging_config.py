"""
Centralised logging configuration for the LEO Satellite Coverage Risk pipeline.

All pipeline entry points (main.py, agent.py) call setup_logging() once at
startup. Individual modules obtain their logger via the standard pattern:

    import logging
    logger = logging.getLogger(__name__)

This keeps module-level code clean while ensuring consistent formatting,
level control, and optional file output across the entire pipeline.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
    fmt: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
) -> None:
    """Configure the root logger for the pipeline.

    Parameters
    ----------
    level:
        Logging level string — "DEBUG", "INFO", "WARNING", "ERROR".
        Defaults to "INFO".
    log_file:
        Optional path to write logs to a file in addition to stdout.
        The parent directory is created if it does not exist.
    fmt:
        Log format string (standard :mod:`logging` format).
    datefmt:
        Date format for the timestamp in log records.

    Notes
    -----
    Calling this function multiple times is safe — existing handlers are
    cleared before reconfiguring to prevent duplicate log lines.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear existing handlers to avoid duplicates on repeated calls
    root_logger.handlers.clear()

    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Optional file handler
    if log_file is not None:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Silence noisy third-party loggers at WARNING unless DEBUG is requested
    if numeric_level > logging.DEBUG:
        for noisy in ("rasterio", "fiona", "pyproj", "urllib3", "matplotlib"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
