"""Centralized, consistent logging for every Spark job.

Usage:
    from pyspark_jobs.common.logger import get_logger
    log = get_logger(__name__)
    log.info("starting bronze ingestion")
"""
from __future__ import annotations

import logging
import os
import sys

_DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_CONFIGURED = False


def configure_logging(level: str | None = None, fmt: str | None = None) -> None:
    """Configure the root logger once for the whole process."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt or _DEFAULT_FORMAT))

    root = logging.getLogger()
    root.setLevel(level)
    # avoid duplicate handlers when Spark/py4j re-imports
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)

    # quiet down chatty libraries
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str, level: str | None = None) -> logging.Logger:
    """Return a configured logger for ``name``."""
    configure_logging(level)
    return logging.getLogger(name)
