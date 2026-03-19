"""Logging utilities for consistent structured output across modules."""

from __future__ import annotations

import logging


def setup_logging(level: str) -> None:
    """Configure root logging for all pipeline entrypoints.

    Args:
        level: Logging level string, for example `INFO` or `DEBUG`.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
