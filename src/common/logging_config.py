"""Centralized logging setup with suppression of noisy third-party loggers."""

import logging


_NOISY_LOGGERS = (
    "httpx",
    "httpcore",
    "urllib3",
    "urllib3.connectionpool",
    "h11",
    "openai",
    "httpie",
    "connectionpool",
    "py4j",
)


def setup_logging(level: str | None = None) -> None:
    """Configure logging and silence noisy third-party loggers.

    Args:
        level: Log level (e.g. INFO, WARNING). Defaults to LOG_LEVEL env or INFO.
    """
    import os

    lvl = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
