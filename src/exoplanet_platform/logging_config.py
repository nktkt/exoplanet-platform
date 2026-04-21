"""Structured logging setup using structlog.

All modules should get a logger via `get_logger(__name__)` and never use the
stdlib logger directly, so we get consistent JSON or console output.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import Processor


def configure_logging(level: str = "INFO", json_output: bool = False) -> None:
    """Configure process-wide structlog + stdlib logging."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_output:
        processors: list[Processor] = [
            *shared_processors,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger preseeded with module context."""
    return structlog.get_logger(name).bind(**initial_context)
