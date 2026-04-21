"""Exception handlers that translate domain errors to HTTP responses.

Registered on the FastAPI app in `create_app`. Each handler returns an
`ErrorResponse` body so clients see a consistent envelope.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from exoplanet_platform.api.schemas import ErrorResponse
from exoplanet_platform.exceptions import (
    DataSourceNotFoundError,
    DataSourceQuotaError,
    DataSourceUnavailableError,
    ExoplanetPlatformError,
    InsufficientDataError,
    StorageError,
    ValidationError,
)
from exoplanet_platform.logging_config import get_logger

logger = get_logger(__name__)


def _error_payload(exc: Exception, detail: str | None = None) -> dict[str, str | None]:
    """Build the JSON body for an error response."""
    return ErrorResponse(
        error=str(exc) or exc.__class__.__name__,
        detail=detail,
        type=exc.__class__.__name__,
    ).model_dump()


async def _not_found_handler(_: Request, exc: DataSourceNotFoundError) -> JSONResponse:
    logger.info("api.error.not_found", error=str(exc))
    return JSONResponse(status_code=404, content=_error_payload(exc))


async def _validation_handler(_: Request, exc: ValidationError) -> JSONResponse:
    logger.info("api.error.validation", error=str(exc))
    return JSONResponse(status_code=400, content=_error_payload(exc))


async def _quota_handler(_: Request, exc: DataSourceQuotaError) -> JSONResponse:
    logger.warning("api.error.quota", error=str(exc))
    return JSONResponse(status_code=429, content=_error_payload(exc))


async def _unavailable_handler(_: Request, exc: DataSourceUnavailableError) -> JSONResponse:
    logger.warning("api.error.unavailable", error=str(exc))
    return JSONResponse(status_code=503, content=_error_payload(exc))


async def _insufficient_handler(_: Request, exc: InsufficientDataError) -> JSONResponse:
    logger.info("api.error.insufficient_data", error=str(exc))
    return JSONResponse(status_code=422, content=_error_payload(exc))


async def _storage_handler(_: Request, exc: StorageError) -> JSONResponse:
    logger.error("api.error.storage", error=str(exc))
    return JSONResponse(status_code=500, content=_error_payload(exc))


async def _platform_handler(_: Request, exc: ExoplanetPlatformError) -> JSONResponse:
    logger.error("api.error.platform", error=str(exc), exc_type=exc.__class__.__name__)
    return JSONResponse(status_code=500, content=_error_payload(exc))


def register_exception_handlers(app: FastAPI) -> None:
    """Attach all platform exception handlers to the FastAPI app.

    Order matters: more specific subclasses must be registered before their
    parents so FastAPI's lookup resolves to the correct handler.
    """
    # FastAPI's exception-handler type is Callable[[Request, Exception], ...],
    # but Starlette dispatches the correct subclass at runtime, so narrower
    # signatures (Callable[[Request, DataSourceNotFoundError], ...]) are fine
    # in practice. Mypy flags this contravariance conflict; the pattern is
    # the canonical FastAPI idiom, so we suppress it at registration.
    app.add_exception_handler(DataSourceNotFoundError, _not_found_handler)  # type: ignore[arg-type]
    app.add_exception_handler(ValidationError, _validation_handler)  # type: ignore[arg-type]
    app.add_exception_handler(DataSourceQuotaError, _quota_handler)  # type: ignore[arg-type]
    app.add_exception_handler(DataSourceUnavailableError, _unavailable_handler)  # type: ignore[arg-type]
    app.add_exception_handler(InsufficientDataError, _insufficient_handler)  # type: ignore[arg-type]
    app.add_exception_handler(StorageError, _storage_handler)  # type: ignore[arg-type]
    app.add_exception_handler(ExoplanetPlatformError, _platform_handler)  # type: ignore[arg-type]
