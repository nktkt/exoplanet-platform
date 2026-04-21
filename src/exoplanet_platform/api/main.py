"""FastAPI application factory and uvicorn entrypoint.

`create_app()` is the canonical way to build the app (used by tests that need
a fresh instance). The module-level `app` binding is what the uvicorn string
`exoplanet_platform.api.main:app` resolves to, and `run()` is the entrypoint
for the `exoplanet-api` console script.
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from exoplanet_platform import __version__
from exoplanet_platform.api.errors import register_exception_handlers
from exoplanet_platform.api.routes import api_router
from exoplanet_platform.config import get_settings
from exoplanet_platform.logging_config import get_logger
from exoplanet_platform.storage.database import init_db

logger = get_logger(__name__)


def create_app() -> FastAPI:
    """Build and configure a FastAPI application instance."""
    settings = get_settings()
    app = FastAPI(
        title="Exoplanet Analysis Platform",
        description="Production-grade API for exoplanet ingestion, storage, and analysis.",
        version=__version__,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    register_exception_handlers(app)
    app.include_router(api_router)

    @app.on_event("startup")
    async def _startup() -> None:
        """Ensure the database schema exists before serving traffic."""
        logger.info("api.startup", environment=settings.environment)
        init_db()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        logger.info("api.shutdown")

    return app


app = create_app()


def run() -> None:
    """Entrypoint for the `exoplanet-api` console script."""
    settings = get_settings()
    uvicorn.run(
        "exoplanet_platform.api.main:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=False,
    )
