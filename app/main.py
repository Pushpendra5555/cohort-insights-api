"""ASGI entrypoint: ``uvicorn app.main:app``."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.routes import documents, health, users
from app.bootstrap import build_container
from app.core.config import Settings, get_settings
from app.core.errors import DomainError, RateLimited
from app.core.logging import configure_logging, request_id_var

logger = logging.getLogger(__name__)


def _error(status_code: int, code: str, message: str, details: object = None) -> JSONResponse:
    body: dict[str, object] = {"error": {"code": code, "message": message}, "request_id": request_id_var.get()}
    if details:
        body["error"]["details"] = details  # type: ignore[index]
    return JSONResponse(status_code=status_code, content=jsonable_encoder(body))


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level)
        app.state.container = await build_container(settings)
        logger.info("api started", extra={"environment": settings.environment})
        try:
            yield
        finally:
            await app.state.container.close()
            logger.info("api stopped")

    app = FastAPI(
        title="Cohort Insights API",
        version="1.0.0",
        description="Ingests documents, runs a two-stage async pipeline, serves version-consistent results.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        try:
            response = await call_next(request)
            response.headers["X-Request-Id"] = request_id
            logger.info(
                "request",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                },
            )
            return response
        finally:
            request_id_var.reset(token)

    @app.exception_handler(DomainError)
    async def domain_error_handler(_: Request, exc: DomainError) -> JSONResponse:
        response = _error(exc.status_code, exc.code, exc.message, exc.details)
        if isinstance(exc, RateLimited):
            response.headers["Retry-After"] = "5"
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return _error(422, "validation_error", "request validation failed", {"errors": exc.errors()})

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error", extra={"path": request.url.path})
        return _error(500, "internal_error", "internal server error")

    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(users.router)
    return app


app = create_app()
