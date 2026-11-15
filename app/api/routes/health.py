"""Health endpoints.

* ``/health/live``  -- process is up (liveness probe; no dependencies).
* ``/health``       -- readiness. MongoDB is required (503 if down). Redis is an
  accelerator with fallbacks, so a Redis outage reports ``degraded`` with 200:
  the service still serves correct responses and should stay in rotation.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Request, Response, status
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.api.deps import get_container
from app.api.schemas import HealthView

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])

_CHECK_TIMEOUT_S = 2.0


@router.get("/health/live", response_model=HealthView)
async def live() -> HealthView:
    return HealthView(status="ok", checks={})


@router.get("/health", response_model=HealthView, responses={503: {"model": HealthView}})
async def health(request: Request, response: Response) -> HealthView:
    container = get_container(request)
    checks: dict[str, str] = {}

    try:
        await asyncio.wait_for(container.mongo.admin.command("ping"), _CHECK_TIMEOUT_S)
        checks["mongodb"] = "ok"
    except (PyMongoError, TimeoutError) as exc:
        logger.warning("health: mongodb check failed", extra={"error": repr(exc)})
        checks["mongodb"] = "unavailable"

    try:
        await asyncio.wait_for(container.redis.ping(), _CHECK_TIMEOUT_S)
        checks["redis"] = "ok"
    except (RedisError, OSError, TimeoutError) as exc:
        logger.warning("health: redis check failed", extra={"error": repr(exc)})
        checks["redis"] = "unavailable"

    if checks["mongodb"] != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthView(status="unavailable", checks=checks)
    return HealthView(status="ok" if checks["redis"] == "ok" else "degraded", checks=checks)
