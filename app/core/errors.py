"""Domain errors. Services raise these; one exception handler maps them to HTTP.

Keeping HTTP concerns out of the service layer means the worker and the API
share the same business logic without importing FastAPI.
"""

from __future__ import annotations

from typing import Any


class DomainError(Exception):
    status_code: int = 400
    code: str = "bad_request"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class Unauthenticated(DomainError):
    status_code = 401
    code = "unauthenticated"


class Forbidden(DomainError):
    status_code = 403
    code = "forbidden"


class NotFound(DomainError):
    """Also used for "exists but owned by someone else" -- deliberately indistinguishable."""

    status_code = 404
    code = "not_found"


class Conflict(DomainError):
    status_code = 409
    code = "conflict"


class VersionConflict(Conflict):
    """Optimistic-concurrency precondition (expected_content_version) did not hold."""

    code = "version_conflict"


class StaleClientVersion(Conflict):
    """Partner resubmission carries a client_version older than the one already applied."""

    code = "stale_client_version"


class ClientVersionRequired(Conflict):
    """Ref is version-ordered; an unversioned resubmission could silently reorder history."""

    code = "client_version_required"


class InvalidState(Conflict):
    code = "invalid_state"


class ConcurrentModification(Conflict):
    """Compare-and-swap retries exhausted under heavy contention on one document."""

    code = "concurrent_modification"


class RateLimited(DomainError):
    status_code = 429
    code = "rate_limited"
