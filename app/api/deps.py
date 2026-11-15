"""Dependency injection.

Caller identity: the assignment specifies no auth scheme, so the caller is
identified by an ``X-User-Id`` header -- a stand-in for the subject claim an
API gateway would extract from a verified JWT. Everything downstream only ever
sees ``CallerId``, so swapping in real token verification touches this file only.
"""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import Depends, Header, Request

from app.api.schemas import USER_ID_PATTERN
from app.bootstrap import Container
from app.core.config import Settings
from app.core.errors import Unauthenticated
from app.services.documents import DocumentService

_USER_ID_RE = re.compile(USER_ID_PATTERN)


def get_container(request: Request) -> Container:
    return request.app.state.container


def get_settings_dep(container: Annotated[Container, Depends(get_container)]) -> Settings:
    return container.settings


def get_document_service(container: Annotated[Container, Depends(get_container)]) -> DocumentService:
    return container.documents


def get_caller_id(
    x_user_id: Annotated[str | None, Header(alias="X-User-Id", description="Authenticated caller id")] = None,
) -> str:
    if x_user_id is None or not _USER_ID_RE.fullmatch(x_user_id):
        raise Unauthenticated("missing or malformed X-User-Id header")
    return x_user_id


CallerId = Annotated[str, Depends(get_caller_id)]
Documents = Annotated[DocumentService, Depends(get_document_service)]
AppSettings = Annotated[Settings, Depends(get_settings_dep)]
