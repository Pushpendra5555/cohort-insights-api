from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from app.api.deps import AppSettings, CallerId, Documents
from app.api.schemas import DocumentPage, ErrorResponse, to_view
from app.core.errors import NotFound
from app.domain.enums import PipelineState

router = APIRouter(prefix="/users", tags=["users"])


@router.get(
    "/{user_id}/documents",
    response_model=DocumentPage,
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
    summary="List the caller's own documents, newest first",
)
async def list_user_documents(
    user_id: str,
    caller: CallerId,
    documents: Documents,
    settings: AppSettings,
    page: Annotated[int, Query(ge=1, le=10_000)] = 1,
    page_size: Annotated[int | None, Query(ge=1)] = None,
    status: Annotated[PipelineState | None, Query()] = None,
) -> DocumentPage:
    if user_id != caller:
        # Same response as for an unknown user: no user enumeration.
        raise NotFound("user not found")
    size = min(page_size or settings.default_page_size, settings.max_page_size)
    items, total = await documents.list_for_user(user_id, status, page, size)
    return DocumentPage(
        items=[to_view(d) for d in items],
        page=page,
        page_size=size,
        total=total,
        has_next=page * size < total,
    )
