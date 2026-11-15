from __future__ import annotations

from fastapi import APIRouter, Response, status

from app.api.deps import CallerId, Documents
from app.api.schemas import (
    DocumentView,
    ErrorResponse,
    SubmitDocumentRequest,
    SubmitDocumentResponse,
    UpdateDocumentRequest,
    to_submit_response,
    to_view,
)
from app.core.errors import Forbidden
from app.domain.enums import SubmitOutcome

router = APIRouter(prefix="/documents", tags=["documents"])

_ERRORS = {
    401: {"model": ErrorResponse},
    404: {"model": ErrorResponse},
    409: {"model": ErrorResponse},
    422: {"model": ErrorResponse},
    429: {"model": ErrorResponse},
}


@router.post(
    "",
    response_model=SubmitDocumentResponse,
    status_code=status.HTTP_201_CREATED,
    responses={200: {"model": SubmitDocumentResponse, "description": "Repeat client_doc_ref"}, 403: {"model": ErrorResponse}, **_ERRORS},
    summary="Submit a document (201 new; 200 for a repeat client_doc_ref)",
)
async def submit_document(
    body: SubmitDocumentRequest, caller: CallerId, documents: Documents, response: Response
) -> SubmitDocumentResponse:
    if body.user_id != caller:
        # Writes on behalf of another user are refused outright. 403 (not 404)
        # is safe here: it reveals nothing about any existing document.
        raise Forbidden("user_id must match the authenticated caller")
    result = await documents.submit(
        user_id=body.user_id,
        title=body.title,
        content=body.content,
        client_doc_ref=body.client_doc_ref,
        client_version=body.client_version,
    )
    if result.outcome is not SubmitOutcome.CREATED:
        response.status_code = status.HTTP_200_OK
    response.headers["Location"] = f"/documents/{result.doc['_id']}"
    return to_submit_response(result.doc, result.outcome, result.cache_hit)


@router.get(
    "/by-ref/{client_doc_ref}",
    response_model=DocumentView,
    responses=_ERRORS,
    summary="Resolve a partner client_doc_ref (index-backed)",
)
async def get_by_ref(client_doc_ref: str, caller: CallerId, documents: Documents) -> DocumentView:
    return to_view(await documents.get_by_ref(client_doc_ref, caller))


@router.get("/{document_id}", response_model=DocumentView, responses=_ERRORS, summary="Poll status and result")
async def get_document(document_id: str, caller: CallerId, documents: Documents) -> DocumentView:
    return to_view(await documents.get(document_id, caller))


@router.patch(
    "/{document_id}",
    response_model=SubmitDocumentResponse,
    responses=_ERRORS,
    summary="Replace content in place and reprocess both stages",
)
async def update_document(
    document_id: str, body: UpdateDocumentRequest, caller: CallerId, documents: Documents
) -> SubmitDocumentResponse:
    result = await documents.update_content(
        document_id, caller, body.content, body.expected_content_version
    )
    return to_submit_response(result.doc, result.outcome, result.cache_hit)


@router.post(
    "/{document_id}/retry",
    response_model=DocumentView,
    status_code=status.HTTP_202_ACCEPTED,
    responses=_ERRORS,
    summary="Resume a failed document from the stage that failed",
)
async def retry_document(document_id: str, caller: CallerId, documents: Documents) -> DocumentView:
    return to_view(await documents.retry(document_id, caller))
