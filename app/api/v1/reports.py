from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.config import settings
from app.repositories.storage import StorageRepository
from app.schemas.common import ApiResponse, ErrorDetail
from app.schemas.report import (
    DeleteReportData,
    ReportCreateRequest,
    ReportListData,
    ReportListItem,
    ReportMutationData,
    ReportPayloadData,
    ReportUpdateRequest,
    SectionPayloadData,
    UploadExcelData,
)
from app.services.hash_service import payload_hash
from app.services.parse_service import parse_excel

router = APIRouter(prefix="/v1/reports", tags=["reports"])
repo = StorageRepository()


def _flatten_chapter_sections(chapters: list[dict]) -> list[dict]:
    sections: list[dict] = []
    for chapter in chapters:
        chapter_key = chapter.get("chapter_key")
        for sec in chapter.get("sections", []):
            if chapter_key and not sec.get("chapter_key"):
                sec["chapter_key"] = chapter_key
            sections.append(sec)
    return sections


def _build_chapters_from_sections(sections: list[dict]) -> list[dict]:
    by_key: dict[str, list[dict]] = {}
    for sec in sections:
        chapter_key = sec.get("chapter_key") or "chapter_1"
        sec_copy = dict(sec)
        sec_copy["chapter_key"] = chapter_key
        by_key.setdefault(chapter_key, []).append(sec_copy)

    chapters: list[dict] = []
    for idx, (chapter_key, secs) in enumerate(by_key.items(), start=1):
        chapters.append(
            {
                "chapter_key": chapter_key,
                "title": chapter_key,
                "subtitle": None,
                "order": idx,
                "status": "draft",
                "sections": secs,
            }
        )
    return chapters


def _normalize_chapters(chapters: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    for chapter in chapters:
        chapter_key = chapter.get("chapter_key") or "chapter_1"
        chapter_sections = []
        for sec in chapter.get("sections", []):
            sec_copy = dict(sec)
            sec_copy["chapter_key"] = sec_copy.get("chapter_key") or chapter_key
            # Keep a dedicated narrative field for descriptive text.
            sec_copy["content"] = sec_copy.get("content") or ""
            sec_copy["content_items"] = sec_copy.get("content_items") or {
                "charts": [],
                "kind": None,
                "items": None,
            }
            chapter_sections.append(sec_copy)

        normalized.append(
            {
                "chapter_key": chapter_key,
                "title": chapter.get("title") or chapter_key,
                "subtitle": chapter.get("subtitle"),
                "order": int(chapter.get("order", 1)),
                "status": chapter.get("status", "active"),
                "sections": chapter_sections,
            }
        )
    return normalized


def _error(status_code: int, code: int, field: str, detail: str) -> HTTPException:
    payload = ApiResponse(
        code=code,
        message="invalid request",
        error=ErrorDetail(field=field, detail=detail),
    ).model_dump()
    return HTTPException(status_code=status_code, detail=payload)


@router.post("", response_model=ApiResponse[ReportMutationData])
def create_report(req: ReportCreateRequest):
    if repo.exists_report(req.report_key):
        raise _error(409, 1003, "report_key", f"report already exists: {req.report_key}")

    chapters = req.chapters
    if not chapters:
        chapters = _build_chapters_from_sections(req.sections)
    chapters = _normalize_chapters(chapters)

    payload = {
        "id": req.id or f"rpt_{req.report_key.replace('-', '_')}",
        "report_key": req.report_key,
        "name": req.name,
        "type": req.type,
        "status": req.status,
        "chapters": chapters,
    }

    digest = payload_hash(payload)
    saved_at = repo.save_report(req.report_key, payload, digest)
    repo.upsert_report_index(
        req.report_key,
        payload,
        status=req.status,
        payload_hash=digest,
        saved_at=saved_at,
    )

    return ApiResponse(
        data=ReportMutationData(
            report_key=req.report_key,
            payload_hash=digest,
            saved_at=saved_at,
        )
    )


@router.patch("/{report_key}", response_model=ApiResponse[ReportMutationData])
def update_report(report_key: str, req: ReportUpdateRequest):
    try:
        info = repo.get_report_info(report_key)
        report_doc = repo.load_report(report_key)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc

    payload = report_doc["payload"]
    if req.name is not None:
        payload["name"] = req.name
    if req.type is not None:
        payload["type"] = req.type
    if req.status is not None:
        payload["status"] = req.status
    if req.chapters is not None:
        payload["chapters"] = _normalize_chapters(req.chapters)
    if req.sections is not None:
        payload["chapters"] = _normalize_chapters(_build_chapters_from_sections(req.sections))

    payload.pop("sections", None)

    digest = payload_hash(payload)
    saved_at = repo.save_report(report_key, payload, digest)
    repo.upsert_report_index(
        report_key,
        payload,
        status=payload.get("status", info.get("status", "active")),
        payload_hash=digest,
        saved_at=saved_at,
    )

    return ApiResponse(
        data=ReportMutationData(
            report_key=report_key,
            payload_hash=digest,
            saved_at=saved_at,
        )
    )


@router.delete("/{report_key}", response_model=ApiResponse[DeleteReportData])
def delete_report(report_key: str):
    try:
        repo.delete_report(report_key)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc

    return ApiResponse(data=DeleteReportData(report_key=report_key, deleted=True))


@router.post("/upload-excel", response_model=ApiResponse[UploadExcelData])
async def upload_excel(file: UploadFile = File(...), report_key: str | None = Form(default=None)):
    if not file.filename.lower().endswith(".xlsx"):
        raise _error(400, 1001, "file", "unsupported excel template")

    source_name = Path(file.filename).name
    dest = settings.upload_dir / source_name
    content = await file.read()
    dest.write_bytes(content)

    try:
        parsed = parse_excel(dest, override_report_key=report_key)
    except ValueError as exc:
        raise _error(400, 1001, "file", str(exc)) from exc

    meta = parsed["report_meta"]
    key = str(meta["report_key"])
    repo.save_parsed(key, source_name, parsed)

    data = UploadExcelData(
        report_key=key,
        source_file=source_name,
        parsed_charts=len(parsed["charts"]),
        parsed_points=len(parsed["chart_points"]),
    )
    return ApiResponse(data=data)


@router.get("", response_model=ApiResponse[ReportListData])
def list_reports():
    items = [ReportListItem(**x) for x in repo.list_reports()]
    return ApiResponse(data=ReportListData(items=items))


@router.get("/{report_key}", response_model=ApiResponse[ReportPayloadData])
def get_report(report_key: str):
    try:
        report_doc = repo.load_report(report_key)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc
    return ApiResponse(data=ReportPayloadData(payload=report_doc["payload"]))


@router.get("/{report_key}/sections/{section_key}", response_model=ApiResponse[SectionPayloadData])
def get_section(report_key: str, section_key: str):
    try:
        report_doc = repo.load_report(report_key)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc

    sections = report_doc["payload"].get("sections", [])
    for sec in sections:
        if sec.get("section_key") == section_key:
            return ApiResponse(data=SectionPayloadData(section_key=section_key, section=sec))

    for chapter in report_doc["payload"].get("chapters", []):
        for sec in chapter.get("sections", []):
            if sec.get("section_key") == section_key:
                return ApiResponse(data=SectionPayloadData(section_key=section_key, section=sec))

    raise _error(404, 1004, "section_key", f"section not found: {section_key}")
