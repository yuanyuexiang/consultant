from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.config import settings
from app.repositories.storage import StorageRepository
from app.schemas.common import ApiResponse, ErrorDetail
from app.schemas.report import (
    AssembleData,
    AssembleRequest,
    PublishData,
    PublishRequest,
    ReportListData,
    ReportListItem,
    ReportPayloadData,
    SectionPayloadData,
    UploadExcelData,
)
from app.services.assembly_service import assemble_report
from app.services.normalize_service import normalize_points
from app.services.parse_service import parse_excel
from app.services.publish_service import payload_hash
from app.validators.report_validator import validate_report_payload

router = APIRouter(prefix="/v1/reports", tags=["reports"])
repo = StorageRepository()


def _error(status_code: int, code: int, field: str, detail: str) -> HTTPException:
    payload = ApiResponse(
        code=code,
        message="invalid request",
        error=ErrorDetail(field=field, detail=detail),
    ).model_dump()
    return HTTPException(status_code=status_code, detail=payload)


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


@router.post("/assemble", response_model=ApiResponse[AssembleData])
def assemble_report_api(req: AssembleRequest):
    try:
        parsed_record = repo.load_parsed(req.report_key)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc

    parsed = parsed_record["payload"]
    normalized = normalize_points(parsed)
    payload = assemble_report(parsed, normalized)

    try:
        validate_report_payload(payload)
    except ValueError as exc:
        raise _error(422, 1002, "payload", str(exc)) from exc

    snapshot_id = repo.next_snapshot_id()
    digest = payload_hash(payload)
    repo.save_snapshot(
        req.report_key,
        snapshot_id,
        digest,
        payload,
        parsed_record.get("source_file", "unknown.xlsx"),
    )

    data = AssembleData(report_key=req.report_key, snapshot_id=snapshot_id, payload_hash=digest)
    return ApiResponse(data=data)


@router.post("/{report_key}/publish", response_model=ApiResponse[PublishData])
def publish_report(report_key: str, req: PublishRequest):
    try:
        snap = repo.load_snapshot(report_key, req.snapshot_id)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "snapshot_id", str(exc)) from exc

    version = repo.update_publish(report_key, req.snapshot_id, snap["payload"])
    data = PublishData(
        report_key=report_key,
        published_version=version,
        snapshot_id=req.snapshot_id,
    )
    return ApiResponse(data=data)


@router.get("", response_model=ApiResponse[ReportListData])
def list_reports():
    items = [ReportListItem(**x) for x in repo.list_reports()]
    return ApiResponse(data=ReportListData(items=items))


@router.get("/{report_key}", response_model=ApiResponse[ReportPayloadData])
def get_report(report_key: str):
    try:
        snapshot_id = repo.get_published_snapshot_id(report_key)
        snap = repo.load_snapshot(report_key, snapshot_id)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc
    return ApiResponse(data=ReportPayloadData(payload=snap["payload"]))


@router.get("/{report_key}/sections/{section_key}", response_model=ApiResponse[SectionPayloadData])
def get_section(report_key: str, section_key: str):
    try:
        snapshot_id = repo.get_published_snapshot_id(report_key)
        snap = repo.load_snapshot(report_key, snapshot_id)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc

    sections = snap["payload"].get("sections", [])
    for sec in sections:
        if sec.get("section_key") == section_key:
            return ApiResponse(data=SectionPayloadData(section_key=section_key, section=sec))

    raise _error(404, 1004, "section_key", f"section not found: {section_key}")
