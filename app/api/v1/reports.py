from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import re
from threading import Lock
from uuid import uuid4
from collections import defaultdict
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

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
    UploadFolderData,
    UploadFolderFileResult,
    UploadFolderTaskAcceptedData,
    UploadFolderTaskStatusData,
)
from app.services.duckdb_service import duckdb_service
from app.services.hash_service import payload_hash
from app.services.parse_service import parse_excel

router = APIRouter(prefix="/v1/reports", tags=["reports"])
repo = StorageRepository()
upload_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="upload-folder")
upload_tasks: dict[str, dict[str, Any]] = {}
upload_tasks_lock = Lock()


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


def _parse_chapter_section_from_name(name: str, fallback_index: int) -> tuple[str, str, int, int]:
    stem = Path(name).stem
    match = re.search(r"chapter\s*(\d+)\s*[_-]?\s*section\s*(\d+)", stem, flags=re.IGNORECASE)
    if match:
        chapter_order = int(match.group(1))
        section_order = int(match.group(2))
        return (
            f"chapter_{chapter_order}",
            f"section_{section_order}",
            chapter_order,
            section_order,
        )

    return ("chapter_1", f"section_{fallback_index}", 1, fallback_index)


def _first_section(payload: dict[str, Any]) -> dict[str, Any] | None:
    chapters = payload.get("chapters")
    if not isinstance(chapters, list):
        return None

    for chapter in chapters:
        if not isinstance(chapter, dict):
            continue
        sections = chapter.get("sections")
        if not isinstance(sections, list):
            continue
        for section in sections:
            if isinstance(section, dict):
                return section

    return None


def _merge_chapters(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged_map: dict[str, dict[str, Any]] = {}
    for chapter in existing:
        if not isinstance(chapter, dict):
            continue
        chapter_key = _text(chapter.get("chapter_key")) or "chapter_1"
        chapter_copy = dict(chapter)
        sections = chapter_copy.get("sections")
        chapter_copy["sections"] = list(sections) if isinstance(sections, list) else []
        merged_map[chapter_key] = chapter_copy

    for chapter in incoming:
        if not isinstance(chapter, dict):
            continue
        chapter_key = _text(chapter.get("chapter_key")) or "chapter_1"
        target = merged_map.get(chapter_key)
        incoming_sections = chapter.get("sections")
        if not isinstance(incoming_sections, list):
            incoming_sections = []

        if target is None:
            merged_map[chapter_key] = {
                "chapter_key": chapter_key,
                "title": chapter.get("title") or chapter_key,
                "subtitle": chapter.get("subtitle"),
                "order": int(chapter.get("order", 1)),
                "status": chapter.get("status", "active"),
                "sections": [dict(sec) for sec in incoming_sections if isinstance(sec, dict)],
            }
            continue

        target_sections = target.get("sections")
        if not isinstance(target_sections, list):
            target_sections = []

        index_by_key = {
            _text(sec.get("section_key")): idx
            for idx, sec in enumerate(target_sections)
            if isinstance(sec, dict)
        }

        for section in incoming_sections:
            if not isinstance(section, dict):
                continue
            section_key = _text(section.get("section_key"))
            if section_key and section_key in index_by_key:
                target_sections[index_by_key[section_key]] = dict(section)
            else:
                target_sections.append(dict(section))

        target["sections"] = target_sections

    merged = list(merged_map.values())
    merged.sort(key=lambda c: int(c.get("order", 1)))
    for chapter in merged:
        sections = chapter.get("sections")
        if isinstance(sections, list):
            sections.sort(key=lambda sec: int((sec or {}).get("order", 1)))
    return merged


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


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _line_type(value: str) -> str | list[int]:
    if value == "dashed":
        return "dashed"
    if value == "dotted":
        return "dotted"
    if value == "dashdot":
        return [6, 3, 1, 3]
    return "solid"


def _symbol(value: str) -> str:
    if value == "circle":
        return "circle"
    if value == "square":
        return "rect"
    if value == "diamond":
        return "diamond"
    if value == "triangle":
        return "triangle"
    return "none"


def _series_type(value: str) -> str:
    return "line" if "line" in value else "scatter"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _set_upload_task(task_id: str, **updates: Any) -> dict[str, Any]:
    with upload_tasks_lock:
        task = upload_tasks.get(task_id)
        if task is None:
            raise KeyError(task_id)
        task.update(updates)
        upload_tasks[task_id] = task
        return dict(task)


def _get_upload_task(task_id: str) -> dict[str, Any] | None:
    with upload_tasks_lock:
        task = upload_tasks.get(task_id)
        if task is None:
            return None
        return dict(task)


def _append_upload_result(task_id: str, result: UploadFolderFileResult) -> None:
    with upload_tasks_lock:
        task = upload_tasks.get(task_id)
        if task is None:
            return
        rows = list(task.get("files", []))
        rows.append(result.model_dump())
        task["files"] = rows
        task["processed_files"] = int(task.get("processed_files", 0)) + 1
        if result.status == "success":
            task["succeeded_files"] = int(task.get("succeeded_files", 0)) + 1
        else:
            task["failed_files"] = int(task.get("failed_files", 0)) + 1
        upload_tasks[task_id] = task


def _build_task_status(task: dict[str, Any]) -> UploadFolderTaskStatusData:
    result_payload = task.get("result")
    result = UploadFolderData(**result_payload) if isinstance(result_payload, dict) else None
    return UploadFolderTaskStatusData(
        task_id=str(task["task_id"]),
        report_key=str(task["report_key"]),
        status=str(task["status"]),
        phase=str(task.get("phase") or "queued"),
        total_files=int(task.get("total_files", 0)),
        processed_files=int(task.get("processed_files", 0)),
        succeeded_files=int(task.get("succeeded_files", 0)),
        failed_files=int(task.get("failed_files", 0)),
        submitted_at=str(task.get("submitted_at") or ""),
        started_at=task.get("started_at"),
        finished_at=task.get("finished_at"),
        files=[UploadFolderFileResult(**row) for row in task.get("files", [])],
        detail=task.get("detail"),
        result=result,
    )


def _run_upload_folder_task(
    task_id: str,
    report_key: str,
    report_name: str,
    mode: str,
    staged_files: list[tuple[str, Path]],
) -> None:
    _set_upload_task(
        task_id,
        status="running",
        phase="parsing",
        started_at=_utc_now_iso(),
        detail=None,
    )

    try:
        chapter_buckets: dict[str, dict[str, Any]] = {}
        fallback_index = 1

        for filename, path in staged_files:
            chapter_key, section_key, chapter_order, section_order = _parse_chapter_section_from_name(
                filename,
                fallback_index,
            )
            fallback_index += 1

            try:
                parsed = parse_excel(path, override_report_key=report_key)
                # Keep parsed artifact for debugging and replay.
                repo.save_parsed(report_key, filename, parsed)

                assembled = parsed.get("assembled_payload")
                if not isinstance(assembled, dict):
                    raise ValueError("assembled payload missing")
                section = _first_section(assembled)
                if not isinstance(section, dict):
                    raise ValueError("section payload missing")
            except ValueError as exc:
                _append_upload_result(
                    task_id,
                    UploadFolderFileResult(
                        source_file=filename,
                        chapter_key=chapter_key,
                        section_key=section_key,
                        status="failed",
                        detail=str(exc),
                    ),
                )
                continue

            section_copy = dict(section)
            section_copy["chapter_key"] = chapter_key
            section_copy["section_key"] = section_key
            section_copy["title"] = section_copy.get("title") or Path(filename).stem
            section_copy["content"] = section_copy.get("content") or ""
            section_copy["order"] = section_order
            section_copy["content_items"] = section_copy.get("content_items") or {
                "charts": [],
                "kind": None,
                "items": None,
            }

            bucket = chapter_buckets.get(chapter_key)
            if bucket is None:
                bucket = {
                    "chapter_key": chapter_key,
                    "title": chapter_key,
                    "subtitle": None,
                    "order": chapter_order,
                    "status": "active",
                    "sections": [],
                }
                chapter_buckets[chapter_key] = bucket
            bucket["sections"].append(section_copy)

            _append_upload_result(
                task_id,
                UploadFolderFileResult(
                    source_file=filename,
                    chapter_key=chapter_key,
                    section_key=section_key,
                    parsed_charts=len(parsed.get("charts", [])),
                    parsed_points=len(parsed.get("chart_points", [])),
                    status="success",
                ),
            )

        task_snapshot = _get_upload_task(task_id)
        if task_snapshot is None:
            return

        success_count = int(task_snapshot.get("succeeded_files", 0))
        total_files = int(task_snapshot.get("total_files", 0))
        failure_count = int(task_snapshot.get("failed_files", 0))
        rows = [UploadFolderFileResult(**row) for row in task_snapshot.get("files", [])]

        if success_count == 0:
            first_detail = next((row.detail for row in rows if row.detail), "no valid files uploaded")
            _set_upload_task(
                task_id,
                status="failed",
                phase="failed",
                detail=first_detail,
                finished_at=_utc_now_iso(),
            )
            return

        incoming_chapters = list(chapter_buckets.values())
        incoming_chapters.sort(key=lambda chapter: int(chapter.get("order", 1)))
        for chapter in incoming_chapters:
            sections = chapter.get("sections")
            if isinstance(sections, list):
                sections.sort(key=lambda sec: int((sec or {}).get("order", 1)))

        payload = {
            "id": f"rpt_{report_key.replace('-', '_')}",
            "report_key": report_key,
            "name": report_name,
            "type": "analytics",
            "status": "active",
            "chapters": incoming_chapters,
        }

        if mode == "append" and repo.exists_report(report_key):
            existing = repo.load_report(report_key)
            existing_payload = existing.get("payload") if isinstance(existing, dict) else None
            if isinstance(existing_payload, dict):
                existing_chapters = existing_payload.get("chapters")
                payload = {
                    **existing_payload,
                    "name": report_name,
                    "chapters": _merge_chapters(
                        existing_chapters if isinstance(existing_chapters, list) else [],
                        incoming_chapters,
                    ),
                }

        _set_upload_task(task_id, phase="persisting")
        digest = payload_hash(payload)
        saved_at = repo.save_report(report_key, payload, digest)
        duckdb_service.replace_report_rows(report_key, payload)
        repo.upsert_report_index(
            report_key,
            payload,
            status=payload.get("status", "active"),
            payload_hash=digest,
            saved_at=saved_at,
        )

        _set_upload_task(
            task_id,
            status="succeeded",
            phase="completed",
            finished_at=_utc_now_iso(),
            result=UploadFolderData(
                report_key=report_key,
                total_files=total_files,
                succeeded_files=success_count,
                failed_files=failure_count,
                files=rows,
            ).model_dump(),
        )
    except Exception as exc:  # noqa: BLE001
        _set_upload_task(
            task_id,
            status="failed",
            phase="failed",
            detail=str(exc),
            finished_at=_utc_now_iso(),
        )


def _normalize_filter_value(value: Any) -> str:
    text = _text(value)
    if not text or text.lower() == "all":
        return "ALL"
    return text


def _collect_category_x(rows: list[dict[str, Any]]) -> list[str | float]:
    ordered: list[str | float] = []
    seen: set[str] = set()

    for row in rows:
        raw = row.get("x")
        if raw is None:
            continue
        as_num = _number(raw)
        value: str | float = as_num if as_num is not None else _text(raw)
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(value)

    if ordered and all(isinstance(item, (int, float)) for item in ordered):
        return sorted(float(item) for item in ordered)

    return ordered


def _build_filtered_option(original: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    x_axis = original.get("xAxis")
    if isinstance(x_axis, list):
        x_axis = x_axis[0] if x_axis else {}
    if not isinstance(x_axis, dict):
        x_axis = {}

    is_time = x_axis.get("type") == "time"

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_text(row.get("legend")) or "Series"].append(row)

    x_values = [] if is_time else _collect_category_x(rows)
    series: list[dict[str, Any]] = []

    for legend, points in grouped.items():
        style = points[0] if points else {}
        if is_time:
            data = [
                [_text(item.get("x")), y]
                for item in points
                if (y := _number(item.get("y"))) is not None
            ]
        else:
            point_map: dict[str, float] = {}
            for item in points:
                y = _number(item.get("y"))
                x = _text(item.get("x"))
                if y is None or not x:
                    continue
                point_map[x] = y
            data = [point_map.get(str(x_value)) for x_value in x_values]

        series.append(
            {
                "name": legend,
                "type": _series_type(_text(style.get("type")) or "line"),
                "data": data,
                "connectNulls": True,
                "showSymbol": "point" in (_text(style.get("type")) or "line"),
                "symbol": _symbol(_text(style.get("shape")) or "none"),
                "symbolSize": max(2, (_number(style.get("point_size")) or 0) / 2),
                "lineStyle": {
                    "type": _line_type(_text(style.get("line_style")) or "solid"),
                    "width": max(1, (_number(style.get("line_width")) or 2) / 2),
                },
                "itemStyle": {
                    "color": _text(style.get("color")) or "#5470C6",
                },
            }
        )

    return {
        **original,
        "xAxis": (
            {
                **x_axis,
                "type": "time",
            }
            if is_time
            else {
                **x_axis,
                "type": "category",
                "data": x_values,
            }
        ),
        "series": series,
    }


def _apply_chart_filters(
    report_key: str,
    section_key: str,
    chart: dict[str, Any],
    filter1: str,
    filter2: str,
) -> dict[str, Any]:
    chart_copy = dict(chart)
    chart_id = _text(chart_copy.get("chart_id"))
    rows: list[dict[str, Any]] = []

    if chart_id:
        rows = duckdb_service.query_chart_rows(report_key, section_key, chart_id, filter1, filter2)

    if not rows:
        meta = chart_copy.get("meta") if isinstance(chart_copy.get("meta"), dict) else {}
        chart_copy["meta"] = {
            **meta,
            "selected_filters": {"filter1": filter1, "filter2": filter2},
            "filtered_rows_count": 0,
        }
        if chart_copy.get("chart_type") != "table":
            option = chart_copy.get("echarts")
            if isinstance(option, dict):
                chart_copy["echarts"] = _build_filtered_option(option, [])
        else:
            table_data = chart_copy.get("table_data")
            if isinstance(table_data, dict):
                chart_copy["table_data"] = {**table_data, "rows": []}
        return chart_copy

    if chart_copy.get("chart_type") != "table":
        option = chart_copy.get("echarts")
        if isinstance(option, dict):
            chart_copy["echarts"] = _build_filtered_option(option, rows)
    else:
        table_rows: list[dict[str, Any]] = []
        for row in rows:
            raw_row = row.get("_raw_row")
            if isinstance(raw_row, dict):
                table_rows.append(raw_row)
                continue
            table_rows.append(
                {
                    "x": row.get("x"),
                    "y": row.get("y"),
                    "legend": row.get("legend"),
                    "filter1": row.get("filter1"),
                    "filter2": row.get("filter2"),
                }
            )

        table_data = chart_copy.get("table_data")
        columns: list[dict[str, str]] = []
        if isinstance(table_data, dict) and isinstance(table_data.get("columns"), list):
            columns = [item for item in table_data.get("columns", []) if isinstance(item, dict)]
        if not columns and table_rows:
            columns = [{"key": key, "title": key} for key in table_rows[0].keys()]

        chart_copy["table_data"] = {
            "columns": columns,
            "rows": table_rows,
        }

    meta = chart_copy.get("meta") if isinstance(chart_copy.get("meta"), dict) else {}
    chart_copy["meta"] = {
        **meta,
        "selected_filters": {"filter1": filter1, "filter2": filter2},
        "filtered_rows_count": len(rows),
    }
    return chart_copy


def _apply_section_filters(
    report_key: str,
    section: dict[str, Any],
    filter1: str,
    filter2: str,
) -> dict[str, Any]:
    section_copy = dict(section)
    section_key = _text(section_copy.get("section_key"))
    content_items = section_copy.get("content_items")
    if not isinstance(content_items, dict):
        return section_copy

    charts = content_items.get("charts")
    if not isinstance(charts, list):
        return section_copy

    filtered_charts = [
        _apply_chart_filters(report_key, section_key, chart, filter1, filter2)
        for chart in charts
        if isinstance(chart, dict)
    ]
    section_copy["content_items"] = {**content_items, "charts": filtered_charts}

    section_meta = section_copy.get("meta") if isinstance(section_copy.get("meta"), dict) else {}
    section_copy["meta"] = {
        **section_meta,
        "selected_filters": {"filter1": filter1, "filter2": filter2},
    }
    return section_copy


def _find_section(payload: dict[str, Any], section_key: str) -> dict[str, Any] | None:
    sections = payload.get("sections", [])
    if isinstance(sections, list):
        for sec in sections:
            if isinstance(sec, dict) and sec.get("section_key") == section_key:
                return sec

    chapters = payload.get("chapters", [])
    if isinstance(chapters, list):
        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            for sec in chapter.get("sections", []):
                if isinstance(sec, dict) and sec.get("section_key") == section_key:
                    return sec
    return None


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
    duckdb_service.replace_report_rows(req.report_key, payload)
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
    duckdb_service.replace_report_rows(report_key, payload)
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

    assembled_payload = parsed.get("assembled_payload")
    if isinstance(assembled_payload, dict):
        digest = payload_hash(assembled_payload)
        saved_at = repo.save_report(key, assembled_payload, digest)
        duckdb_service.replace_report_rows(key, assembled_payload)
        repo.upsert_report_index(
            key,
            assembled_payload,
            status=assembled_payload.get("status", "active"),
            payload_hash=digest,
            saved_at=saved_at,
        )

    parsed_charts = len(parsed.get("charts", []))
    parsed_points = len(parsed.get("chart_points", []))

    data = UploadExcelData(
        report_key=key,
        source_file=source_name,
        parsed_charts=parsed_charts,
        parsed_points=parsed_points,
    )
    return ApiResponse(data=data)


@router.post("/upload-folder", response_model=ApiResponse[UploadFolderTaskAcceptedData])
async def upload_folder(
    files: list[UploadFile] = File(...),
    report_key: str = Form(...),
    report_name: str = Form(...),
    mode: str = Form(default="replace"),
):
    if mode not in {"replace", "append"}:
        raise _error(400, 1001, "mode", "mode must be 'replace' or 'append'")
    if not files:
        raise _error(400, 1001, "files", "at least one file is required")

    report_name_text = _text(report_name)
    if not report_name_text:
        raise _error(400, 1001, "report_name", "report_name is required")

    key_raw = _text(report_key)
    if not key_raw:
        raise _error(400, 1001, "report_key", "report_key is required")

    key = re.sub(r"[^a-zA-Z0-9_-]+", "-", key_raw).strip("-")
    if not key:
        raise _error(400, 1001, "report_key", "report_key is invalid")

    task_id = uuid4().hex
    task_upload_root = settings.upload_dir / key
    task_upload_root.mkdir(parents=True, exist_ok=True)

    staged_files: list[tuple[str, Path]] = []
    initial_rows: list[dict[str, Any]] = []

    for item in files:
        filename = Path(item.filename or "").name
        if not filename:
            filename = f"unknown-{uuid4().hex[:8]}.xlsx"

        if not filename.lower().endswith(".xlsx"):
            initial_rows.append(
                UploadFolderFileResult(
                    source_file=filename,
                    status="failed",
                    detail="unsupported file extension",
                ).model_dump()
            )
            continue

        content = await item.read()
        dest = task_upload_root / filename
        dest.write_bytes(content)
        staged_files.append((filename, dest))

    if not staged_files:
        detail = "no valid xlsx files uploaded"
        raise _error(400, 1001, "files", detail)

    now = _utc_now_iso()
    queued = {
        "task_id": task_id,
        "report_key": key,
        "status": "queued",
        "phase": "queued",
        "total_files": len(files),
        "processed_files": len(initial_rows),
        "succeeded_files": 0,
        "failed_files": len(initial_rows),
        "submitted_at": now,
        "started_at": None,
        "finished_at": None,
        "files": initial_rows,
        "detail": None,
        "result": None,
    }
    with upload_tasks_lock:
        upload_tasks[task_id] = queued

    upload_executor.submit(_run_upload_folder_task, task_id, key, report_name_text, mode, staged_files)

    return ApiResponse(
        data=UploadFolderTaskAcceptedData(
            task_id=task_id,
            report_key=key,
            status="queued",
            total_files=len(files),
            submitted_at=now,
        )
    )


@router.get("/upload-folder/tasks/{task_id}", response_model=ApiResponse[UploadFolderTaskStatusData])
def get_upload_folder_task(task_id: str):
    task = _get_upload_task(task_id)
    if task is None:
        raise _error(404, 1004, "task_id", f"upload task not found: {task_id}")
    return ApiResponse(data=_build_task_status(task))


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
def get_section(
    report_key: str,
    section_key: str,
    filter1: str = Query(default="ALL"),
    filter2: str = Query(default="ALL"),
):
    try:
        report_doc = repo.load_report(report_key)
    except FileNotFoundError as exc:
        raise _error(404, 1004, "report_key", str(exc)) from exc

    section = _find_section(report_doc["payload"], section_key)
    if section is not None:
        normalized_filter1 = _normalize_filter_value(filter1)
        normalized_filter2 = _normalize_filter_value(filter2)
        filtered = _apply_section_filters(
            report_key,
            section,
            normalized_filter1,
            normalized_filter2,
        )
        return ApiResponse(data=SectionPayloadData(section_key=section_key, section=filtered))

    raise _error(404, 1004, "section_key", f"section not found: {section_key}")
