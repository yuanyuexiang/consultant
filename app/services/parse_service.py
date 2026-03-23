from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_SHEETS = {
    "report_meta": ["report_key", "name", "type", "status"],
    "sections": ["section_key", "title", "subtitle", "order_no", "layout"],
    "charts": [
        "chart_id",
        "section_key",
        "chart_type",
        "title",
        "subtitle",
        "formatter",
        "option_template_json",
    ],
    "chart_points": ["chart_id", "series_name", "point_time", "metric_value"],
}


def _validate_columns(df: pd.DataFrame, cols: list[str], sheet: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"sheet '{sheet}' missing columns: {', '.join(missing)}")


def _load_excel(path: Path) -> dict[str, pd.DataFrame]:
    try:
        xls = pd.ExcelFile(path)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"failed to read excel: {exc}") from exc

    frames: dict[str, pd.DataFrame] = {}
    for sheet, cols in REQUIRED_SHEETS.items():
        if sheet not in xls.sheet_names:
            raise ValueError(f"missing required sheet: {sheet}")
        df = pd.read_excel(xls, sheet_name=sheet)
        _validate_columns(df, cols, sheet)
        frames[sheet] = df
    return frames


def _coerce_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw in df.to_dict(orient="records"):
        item = {k: _coerce_scalar(v) for k, v in raw.items()}
        records.append(item)
    return records


def parse_excel(path: Path, override_report_key: str | None = None) -> dict[str, Any]:
    frames = _load_excel(path)

    report_meta = _df_to_records(frames["report_meta"])
    if not report_meta:
        raise ValueError("sheet 'report_meta' is empty")

    parsed = {
        "report_meta": report_meta[0],
        "sections": _df_to_records(frames["sections"]),
        "charts": _df_to_records(frames["charts"]),
        "chart_points": _df_to_records(frames["chart_points"]),
    }

    if override_report_key:
        parsed["report_meta"]["report_key"] = override_report_key

    # Validate option_template_json basic JSON syntax when provided.
    for chart in parsed["charts"]:
        raw = chart.get("option_template_json")
        if raw in (None, ""):
            chart["option_template_json"] = {}
            continue
        if isinstance(raw, dict):
            continue
        try:
            chart["option_template_json"] = json.loads(str(raw))
        except json.JSONDecodeError as exc:
            chart_id = chart.get("chart_id", "unknown")
            raise ValueError(
                f"chart '{chart_id}' option_template_json invalid JSON: {exc}"
            ) from exc

    return parsed
