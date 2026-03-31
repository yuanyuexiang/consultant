from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

TEMPLATE_V2_SHEETS = {"chart_config", "chart_data", "column_dictionary"}
TEMPLATE_V2_TABLE_SHEETS = {"chart_config", "table_data", "column_dictionary"}
LATEST_CFG_REQUIRED_FIELDS = {"chapter_name", "section_name"}


def _normalize_sheet_name(name: str) -> str:
    return str(name).strip().lower()


def _build_sheet_name_map(xls: pd.ExcelFile) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for name in xls.sheet_names:
        key = _normalize_sheet_name(name)
        if key and key not in mapped:
            mapped[key] = name
    return mapped


def _open_excel(path: Path) -> pd.ExcelFile:
    try:
        return pd.ExcelFile(path)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"failed to read excel: {exc}") from exc


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


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_cfg_map(df: pd.DataFrame) -> dict[str, str]:
    if "key" not in df.columns or "value" not in df.columns:
        raise ValueError("sheet 'chart_config' missing columns: key, value")

    cfg: dict[str, str] = {}
    for row in _df_to_records(df):
        key = _text(row.get("key")).lower()
        if not key:
            continue
        cfg[key] = _text(row.get("value"))
    return cfg


def _require_cfg_fields(cfg: dict[str, str]) -> None:
    missing = [field for field in sorted(LATEST_CFG_REQUIRED_FIELDS) if not _text(cfg.get(field))]
    if missing:
        raise ValueError(f"chart_config missing required keys: {', '.join(missing)}")


def _slugify(text: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in text.strip())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "report"


def _is_excel_serial(value: float) -> bool:
    return 20000 < value < 80000


def _excel_serial_to_date(value: float) -> datetime | None:
    if not _is_excel_serial(value):
        return None
    base = datetime(1899, 12, 30)
    try:
        return base + pd.to_timedelta(value, unit="D")
    except Exception:  # noqa: BLE001
        return None


def _to_date(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, (int, float)):
        return _excel_serial_to_date(float(value))

    text = _text(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.to_pydatetime()
    return None


def _format_date_key(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _detect_x_semantic(dictionary_rows: list[dict[str, Any]]) -> str:
    x_row = next(
        (
            row
            for row in dictionary_rows
            if _text(row.get("column_name")).lower() == "x"
        ),
        None,
    )
    if x_row is None:
        return "unknown"

    text = " ".join(
        [
            _text(x_row.get("chinese_name")).lower(),
            _text(x_row.get("description")).lower(),
            _text(x_row.get("example_values")).lower(),
        ]
    )

    if any(token in text for token in ["日期", "date", "time", "年月"]):
        return "time"
    if any(token in text for token in ["连续", "整数", "numeric", "number"]):
        return "numeric"
    return "unknown"


def _normalize_template_row(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "x": raw.get("x"),
        "y": _number(raw.get("y")),
        "panel": _text(raw.get("panel"), "Chart 1"),
        "panel_order": int(_number(raw.get("panel_order")) or 9999),
        "legend": _text(raw.get("legend"), "Series"),
        "legend_order": int(_number(raw.get("legend_order")) or 9999),
        "type": _text(raw.get("type"), "line").lower(),
        "shape": _text(raw.get("shape"), "none").lower(),
        "line_style": _text(raw.get("line_style"), "solid").lower(),
        "line_width": _number(raw.get("line_width")) or 2.0,
        "point_size": _number(raw.get("point_size")) or 0.0,
        "color": _text(raw.get("color"), "#5470C6"),
        "y_format": _text(raw.get("y_format")),
        "filter1": _text(raw.get("filter1")),
        "filter2": _text(raw.get("filter2")),
    }


def _detect_template_kind(rows: list[dict[str, Any]], x_semantic: str) -> str:
    if x_semantic == "time":
        return "timeseries"
    if x_semantic == "numeric":
        return "facet"

    if any(_text(row.get("y_format")) for row in rows):
        return "timeseries"

    date_like = sum(1 for row in rows if _to_date(row.get("x")) is not None)
    if date_like / max(len(rows), 1) > 0.7:
        return "timeseries"

    serial_like = sum(
        1
        for row in rows
        if isinstance(row.get("x"), (int, float)) and _is_excel_serial(float(row["x"]))
    )
    if serial_like / max(len(rows), 1) > 0.7:
        return "timeseries"

    return "facet"


def _normalize_rows_for_kind(rows: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    if kind != "timeseries":
        return rows

    normalized: list[dict[str, Any]] = []
    for row in rows:
        x_value = row.get("x")
        parsed_date = _to_date(x_value)
        if parsed_date is not None:
            normalized.append({**row, "x": _format_date_key(parsed_date)})
        else:
            normalized.append(row)
    return normalized


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


def _axis_label_template(y_format: str, kind: str) -> str:
    if y_format == "%":
        return "{value}%"
    if y_format == "bp":
        return "{value} bp"
    if y_format == "x":
        return "{value}x"
    if kind == "facet":
        return "{value}%"
    return "{value}"


def _filter_options(rows: list[dict[str, Any]], field: str) -> list[str]:
    values = sorted({
        _text(row.get(field))
        for row in rows
        if _text(row.get(field)) and _text(row.get(field)).lower() != "all"
    })
    if not values:
        return ["All"]
    return ["All", *values]


def _build_x_values(rows: list[dict[str, Any]], kind: str) -> list[str | float]:
    if kind == "timeseries":
        return []
    xs = [row.get("x") for row in rows if row.get("x") is not None]
    unique = list({str(item): item for item in xs}.values())

    if unique and all(isinstance(item, (int, float)) for item in unique):
        return sorted(float(item) for item in unique)

    return [str(item) for item in unique]


def _build_point_map(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    mapped: dict[str, float | None] = {}
    for row in rows:
        y = row.get("y")
        if y is None:
            continue
        mapped[str(row.get("x"))] = y
    return mapped


def _build_option_for_panel(rows: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    legend_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        legend_groups[_text(row.get("legend"), "Series")].append(row)

    x_values = _build_x_values(rows, kind)
    series: list[dict[str, Any]] = []

    legends_with_order = [
        (
            min(int(point.get("legend_order") or 9999) for point in points),
            legend,
            points,
        )
        for legend, points in legend_groups.items()
    ]
    legends_with_order.sort(key=lambda item: (item[0], item[1]))

    for _, legend, points in legends_with_order:
        style = points[0]
        point_map = _build_point_map(points)

        if kind == "timeseries":
            data = [
                [str(item.get("x")), item.get("y")]
                for item in points
                if item.get("y") is not None
            ]
        else:
            data = [point_map.get(str(x_value)) for x_value in x_values]

        chart_type = "line" if "line" in _text(style.get("type"), "line") else "scatter"

        series.append(
            {
                "name": legend,
                "type": chart_type,
                "data": data,
                "connectNulls": True,
                "showSymbol": "point" in _text(style.get("type"), "line"),
                "symbol": _symbol(_text(style.get("shape"), "none")),
                "symbolSize": max(2, (_number(style.get("point_size")) or 0) / 2),
                "lineStyle": {
                    "type": _line_type(_text(style.get("line_style"), "solid")),
                    "width": max(1, (_number(style.get("line_width")) or 2) / 2)
                    if "line" in _text(style.get("type"), "line")
                    else 0,
                },
                "itemStyle": {
                    "color": _text(style.get("color"), "#5470C6"),
                },
            }
        )

    y_format = _text(rows[0].get("y_format")) if rows else ""

    return {
        "tooltip": {"trigger": "axis"},
        "legend": {"show": True, "bottom": 0},
        "grid": {"left": 50, "right": 20, "top": 40, "bottom": 45},
        "xAxis": (
            {"type": "time", "axisLabel": {"hideOverlap": True}}
            if kind == "timeseries"
            else {"type": "category", "data": x_values}
        ),
        "yAxis": {
            "type": "value",
            "axisLabel": {"formatter": _axis_label_template(y_format, kind)},
        },
        "series": series,
        "animation": False,
    }


def _build_template_v2_payload(
    path: Path,
    cfg: dict[str, str],
    rows: list[dict[str, Any]],
    kind: str,
    report_key: str,
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_text(row.get("panel"), "Chart 1")].append(row)

    panel_entries: list[tuple[int, str, list[dict[str, Any]]]] = []
    for panel_key, panel_rows in grouped.items():
        panel_order = min(int(row.get("panel_order") or 9999) for row in panel_rows)
        panel_entries.append((panel_order, panel_key, panel_rows))
    panel_entries.sort(key=lambda item: (item[0], item[1]))

    charts: list[dict[str, Any]] = []
    for idx, (_, panel_key, panel_rows) in enumerate(panel_entries, start=1):
        first = panel_rows[0] if panel_rows else {}
        chart_id = f"chart_{idx}"

        charts.append(
            {
                "chart_id": chart_id,
                "chart_type": "line",
                "title": panel_key,
                "subtitle": None,
                "echarts": _build_option_for_panel(panel_rows, kind),
                "table_data": None,
                "meta": {
                    "source_template": kind,
                    "panel": panel_key,
                    "formatter": _text(first.get("y_format")) or None,
                    "filters": {
                        "filter1": _filter_options(panel_rows, "filter1"),
                        "filter2": _filter_options(panel_rows, "filter2"),
                    },
                    "source_rows": [
                        {
                            "x": row.get("x"),
                            "y": row.get("y"),
                            "panel_order": row.get("panel_order"),
                            "legend": row.get("legend"),
                            "legend_order": row.get("legend_order"),
                            "type": row.get("type"),
                            "shape": row.get("shape"),
                            "line_style": row.get("line_style"),
                            "line_width": row.get("line_width"),
                            "point_size": row.get("point_size"),
                            "color": row.get("color"),
                            "y_format": row.get("y_format"),
                            "filter1": row.get("filter1"),
                            "filter2": row.get("filter2"),
                        }
                        for row in panel_rows
                    ],
                },
            }
        )

    chapter_title = cfg.get("chapter_name") or path.stem
    section_title = cfg.get("section_name") or chapter_title
    report_name = cfg.get("title") or chapter_title
    report_type = cfg.get("type") or "analytics"
    report_status = cfg.get("status") or "active"
    subtitle = cfg.get("subtitle") or None

    payload = {
        "id": f"rpt_{report_key.replace('-', '_')}",
        "report_key": report_key,
        "name": report_name,
        "type": report_type,
        "status": report_status,
        "chapters": [
            {
                "chapter_key": "chapter_1",
                "title": chapter_title,
                "subtitle": subtitle,
                "order": 1,
                "status": report_status,
                "sections": [
                    {
                        "chapter_key": "chapter_1",
                        "chapter_name": chapter_title,
                        "section_name": section_title,
                        "section_key": "section_1",
                        "title": section_title,
                        "subtitle": subtitle,
                        "content": subtitle or "",
                        "order": 1,
                        "content_items": {"charts": charts, "kind": None, "items": None},
                    }
                ],
            }
        ],
    }

    chart_points = [
        {
            "chart_id": row.get("panel"),
            "series_name": row.get("legend"),
            "point_time": row.get("x"),
            "metric_value": row.get("y"),
            "filter1": row.get("filter1"),
            "filter2": row.get("filter2"),
        }
        for row in rows
    ]

    return {
        "report_meta": {
            "report_key": report_key,
            "name": report_name,
            "type": report_type,
            "status": report_status,
        },
        "sections": payload["chapters"][0]["sections"],
        "charts": charts,
        "chart_points": chart_points,
        "assembled_payload": payload,
        "template_kind": kind,
    }


def _build_template_table_payload(
    path: Path,
    cfg: dict[str, str],
    rows: list[dict[str, Any]],
    report_key: str,
) -> dict[str, Any]:
    panel_field = "panel" if any("panel" in row for row in rows) else None

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        panel_key = _text(row.get(panel_field), "Table 1") if panel_field else "Table 1"
        grouped[panel_key].append(row)

    panel_keys = sorted(grouped.keys())

    charts: list[dict[str, Any]] = []
    for idx, panel_key in enumerate(panel_keys, start=1):
        panel_rows = grouped[panel_key]
        first = panel_rows[0] if panel_rows else {}

        columns_order: list[str] = []
        for row in panel_rows:
            for key in row.keys():
                if key not in columns_order:
                    columns_order.append(key)

        table_columns = [{"key": key, "title": key} for key in columns_order]
        table_rows = []
        source_rows = []

        for row_idx, row in enumerate(panel_rows, start=1):
            normalized_row = {key: row.get(key) for key in columns_order}
            table_rows.append(normalized_row)
            source_rows.append(
                {
                    **normalized_row,
                    "x": row.get("x"),
                    "y": _number(row.get("y")),
                    "legend": row.get("legend"),
                    "type": row.get("type"),
                    "shape": row.get("shape"),
                    "line_style": row.get("line_style"),
                    "line_width": _number(row.get("line_width")),
                    "point_size": _number(row.get("point_size")),
                    "color": row.get("color"),
                    "y_format": row.get("y_format"),
                    "filter1": _text(row.get("filter1"), "All"),
                    "filter2": _text(row.get("filter2"), "All"),
                    "_row_order": row_idx,
                }
            )

        charts.append(
            {
                "chart_id": f"table_{idx}",
                "chart_type": "table",
                "title": panel_key,
                "subtitle": None,
                "echarts": None,
                "table_data": {
                    "columns": table_columns,
                    "rows": table_rows,
                },
                "meta": {
                    "source_template": "table",
                    "panel": panel_key,
                    "formatter": _text(first.get("y_format")) or None,
                    "filters": {
                        "filter1": _filter_options(source_rows, "filter1"),
                        "filter2": _filter_options(source_rows, "filter2"),
                    },
                    "source_rows": source_rows,
                },
            }
        )

    chapter_title = cfg.get("chapter_name") or path.stem
    section_title = cfg.get("section_name") or chapter_title
    report_name = cfg.get("title") or chapter_title
    report_type = cfg.get("type") or "analytics"
    report_status = cfg.get("status") or "active"
    subtitle = cfg.get("subtitle") or None

    payload = {
        "id": f"rpt_{report_key.replace('-', '_')}",
        "report_key": report_key,
        "name": report_name,
        "type": report_type,
        "status": report_status,
        "chapters": [
            {
                "chapter_key": "chapter_1",
                "title": chapter_title,
                "subtitle": subtitle,
                "order": 1,
                "status": report_status,
                "sections": [
                    {
                        "chapter_key": "chapter_1",
                        "chapter_name": chapter_title,
                        "section_name": section_title,
                        "section_key": "section_1",
                        "title": section_title,
                        "subtitle": subtitle,
                        "content": subtitle or "",
                        "order": 1,
                        "content_items": {"charts": charts, "kind": None, "items": None},
                    }
                ],
            }
        ],
    }

    return {
        "report_meta": {
            "report_key": report_key,
            "name": report_name,
            "type": report_type,
            "status": report_status,
        },
        "sections": payload["chapters"][0]["sections"],
        "charts": charts,
        "chart_points": [],
        "assembled_payload": payload,
        "template_kind": "table",
    }


def _parse_template_v2(
    xls: pd.ExcelFile,
    path: Path,
    override_report_key: str | None,
    sheet_name_map: dict[str, str],
) -> dict[str, Any]:
    cfg_df = pd.read_excel(xls, sheet_name=sheet_name_map["chart_config"])
    data_df = pd.read_excel(xls, sheet_name=sheet_name_map["chart_data"])
    dictionary_df = pd.read_excel(xls, sheet_name=sheet_name_map["column_dictionary"])

    cfg_map = _parse_cfg_map(cfg_df)
    _require_cfg_fields(cfg_map)

    raw_rows = _df_to_records(data_df)
    if not raw_rows:
        raise ValueError("sheet 'chart_data' is empty")

    dictionary_rows = _df_to_records(dictionary_df)
    x_semantic = _detect_x_semantic(dictionary_rows)

    normalized_rows = [_normalize_template_row(row) for row in raw_rows]
    kind = _detect_template_kind(normalized_rows, x_semantic)
    normalized_rows = _normalize_rows_for_kind(normalized_rows, kind)

    report_key = override_report_key or cfg_map.get("report_key") or _slugify(path.stem)
    return _build_template_v2_payload(path, cfg_map, normalized_rows, kind, report_key)


def _parse_table_template_v2(
    xls: pd.ExcelFile,
    path: Path,
    override_report_key: str | None,
    sheet_name_map: dict[str, str],
) -> dict[str, Any]:
    cfg_df = pd.read_excel(xls, sheet_name=sheet_name_map["chart_config"])
    data_df = pd.read_excel(xls, sheet_name=sheet_name_map["table_data"])

    cfg_map = _parse_cfg_map(cfg_df)
    _require_cfg_fields(cfg_map)
    raw_rows = _df_to_records(data_df)
    if not raw_rows:
        raise ValueError("sheet 'table_data' is empty")

    report_key = override_report_key or cfg_map.get("report_key") or _slugify(path.stem)
    return _build_template_table_payload(path, cfg_map, raw_rows, report_key)


def parse_excel(path: Path, override_report_key: str | None = None) -> dict[str, Any]:
    xls = _open_excel(path)
    sheet_name_map = _build_sheet_name_map(xls)

    if TEMPLATE_V2_SHEETS.issubset(set(sheet_name_map.keys())):
        return _parse_template_v2(xls, path, override_report_key, sheet_name_map)

    if TEMPLATE_V2_TABLE_SHEETS.issubset(set(sheet_name_map.keys())):
        return _parse_table_template_v2(xls, path, override_report_key, sheet_name_map)

    available = ", ".join(xls.sheet_names)
    raise ValueError(f"unsupported latest template sheets: {available}")
