from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

TEMPLATE_20260403_CHART_SHEETS = {
    "chart_config",
    "chart_data",
    "shaded_regions",
    "reference_lines",
    "column_dictionary",
    "shaded_regions_dict",
    "reference_lines_dict",
}
TEMPLATE_20260403_TABLE_SHEETS = {
    "chart_config",
    "table_data",
    "table_style",
    "header_config",
    "column_dictionary",
}

CHART_DATA_REQUIRED_COLUMNS = {
    "x",
    "y",
    "panel",
    "panel_order",
    "legend",
    "legend_order",
    "type",
    "shape",
    "line_style",
    "line_width",
    "point_size",
    "color",
    "y_format",
    "filter1",
    "filter2",
    "x_label",
    "y_label",
}

SHADED_REGIONS_REQUIRED_COLUMNS = {
    "panel",
    "x_start",
    "x_end",
    "y_start",
    "y_end",
    "color",
    "opacity",
    "label",
    "label_x",
    "label_y",
    "label_color",
    "label_size",
    "label_style",
}

REFERENCE_LINES_REQUIRED_COLUMNS = {
    "panel",
    "orientation",
    "value",
    "color",
    "line_style",
    "line_width",
    "label",
    "label_position",
    "label_color",
    "label_size",
}

HEADER_CONFIG_REQUIRED_COLUMNS = {
    "group_name",
    "start_col",
    "end_col",
    "bg_color",
    "font_color",
}

TABLE_STYLE_TOKEN_WHITELIST = {
    "bg_light_blue",
    "bg_light_yellow",
    "bg_light_green",
    "bg_light_red",
    "font_red",
    "font_green",
    "font_blue",
    "font_orange",
    "font_white",
    "bold",
    "italic",
    "underline",
    "border_top_thick",
}

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


def _int_strict(value: Any, sheet: str, field: str, row_number: int) -> int:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"sheet '{sheet}' row {row_number} column '{field}' must be int")
    if isinstance(value, bool):
        raise ValueError(f"sheet '{sheet}' row {row_number} column '{field}' must be int")

    num = _number(value)
    if num is None or int(num) != num:
        raise ValueError(f"sheet '{sheet}' row {row_number} column '{field}' must be int")
    return int(num)


def _float_strict(value: Any, sheet: str, field: str, row_number: int) -> float:
    num = _number(value)
    if num is None:
        raise ValueError(f"sheet '{sheet}' row {row_number} column '{field}' must be float")
    return float(num)


def _normalized_columns(df: pd.DataFrame) -> set[str]:
    return {_normalize_sheet_name(col) for col in df.columns}


def _require_columns(df: pd.DataFrame, sheet: str, required: set[str]) -> None:
    present = _normalized_columns(df)
    missing = sorted(col for col in required if col not in present)
    if missing:
        raise ValueError(f"sheet '{sheet}' missing columns: {', '.join(missing)}")


def _require_sheets(sheet_name_map: dict[str, str], required: set[str], kind: str) -> None:
    present = set(sheet_name_map.keys())
    missing = sorted(sheet for sheet in required if sheet not in present)
    if missing:
        raise ValueError(
            f"unsupported {kind} template (report20260403): missing sheets: {', '.join(missing)}"
        )


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


def _apply_cfg_backward_compat(path: Path, cfg: dict[str, str]) -> dict[str, str]:
    normalized = dict(cfg)
    # Older templates may not provide chapter_name/section_name.
    # Fall back to title or filename to keep parsing backward compatible.
    chapter_name = _text(normalized.get("chapter_name")) or _text(normalized.get("title")) or path.stem
    section_name = _text(normalized.get("section_name")) or _text(normalized.get("title")) or chapter_name
    normalized["chapter_name"] = chapter_name
    normalized["section_name"] = section_name
    return normalized


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


def _normalize_template_row(raw: dict[str, Any], row_number: int) -> dict[str, Any]:
    return {
        "x": raw.get("x"),
        "y": _float_strict(raw.get("y"), "chart_data", "y", row_number),
        "panel": _text(raw.get("panel"), "Chart 1"),
        "panel_order": _int_strict(raw.get("panel_order"), "chart_data", "panel_order", row_number),
        "legend": _text(raw.get("legend"), "Series"),
        "legend_order": _int_strict(raw.get("legend_order"), "chart_data", "legend_order", row_number),
        "type": _text(raw.get("type"), "line").lower(),
        "shape": _text(raw.get("shape"), "none").lower(),
        "line_style": _text(raw.get("line_style"), "solid").lower(),
        "line_width": _int_strict(raw.get("line_width"), "chart_data", "line_width", row_number),
        "point_size": _int_strict(raw.get("point_size"), "chart_data", "point_size", row_number),
        "color": _text(raw.get("color"), "#5470C6"),
        "y_format": _text(raw.get("y_format")),
        "filter1": _text(raw.get("filter1")),
        "filter2": _text(raw.get("filter2")),
        "x_label": _text(raw.get("x_label")),
        "y_label": _text(raw.get("y_label")),
    }


def _detect_template_kind(rows: list[dict[str, Any]], x_semantic: str) -> str:
    date_like = sum(1 for row in rows if _to_date(row.get("x")) is not None)
    if date_like / max(len(rows), 1) > 0.7:
        return "timeseries"

    numeric_int_like = sum(
        1
        for row in rows
        if (num := _number(row.get("x"))) is not None and int(num) == num and not _is_excel_serial(float(num))
    )
    if numeric_int_like / max(len(rows), 1) > 0.7:
        return "facet"

    if x_semantic == "time":
        return "timeseries"
    if x_semantic == "numeric":
        return "facet"

    serial_like = sum(
        1
        for row in rows
        if isinstance(row.get("x"), (int, float)) and _is_excel_serial(float(row["x"]))
    )
    if serial_like / max(len(rows), 1) > 0.7:
        return "timeseries"

    return "facet"


def _normalize_rows_for_kind(rows: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []

    for idx, row in enumerate(rows, start=2):
        x_value = row.get("x")
        if kind == "timeseries":
            parsed_date = _to_date(x_value)
            if parsed_date is None:
                raise ValueError(f"sheet 'chart_data' row {idx} column 'x' must be date string")
            normalized.append({**row, "x": _format_date_key(parsed_date)})
            continue

        normalized.append({**row, "x": _int_strict(x_value, "chart_data", "x", idx)})

    return normalized


def _normalize_shaded_x(value: Any, kind: str, row_number: int, field: str) -> str | int | None:
    text = _text(value)
    if not text:
        return None

    if kind == "timeseries":
        parsed_date = _to_date(value)
        if parsed_date is None:
            raise ValueError(f"sheet 'shaded_regions' row {row_number} column '{field}' must be date")
        return _format_date_key(parsed_date)

    return _int_strict(text, "shaded_regions", field, row_number)


def _normalize_reference_x(value: Any, kind: str, row_number: int) -> str | int:
    if kind == "timeseries":
        parsed_date = _to_date(value)
        if parsed_date is None:
            raise ValueError("sheet 'reference_lines' row " f"{row_number} column 'value' must be date")
        return _format_date_key(parsed_date)
    return _int_strict(value, "reference_lines", "value", row_number)


def _parse_shaded_regions(rows: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row_number, raw in enumerate(rows, start=2):
        panel = _text(raw.get("panel"))
        if not panel:
            continue

        opacity_value = raw.get("opacity")
        opacity = _number(opacity_value) if _text(opacity_value) else None
        if opacity is not None and not (0.0 <= opacity <= 1.0):
            raise ValueError(
                f"sheet 'shaded_regions' row {row_number} column 'opacity' must be within [0,1]"
            )

        label_size_value = raw.get("label_size")
        label_size = (
            _int_strict(label_size_value, "shaded_regions", "label_size", row_number)
            if _text(label_size_value)
            else None
        )

        y_start_value = raw.get("y_start")
        y_end_value = raw.get("y_end")

        parsed.append(
            {
                "panel": panel,
                "x_start": _normalize_shaded_x(raw.get("x_start"), kind, row_number, "x_start"),
                "x_end": _normalize_shaded_x(raw.get("x_end"), kind, row_number, "x_end"),
                "y_start": _number(y_start_value) if _text(y_start_value) else None,
                "y_end": _number(y_end_value) if _text(y_end_value) else None,
                "color": _text(raw.get("color"), "#E2EFDA"),
                "opacity": opacity,
                "label": _text(raw.get("label")),
                "label_x": _text(raw.get("label_x"), "center").lower(),
                "label_y": _text(raw.get("label_y"), "center").lower(),
                "label_color": _text(raw.get("label_color")),
                "label_size": label_size,
                "label_style": _text(raw.get("label_style"), "normal").lower(),
            }
        )
    return parsed


def _parse_reference_lines(rows: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row_number, raw in enumerate(rows, start=2):
        panel = _text(raw.get("panel"))
        if not panel:
            continue

        orientation = _text(raw.get("orientation")).lower()
        if orientation not in {"horizontal", "vertical"}:
            raise ValueError(
                f"sheet 'reference_lines' row {row_number} column 'orientation' must be horizontal/vertical"
            )

        label_size_value = raw.get("label_size")
        label_size = (
            _int_strict(label_size_value, "reference_lines", "label_size", row_number)
            if _text(label_size_value)
            else None
        )

        value = (
            _float_strict(raw.get("value"), "reference_lines", "value", row_number)
            if orientation == "horizontal"
            else _normalize_reference_x(raw.get("value"), kind, row_number)
        )

        parsed.append(
            {
                "panel": panel,
                "orientation": orientation,
                "value": value,
                "color": _text(raw.get("color"), "#999999"),
                "line_style": _text(raw.get("line_style"), "solid").lower(),
                "line_width": _int_strict(raw.get("line_width"), "reference_lines", "line_width", row_number),
                "label": _text(raw.get("label")),
                "label_position": _text(raw.get("label_position"), "end").lower(),
                "label_color": _text(raw.get("label_color")),
                "label_size": label_size,
            }
        )
    return parsed


def _label_position(label_x: str, label_y: str) -> str:
    mapping = {
        ("left", "top"): "insideTopLeft",
        ("left", "center"): "insideLeft",
        ("left", "bottom"): "insideBottomLeft",
        ("center", "top"): "insideTop",
        ("center", "center"): "inside",
        ("center", "bottom"): "insideBottom",
        ("right", "top"): "insideTopRight",
        ("right", "center"): "insideRight",
        ("right", "bottom"): "insideBottomRight",
    }
    return mapping.get((label_x, label_y), "inside")


def _build_mark_area(shaded_regions: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not shaded_regions:
        return None

    data: list[list[dict[str, Any]]] = []
    for region in shaded_regions:
        start: dict[str, Any] = {}
        end: dict[str, Any] = {}

        if region.get("x_start") is not None:
            start["xAxis"] = region["x_start"]
        if region.get("x_end") is not None:
            end["xAxis"] = region["x_end"]
        if region.get("y_start") is not None:
            start["yAxis"] = region["y_start"]
        if region.get("y_end") is not None:
            end["yAxis"] = region["y_end"]

        # Fill missing X boundaries explicitly for stable left/right range semantics.
        if "xAxis" in start and "xAxis" not in end:
            end["xAxis"] = "max"
        if "xAxis" in end and "xAxis" not in start:
            start["xAxis"] = "min"

        # For Y boundaries:
        # - If one side is provided, expand the other side to keep a valid band.
        # - If both are omitted, keep Y unbounded so the shaded region spans full chart height.
        if "yAxis" in start and "yAxis" not in end:
            end["yAxis"] = "max"
        if "yAxis" in end and "yAxis" not in start:
            start["yAxis"] = "min"

        if "xAxis" not in start and "xAxis" not in end:
            start["xAxis"] = "min"
            end["xAxis"] = "max"

        label = _text(region.get("label"))
        if label:
            start["name"] = label

        item_style: dict[str, Any] = {}
        color = _text(region.get("color"))
        if color:
            item_style["color"] = color
        opacity = region.get("opacity")
        if opacity is not None:
            item_style["opacity"] = opacity
        if item_style:
            start["itemStyle"] = item_style

        if label:
            label_cfg: dict[str, Any] = {
                "show": True,
                "position": _label_position(_text(region.get("label_x")), _text(region.get("label_y"))),
                "formatter": label,
                "backgroundColor": "rgba(247, 251, 255, 0.92)",
                "borderColor": _text(region.get("label_color")) or _text(region.get("color")) or "#5B9BD5",
                "borderWidth": 1,
                "borderRadius": 8,
                "padding": [4, 10],
            }
            label_color = _text(region.get("label_color"))
            if label_color:
                label_cfg["color"] = label_color
            label_size = region.get("label_size")
            if label_size is not None:
                label_cfg["fontSize"] = label_size
            label_style = _text(region.get("label_style"), "normal")
            if label_style == "bold":
                label_cfg["fontWeight"] = "bold"
            elif label_style == "italic":
                label_cfg["fontStyle"] = "italic"
            start["label"] = label_cfg

        data.append([start, end])

    return {
        "silent": True,
        "data": data,
    }


def _build_mark_line(reference_lines: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not reference_lines:
        return None

    data: list[dict[str, Any]] = []
    for line in reference_lines:
        item: dict[str, Any] = {}
        if line.get("orientation") == "horizontal":
            item["yAxis"] = line.get("value")
        else:
            item["xAxis"] = line.get("value")

        label = _text(line.get("label"))
        if label:
            item["name"] = label

        line_style: dict[str, Any] = {
            "type": _line_type(_text(line.get("line_style"), "solid")),
            "width": max(1, line.get("line_width") or 1),
        }
        color = _text(line.get("color"))
        if color:
            line_style["color"] = color
        item["lineStyle"] = line_style

        if label:
            label_cfg: dict[str, Any] = {
                "show": True,
                "formatter": label,
                "position": _text(line.get("label_position"), "end"),
            }
            label_color = _text(line.get("label_color"))
            if label_color:
                label_cfg["color"] = label_color
            label_size = line.get("label_size")
            if label_size is not None:
                label_cfg["fontSize"] = label_size
            item["label"] = label_cfg

        data.append(item)

    return {
        "symbol": ["none", "none"],
        "data": data,
    }


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
        x_num = _number(row.get("x"))
        if x_num is not None:
            mapped[str(float(x_num))] = y
    return mapped


def _build_option_for_panel(
    rows: list[dict[str, Any]],
    kind: str,
    x_label: str | None = None,
    y_label: str | None = None,
    shaded_regions: list[dict[str, Any]] | None = None,
    reference_lines: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    shaded_regions = shaded_regions or []
    reference_lines = reference_lines or []
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
            # Keep one value per date for each legend to avoid duplicated points
            # when upstream rows contain repeated x for the same legend.
            date_point_map: dict[str, float] = {}
            for item in points:
                y_value = item.get("y")
                if y_value is None:
                    continue
                x_value = _text(item.get("x"))
                if not x_value:
                    continue
                date_point_map[x_value] = y_value
            data = [[x_key, y_value] for x_key, y_value in sorted(date_point_map.items())]
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

    option: dict[str, Any] = {
        "tooltip": {"trigger": "axis"},
        "legend": {"show": True, "bottom": 0},
        "grid": {"left": 50, "right": 20, "top": 40, "bottom": 45},
        "xAxis": (
            {
                "type": "time",
                "axisLabel": {"hideOverlap": True},
                "name": x_label,
                "nameLocation": "middle",
                "nameGap": 30,
            }
            if kind == "timeseries"
            else {"type": "category", "data": x_values, "name": x_label, "nameLocation": "middle", "nameGap": 30}
        ),
        "yAxis": {
            "type": "value",
            "axisLabel": {"formatter": _axis_label_template(y_format, kind)},
            "name": y_label,
            "nameGap": 40,
        },
        "series": series,
        "animation": False,
    }

    mark_area = _build_mark_area(shaded_regions)
    mark_line = _build_mark_line(reference_lines)

    if series and (mark_area or mark_line):
        # ECharts handles markArea/markLine most reliably when attached to a concrete series.
        if mark_area:
            series[0]["markArea"] = mark_area
        if mark_line:
            series[0]["markLine"] = mark_line

    return option


def _build_template_v2_payload(
    path: Path,
    cfg: dict[str, str],
    rows: list[dict[str, Any]],
    shaded_regions: list[dict[str, Any]],
    reference_lines: list[dict[str, Any]],
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
        panel_shaded_regions = [item for item in shaded_regions if _text(item.get("panel")) == panel_key]
        panel_reference_lines = [item for item in reference_lines if _text(item.get("panel")) == panel_key]

        echarts_option = _build_option_for_panel(
            panel_rows,
            kind,
            _text(first.get("x_label")) or None,
            _text(first.get("y_label")) or None,
            panel_shaded_regions,
            panel_reference_lines,
        )

        charts.append(
            {
                "chart_id": chart_id,
                "chart_type": "line",
                "title": panel_key,
                "subtitle": None,
                "echarts": echarts_option,
                "table_data": None,
                "meta": {
                    "source_template": kind,
                    "panel": panel_key,
                    "formatter": _text(first.get("y_format")) or None,
                    "x_label": _text(first.get("x_label")) or None,
                    "y_label": _text(first.get("y_label")) or None,
                    "filter_labels": {
                        "filter1": _text(cfg.get("filter1_label")),
                        "filter2": _text(cfg.get("filter2_label")),
                    },
                    "filters": {
                        "filter1": _filter_options(panel_rows, "filter1"),
                        "filter2": _filter_options(panel_rows, "filter2"),
                    },
                    "shaded_regions": panel_shaded_regions,
                    "reference_lines": panel_reference_lines,
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
                            "x_label": row.get("x_label"),
                            "y_label": row.get("y_label"),
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
    table_presentation: dict[str, Any],
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
                    "presentation": table_presentation,
                },
                "meta": {
                    "source_template": "table",
                    "panel": panel_key,
                    "formatter": _text(first.get("y_format")) or None,
                    "filter_labels": {
                        "filter1": _text(cfg.get("filter1_label")),
                        "filter2": _text(cfg.get("filter2_label")),
                    },
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
    _require_sheets(sheet_name_map, TEMPLATE_20260403_CHART_SHEETS, "chart")
    cfg_df = pd.read_excel(xls, sheet_name=sheet_name_map["chart_config"])
    data_df = pd.read_excel(xls, sheet_name=sheet_name_map["chart_data"])
    shaded_regions_df = pd.read_excel(xls, sheet_name=sheet_name_map["shaded_regions"])
    reference_lines_df = pd.read_excel(xls, sheet_name=sheet_name_map["reference_lines"])
    dictionary_df = pd.read_excel(xls, sheet_name=sheet_name_map["column_dictionary"])

    _require_columns(data_df, "chart_data", CHART_DATA_REQUIRED_COLUMNS)
    _require_columns(shaded_regions_df, "shaded_regions", SHADED_REGIONS_REQUIRED_COLUMNS)
    _require_columns(reference_lines_df, "reference_lines", REFERENCE_LINES_REQUIRED_COLUMNS)

    cfg_map = _parse_cfg_map(cfg_df)
    cfg_map = _apply_cfg_backward_compat(path, cfg_map)
    _require_cfg_fields(cfg_map)

    raw_rows = _df_to_records(data_df)
    if not raw_rows:
        raise ValueError("sheet 'chart_data' is empty")

    dictionary_rows = _df_to_records(dictionary_df)
    x_semantic = _detect_x_semantic(dictionary_rows)

    normalized_rows = [_normalize_template_row(row, idx) for idx, row in enumerate(raw_rows, start=2)]
    kind = _detect_template_kind(normalized_rows, x_semantic)
    normalized_rows = _normalize_rows_for_kind(normalized_rows, kind)

    shaded_regions = _parse_shaded_regions(_df_to_records(shaded_regions_df), kind)
    reference_lines = _parse_reference_lines(_df_to_records(reference_lines_df), kind)

    report_key = override_report_key or cfg_map.get("report_key") or _slugify(path.stem)
    return _build_template_v2_payload(
        path,
        cfg_map,
        normalized_rows,
        shaded_regions,
        reference_lines,
        kind,
        report_key,
    )


def _normalize_table_row(raw: dict[str, Any], columns_order: list[str]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key in columns_order:
        value = raw.get(key)
        normalized[key] = "" if value is None else str(value)
    return normalized


def _parse_table_style(
    style_rows: list[dict[str, Any]],
    data_columns: list[str],
) -> dict[str, Any]:
    cell_styles: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for row_index, row in enumerate(style_rows):
        for column in data_columns:
            raw_tokens = _text(row.get(column))
            if not raw_tokens:
                continue

            tokens = [token.strip().lower() for token in raw_tokens.split(",") if token.strip()]
            valid_tokens = [token for token in tokens if token in TABLE_STYLE_TOKEN_WHITELIST]
            invalid_tokens = [token for token in tokens if token not in TABLE_STYLE_TOKEN_WHITELIST]

            if valid_tokens:
                cell_styles.append(
                    {
                        "row_index": row_index,
                        "column": column,
                        "tokens": valid_tokens,
                    }
                )

            if invalid_tokens:
                warnings.append(
                    {
                        "row_index": row_index,
                        "column": column,
                        "unknown_tokens": invalid_tokens,
                    }
                )

    return {
        "cell_styles": cell_styles,
        "warnings": warnings,
    }


def _parse_header_config(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=2):
        group_name = _text(row.get("group_name"))
        start_col = _text(row.get("start_col"))
        end_col = _text(row.get("end_col"))
        if not group_name or not start_col or not end_col:
            raise ValueError(
                f"sheet 'header_config' row {idx} must provide group_name, start_col, end_col"
            )
        groups.append(
            {
                "group_name": group_name,
                "start_col": start_col,
                "end_col": end_col,
                "bg_color": _text(row.get("bg_color")),
                "font_color": _text(row.get("font_color")),
            }
        )
    return groups


def _parse_table_template_v2(
    xls: pd.ExcelFile,
    path: Path,
    override_report_key: str | None,
    sheet_name_map: dict[str, str],
) -> dict[str, Any]:
    _require_sheets(sheet_name_map, TEMPLATE_20260403_TABLE_SHEETS, "table")
    cfg_df = pd.read_excel(xls, sheet_name=sheet_name_map["chart_config"])
    data_df = pd.read_excel(xls, sheet_name=sheet_name_map["table_data"])
    style_df = pd.read_excel(xls, sheet_name=sheet_name_map["table_style"])
    header_df = pd.read_excel(xls, sheet_name=sheet_name_map["header_config"])

    _require_columns(header_df, "header_config", HEADER_CONFIG_REQUIRED_COLUMNS)

    cfg_map = _parse_cfg_map(cfg_df)
    cfg_map = _apply_cfg_backward_compat(path, cfg_map)
    _require_cfg_fields(cfg_map)

    raw_rows = _df_to_records(data_df)
    if not raw_rows:
        raise ValueError("sheet 'table_data' is empty")

    data_columns = [str(col).strip() for col in data_df.columns]
    missing_style_columns = sorted(col for col in data_columns if _normalize_sheet_name(col) not in _normalized_columns(style_df))
    if missing_style_columns:
        raise ValueError(f"sheet 'table_style' missing columns: {', '.join(missing_style_columns)}")

    normalized_rows = [_normalize_table_row(row, data_columns) for row in raw_rows]
    table_style = _parse_table_style(_df_to_records(style_df), data_columns)
    header_groups = _parse_header_config(_df_to_records(header_df))

    table_presentation = {
        "cell_styles": table_style["cell_styles"],
        "header_groups": header_groups,
        "style_warnings": table_style["warnings"],
    }

    report_key = override_report_key or cfg_map.get("report_key") or _slugify(path.stem)
    return _build_template_table_payload(path, cfg_map, normalized_rows, table_presentation, report_key)


def parse_excel(path: Path, override_report_key: str | None = None) -> dict[str, Any]:
    xls = _open_excel(path)
    sheet_name_map = _build_sheet_name_map(xls)

    sheet_keys = set(sheet_name_map.keys())

    if "chart_data" in sheet_keys:
        return _parse_template_v2(xls, path, override_report_key, sheet_name_map)

    if "table_data" in sheet_keys:
        return _parse_table_template_v2(xls, path, override_report_key, sheet_name_map)

    available = ", ".join(xls.sheet_names)
    raise ValueError(
        "unsupported template: only report20260403 format is accepted; "
        f"available sheets: {available}"
    )
