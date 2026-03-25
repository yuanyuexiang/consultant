from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def assemble_report(parsed: dict[str, Any], normalized: dict[str, Any]) -> dict[str, Any]:
    report_meta = parsed["report_meta"]
    sections = parsed["sections"]
    chapters = parsed.get("chapters", [])
    charts = parsed["charts"]
    points_by_chart: dict[str, list[dict[str, Any]]] = normalized["points_by_chart"]

    chapter_map: dict[str, dict[str, Any]] = {}
    if chapters:
        for chap in chapters:
            key = str(chap.get("chapter_key") or "chapter_1")
            chapter_map[key] = {
                "chapter_key": key,
                "title": chap.get("title", key),
                "subtitle": chap.get("subtitle"),
                "order": int(chap.get("order_no", 1)),
                "status": chap.get("status", "draft"),
                "sections": [],
            }
    else:
        chapter_map["chapter_1"] = {
            "chapter_key": "chapter_1",
            "title": "Default Chapter",
            "subtitle": None,
            "order": 1,
            "status": "draft",
            "sections": [],
        }

    section_map: dict[str, dict[str, Any]] = {}
    for sec in sections:
        chapter_key = str(sec.get("chapter_key") or "chapter_1")
        if chapter_key not in chapter_map:
            chapter_map[chapter_key] = {
                "chapter_key": chapter_key,
                "title": chapter_key,
                "subtitle": None,
                "order": 999,
                "status": "draft",
                "sections": [],
            }
        section_map[str(sec["section_key"])] = {
            "id": sec.get("id"),
            "chapter_key": chapter_key,
            "section_key": str(sec["section_key"]),
            "title": sec.get("title", ""),
            "subtitle": sec.get("subtitle"),
            "status": sec.get("status", "draft"),
            "order": int(sec.get("order_no", 0)),
            "layout": sec.get("layout"),
            "content": sec.get("content_template"),
            "content_items": {"charts": [], "kind": None, "items": None},
        }

    for chart in charts:
        chart_id = str(chart["chart_id"])
        section_key = str(chart["section_key"])
        rows = points_by_chart.get(chart_id, [])

        points_by_series: dict[str, dict[str, float | None]] = defaultdict(dict)
        xaxis_set: set[str] = set()
        for row in rows:
            x = row["point_time"]
            xaxis_set.add(x)
            points_by_series[row["series_name"]][x] = _safe_float(row["metric_value"])

        xaxis = sorted([x for x in xaxis_set if x])
        series: list[dict[str, Any]] = []
        for series_name, series_points in points_by_series.items():
            series.append(
                {
                    "name": series_name,
                    "type": "line",
                    "data": [series_points.get(x, None) for x in xaxis],
                }
            )

        option_template = chart.get("option_template_json") or {}
        echarts = {
            "xAxis": option_template.get("xAxis", {"type": "category", "data": xaxis}),
            "yAxis": option_template.get("yAxis", {"type": "value"}),
            "series": option_template.get("series", series),
        }
        if "data" not in echarts["xAxis"]:
            echarts["xAxis"]["data"] = xaxis
        if echarts["series"] == option_template.get("series"):
            # If template provides series, inject normalized data by matching name.
            remap = {s["name"]: s for s in series}
            for item in echarts["series"]:
                name = item.get("name")
                if name in remap:
                    item["data"] = remap[name]["data"]

        out_chart = {
            "chart_id": chart_id,
            "chart_type": chart.get("chart_type", "line"),
            "title": chart.get("title", chart_id),
            "subtitle": chart.get("subtitle"),
            "echarts": echarts if chart.get("chart_type", "line") == "line" else None,
            "table_data": None,
            "meta": {
                "formatter": chart.get("formatter"),
                "metric_name": chart.get("metric_name", chart_id),
            },
        }
        if section_key in section_map:
            section_map[section_key]["content_items"]["charts"].append(out_chart)

    sorted_sections = sorted(section_map.values(), key=lambda x: x["order"])
    for sec in sorted_sections:
        chapter_key = sec.get("chapter_key", "chapter_1")
        chapter_map[chapter_key]["sections"].append(sec)

    sorted_chapters = sorted(chapter_map.values(), key=lambda x: x["order"])
    for chapter in sorted_chapters:
        chapter["sections"] = sorted(chapter["sections"], key=lambda x: x["order"])

    return {
        "id": report_meta.get("id") or f"rpt_{str(report_meta['report_key']).replace('-', '_')}",
        "report_key": report_meta["report_key"],
        "name": report_meta.get("name", report_meta["report_key"]),
        "type": report_meta.get("type", "analytics"),
        "status": report_meta.get("status", "draft"),
        "published_version": int(report_meta.get("published_version", 0)),
        "generated_at": datetime.now(UTC).isoformat(),
        "chapters": sorted_chapters,
    }
