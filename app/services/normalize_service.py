from __future__ import annotations

from collections import defaultdict
from typing import Any


def _to_month(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if len(text) >= 7:
        return text[:7]
    return text


def normalize_points(parsed: dict[str, Any]) -> dict[str, Any]:
    report_key = parsed["report_meta"]["report_key"]
    chart_points = parsed["chart_points"]

    normalized: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in chart_points:
        chart_id = str(row["chart_id"])
        normalized[chart_id].append(
            {
                "report_key": report_key,
                "chart_id": chart_id,
                "series_name": str(row["series_name"]),
                "point_time": _to_month(row["point_time"]),
                "metric_value": row.get("metric_value", None),
            }
        )

    # Keep xAxis monotonic by lexical sort on YYYY-MM.
    for chart_id in normalized:
        normalized[chart_id].sort(key=lambda x: x["point_time"])

    return {"report_key": report_key, "points_by_chart": dict(normalized)}
