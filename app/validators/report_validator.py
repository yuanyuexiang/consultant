from __future__ import annotations

from typing import Any


def _raise(msg: str) -> None:
    raise ValueError(msg)


def _extract_sections(payload: dict[str, Any]) -> list[dict[str, Any]]:
    sections = payload.get("sections")
    if sections:
        return sections

    collected: list[dict[str, Any]] = []
    for chapter in payload.get("chapters", []):
        collected.extend(chapter.get("sections", []))
    return collected


def validate_report_payload(payload: dict[str, Any]) -> None:
    sections = _extract_sections(payload)

    # Rule 1: section order unique inside one report.
    orders = [sec.get("order") for sec in sections]
    clean_orders = [o for o in orders if o is not None]
    if len(clean_orders) != len(set(clean_orders)):
        _raise("section order_no must be unique within one report")

    for sec in sections:
        charts = sec.get("content_items", {}).get("charts", [])
        for chart in charts:
            series = (
                chart.get("echarts", {}).get("series", []) if chart.get("echarts") else []
            )
            xaxis = (
                chart.get("echarts", {}).get("xAxis", {}).get("data", [])
                if chart.get("echarts")
                else []
            )

            # Rule 2: each chart has at least one series.
            if chart.get("chart_type") == "line" and not series:
                _raise(f"chart '{chart.get('chart_id')}' has no series")

            # Rule 3: each series point count equals xAxis length.
            for item in series:
                data = item.get("data", [])
                if len(data) != len(xaxis):
                    _raise(
                        "chart "
                        f"'{chart.get('chart_id')}' "
                        "series "
                        f"'{item.get('name')}' "
                        "length mismatch with xAxis"
                    )

            # Rule 4: missing values must remain null, no forced zero handling here.
            # This is naturally satisfied by preserving None in assembly.
