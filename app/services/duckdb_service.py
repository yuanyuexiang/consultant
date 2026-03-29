from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from app.config import settings


class DuckDBService:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or settings.duckdb_file
        self._ensure_schema()

    def _connect(self):
        return duckdb.connect(str(self.db_path))

    def _ensure_schema(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS report_chart_rows (
                  report_key TEXT NOT NULL,
                  section_key TEXT NOT NULL,
                  chart_id TEXT NOT NULL,
                  x_value TEXT,
                  y_value DOUBLE,
                  legend TEXT,
                  kind TEXT,
                  shape TEXT,
                  line_style TEXT,
                  line_width DOUBLE,
                  point_size DOUBLE,
                  color TEXT,
                  y_format TEXT,
                  filter1 TEXT,
                filter2 TEXT,
                raw_row_json TEXT,
                row_order BIGINT
                )
                """
            )
            con.execute("ALTER TABLE report_chart_rows ADD COLUMN IF NOT EXISTS raw_row_json TEXT")
            con.execute("ALTER TABLE report_chart_rows ADD COLUMN IF NOT EXISTS row_order BIGINT")
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_report_chart_rows
                ON report_chart_rows(report_key, section_key, chart_id, filter1, filter2)
                """
            )

    def replace_report_rows(self, report_key: str, payload: dict[str, Any]) -> None:
        rows = self._extract_rows(report_key, payload)
        with self._connect() as con:
            con.execute("DELETE FROM report_chart_rows WHERE report_key = ?", [report_key])
            if not rows:
                return
            con.executemany(
                """
                INSERT INTO report_chart_rows (
                  report_key, section_key, chart_id,
                  x_value, y_value, legend, kind, shape,
                  line_style, line_width, point_size,
                                    color, y_format, filter1, filter2,
                                    raw_row_json, row_order
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def query_chart_rows(
        self,
        report_key: str,
        section_key: str,
        chart_id: str,
        filter1: str,
        filter2: str,
    ) -> list[dict[str, Any]]:
        sql = [
            """
            SELECT
              x_value,
              y_value,
              legend,
              kind,
              shape,
              line_style,
              line_width,
              point_size,
              color,
              y_format,
              filter1,
                            filter2,
                            raw_row_json,
                            row_order
            FROM report_chart_rows
            WHERE report_key = ? AND section_key = ? AND chart_id = ?
            """
        ]
        params: list[Any] = [report_key, section_key, chart_id]

        if filter1 != "All":
            sql.append("AND COALESCE(filter1, 'All') = ?")
            params.append(filter1)
        if filter2 != "All":
            sql.append("AND COALESCE(filter2, 'All') = ?")
            params.append(filter2)

        sql.append("ORDER BY COALESCE(row_order, 0), x_value, legend")

        with self._connect() as con:
            cur = con.execute("\n".join(sql), params)
            fetched = cur.fetchall()

        rows: list[dict[str, Any]] = []
        for item in fetched:
            rows.append(
                {
                    "x": item[0],
                    "y": item[1],
                    "legend": item[2],
                    "type": item[3],
                    "shape": item[4],
                    "line_style": item[5],
                    "line_width": item[6],
                    "point_size": item[7],
                    "color": item[8],
                    "y_format": item[9],
                    "filter1": item[10],
                    "filter2": item[11],
                    "_raw_row": self._json_loads(item[12]),
                    "_row_order": item[13],
                }
            )
        return rows

    def _extract_rows(self, report_key: str, payload: dict[str, Any]) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        chapters = payload.get("chapters", [])
        if not isinstance(chapters, list):
            return rows

        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            for section in chapter.get("sections", []):
                if not isinstance(section, dict):
                    continue
                section_key = str(section.get("section_key") or "")
                charts = (section.get("content_items") or {}).get("charts", [])
                if not isinstance(charts, list):
                    continue

                for chart in charts:
                    if not isinstance(chart, dict):
                        continue
                    chart_id = str(chart.get("chart_id") or "")
                    meta = chart.get("meta")
                    if not isinstance(meta, dict):
                        continue
                    source_rows = meta.get("source_rows")
                    if not isinstance(source_rows, list):
                        continue

                    for item in source_rows:
                        if not isinstance(item, dict):
                            continue
                        rows.append(
                            (
                                report_key,
                                section_key,
                                chart_id,
                                self._text(item.get("x")),
                                self._number(item.get("y")),
                                self._text(item.get("legend")),
                                self._text(item.get("type")),
                                self._text(item.get("shape")),
                                self._text(item.get("line_style")),
                                self._number(item.get("line_width")),
                                self._number(item.get("point_size")),
                                self._text(item.get("color")),
                                self._text(item.get("y_format")),
                                self._coalesce_filter(item.get("filter1")),
                                self._coalesce_filter(item.get("filter2")),
                                json.dumps(item, ensure_ascii=True),
                                self._integer(item.get("_row_order")),
                            )
                        )
        return rows

    @staticmethod
    def _text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coalesce_filter(value: Any) -> str:
        text = DuckDBService._text(value)
        return text or "All"

    @staticmethod
    def _number(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
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

    @staticmethod
    def _integer(value: Any) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    @staticmethod
    def _json_loads(value: Any) -> dict[str, Any] | None:
        if not value:
            return None
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except (TypeError, ValueError):
            return None


duckdb_service = DuckDBService()
