import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.config import settings


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


class StorageRepository:
    def _load_index(self) -> dict[str, Any]:
        return _read_json(settings.meta_file, {"sequence": 10000, "reports": {}})

    def _save_index(self, index: dict[str, Any]) -> None:
        _write_json(settings.meta_file, index)

    def save_parsed(
        self,
        report_key: str,
        source_file: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = datetime.now(UTC).isoformat()
        record = {
            "report_key": report_key,
            "source_file": source_file,
            "parsed_at": now,
            "payload": payload,
        }
        path = settings.parse_dir / f"{report_key}.json"
        _write_json(path, record)
        return record

    def load_parsed(self, report_key: str) -> dict[str, Any]:
        path = settings.parse_dir / f"{report_key}.json"
        data = _read_json(path, None)
        if data is None:
            raise FileNotFoundError(f"parsed data not found for {report_key}")
        return data

    def save_report(self, report_key: str, payload: dict[str, Any], payload_hash: str) -> str:
        saved_at = datetime.now(UTC).isoformat()
        report_doc = {
            "report_key": report_key,
            "saved_at": saved_at,
            "payload_hash": payload_hash,
            "payload": payload,
        }
        path = settings.reports_dir / f"{report_key}.json"
        _write_json(path, report_doc)
        return saved_at

    def load_report(self, report_key: str) -> dict[str, Any]:
        path = settings.reports_dir / f"{report_key}.json"
        data = _read_json(path, None)
        if data is None:
            raise FileNotFoundError(f"report not found: {report_key}")
        return data

    def upsert_report_index(
        self,
        report_key: str,
        payload: dict[str, Any],
        status: str,
        payload_hash: str,
        saved_at: str,
    ) -> None:
        index = self._load_index()
        reports = index.setdefault("reports", {})
        reports[report_key] = {
            "updated_at": saved_at,
            "name": payload.get("name", report_key),
            "id": payload.get("id", report_key),
            "type": payload.get("type", "analytics"),
            "status": status,
            "payload_hash": payload_hash,
        }
        self._save_index(index)

    def list_reports(self) -> list[dict[str, Any]]:
        index = self._load_index()
        items: list[dict[str, Any]] = []
        for report_key, info in index.get("reports", {}).items():
            items.append(
                {
                    "report_key": report_key,
                    "id": info.get("id", report_key),
                    "name": info.get("name", report_key),
                    "type": info.get("type", "analytics"),
                    "status": info.get("status", "active"),
                    "updated_at": info.get("updated_at"),
                }
            )
        return sorted(items, key=lambda x: x["report_key"])

    def exists_report(self, report_key: str) -> bool:
        index = self._load_index()
        return report_key in index.get("reports", {})

    def get_report_info(self, report_key: str) -> dict[str, Any]:
        index = self._load_index()
        info = index.get("reports", {}).get(report_key)
        if not info:
            raise FileNotFoundError(f"report not found: {report_key}")
        return info

    def delete_report(self, report_key: str) -> None:
        index = self._load_index()
        reports = index.setdefault("reports", {})
        if report_key not in reports:
            raise FileNotFoundError(f"report not found: {report_key}")
        del reports[report_key]
        self._save_index(index)

        report_file = settings.reports_dir / f"{report_key}.json"
        if report_file.exists():
            report_file.unlink()

        parsed_file = settings.parse_dir / f"{report_key}.json"
        if parsed_file.exists():
            parsed_file.unlink()
