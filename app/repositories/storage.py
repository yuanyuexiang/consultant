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

    def next_snapshot_id(self) -> int:
        index = _read_json(settings.meta_file, {"sequence": 10000, "reports": {}})
        index["sequence"] += 1
        _write_json(settings.meta_file, index)
        return index["sequence"]

    def save_snapshot(
        self,
        report_key: str,
        snapshot_id: int,
        payload_hash: str,
        payload_json: dict[str, Any],
        source_file: str,
    ) -> None:
        snap = {
            "snapshot_id": snapshot_id,
            "report_key": report_key,
            "payload_hash": payload_hash,
            "source_file": source_file,
            "generated_at": datetime.now(UTC).isoformat(),
            "payload": payload_json,
        }
        path = settings.snapshot_dir / report_key / f"{snapshot_id}.json"
        _write_json(path, snap)

    def load_snapshot(self, report_key: str, snapshot_id: int) -> dict[str, Any]:
        path = settings.snapshot_dir / report_key / f"{snapshot_id}.json"
        data = _read_json(path, None)
        if data is None:
            raise FileNotFoundError(f"snapshot not found: {report_key}/{snapshot_id}")
        return data

    def update_publish(
        self,
        report_key: str,
        snapshot_id: int,
        payload: dict[str, Any],
    ) -> int:
        index = _read_json(settings.meta_file, {"sequence": 10000, "reports": {}})
        reports = index.setdefault("reports", {})
        info = reports.setdefault(report_key, {"published_version": 0})
        info["published_version"] = int(info.get("published_version", 0)) + 1
        info["snapshot_id"] = snapshot_id
        info["updated_at"] = datetime.now(UTC).isoformat()
        info["name"] = payload.get("name", report_key)
        info["id"] = payload.get("id", report_key)
        info["type"] = payload.get("type", "analytics")
        info["status"] = "published"
        _write_json(settings.meta_file, index)
        return info["published_version"]

    def list_reports(self) -> list[dict[str, Any]]:
        index = _read_json(settings.meta_file, {"sequence": 10000, "reports": {}})
        items: list[dict[str, Any]] = []
        for report_key, info in index.get("reports", {}).items():
            items.append(
                {
                    "report_key": report_key,
                    "id": info.get("id", report_key),
                    "name": info.get("name", report_key),
                    "type": info.get("type", "analytics"),
                    "status": info.get("status", "draft"),
                    "published_version": int(info.get("published_version", 0)),
                    "snapshot_id": info.get("snapshot_id"),
                }
            )
        return sorted(items, key=lambda x: x["report_key"])

    def get_published_snapshot_id(self, report_key: str) -> int:
        index = _read_json(settings.meta_file, {"sequence": 10000, "reports": {}})
        info = index.get("reports", {}).get(report_key)
        if not info or "snapshot_id" not in info:
            raise FileNotFoundError(f"published report not found: {report_key}")
        return int(info["snapshot_id"])
