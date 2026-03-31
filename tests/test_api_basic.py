from pathlib import Path
from time import monotonic, sleep

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

client = TestClient(app)


def _wait_upload_folder_task(task_id: str, timeout_s: float = 30.0) -> dict:
    deadline = monotonic() + timeout_s
    while monotonic() < deadline:
        resp = client.get(f"/consultant/api/v1/reports/upload-folder/tasks/{task_id}")
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        data = body["data"]
        status = data["status"]
        if status in {"succeeded", "failed"}:
            return data
        sleep(0.05)
    raise AssertionError(f"upload task timeout: {task_id}")


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"]["status"] == "ok"


def test_list_reports_success():
    resp = client.get("/consultant/api/v1/reports")
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert isinstance(body["data"]["items"], list)


def test_report_crud(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    settings.runtime_dir = runtime_dir
    settings.upload_dir = runtime_dir / "uploads"
    settings.parse_dir = runtime_dir / "parsed"
    settings.reports_dir = runtime_dir / "reports"
    settings.meta_file = runtime_dir / "reports_index.json"
    settings.ensure_dirs()

    create_payload = {
        "report_key": "crud-demo",
        "name": "CRUD Demo",
        "type": "analytics",
        "status": "active",
        "sections": [],
    }
    create_resp = client.post("/consultant/api/v1/reports", json=create_payload)
    assert create_resp.status_code == 200
    create_body = create_resp.json()
    assert create_body["code"] == 0
    assert create_body["data"]["report_key"] == "crud-demo"
    assert create_body["data"]["saved_at"]

    update_resp = client.patch(
        "/consultant/api/v1/reports/crud-demo",
        json={"name": "CRUD Demo Updated", "status": "active"},
    )
    assert update_resp.status_code == 200
    update_body = update_resp.json()
    assert update_body["code"] == 0

    detail_resp = client.get("/consultant/api/v1/reports/crud-demo")
    assert detail_resp.status_code == 200
    detail_body = detail_resp.json()
    assert detail_body["data"]["payload"]["name"] == "CRUD Demo Updated"

    delete_resp = client.delete("/consultant/api/v1/reports/crud-demo")
    assert delete_resp.status_code == 200
    delete_body = delete_resp.json()
    assert delete_body["data"]["deleted"] is True

    missing_resp = client.get("/consultant/api/v1/reports/crud-demo")
    assert missing_resp.status_code == 404


def test_upload_template_v2_auto_build_report(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    settings.runtime_dir = runtime_dir
    settings.upload_dir = runtime_dir / "uploads"
    settings.parse_dir = runtime_dir / "parsed"
    settings.reports_dir = runtime_dir / "reports"
    settings.meta_file = runtime_dir / "reports_index.json"
    settings.ensure_dirs()

    template_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "report20260330"
        / "chapter1_section3.xlsx"
    )
    assert template_path.exists(), f"template not found: {template_path}"

    upload_resp = client.post(
        "/consultant/api/v1/reports/upload-excel",
        files={
            "file": (
                template_path.name,
                template_path.read_bytes(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
        data={"report_key": "template-v2-demo"},
    )

    assert upload_resp.status_code == 200, upload_resp.json()
    upload_body = upload_resp.json()
    assert upload_body["code"] == 0
    assert upload_body["data"]["report_key"] == "template-v2-demo"
    assert upload_body["data"]["parsed_charts"] > 0

    detail_resp = client.get("/consultant/api/v1/reports/template-v2-demo")
    assert detail_resp.status_code == 200
    detail_body = detail_resp.json()
    payload = detail_body["data"]["payload"]

    assert payload["report_key"] == "template-v2-demo"
    assert payload["chapters"]
    section = payload["chapters"][0]["sections"][0]
    chart = section["content_items"]["charts"][0]

    assert payload["chapters"][0]["title"] == "Portfolio Performance Overview"
    assert section["title"] == "Vintage Origination Trends"
    assert section["section_name"] == "Vintage Origination Trends"

    assert "filters" in chart["meta"]
    assert "filter1" in chart["meta"]["filters"]
    assert "filter2" in chart["meta"]["filters"]
    assert "source_rows" in chart["meta"]
    assert chart["echarts"]["xAxis"]["type"] == "time"


def test_section_filter_query_uses_duckdb_rows(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    settings.runtime_dir = runtime_dir
    settings.upload_dir = runtime_dir / "uploads"
    settings.parse_dir = runtime_dir / "parsed"
    settings.reports_dir = runtime_dir / "reports"
    settings.meta_file = runtime_dir / "reports_index.json"
    settings.duckdb_file = runtime_dir / "analytics.duckdb"
    settings.ensure_dirs()

    template_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "report20260330"
        / "chapter1_section3.xlsx"
    )

    upload_resp = client.post(
        "/consultant/api/v1/reports/upload-excel",
        files={
            "file": (
                template_path.name,
                template_path.read_bytes(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
        data={"report_key": "duckdb-filter-demo"},
    )
    assert upload_resp.status_code == 200, upload_resp.json()

    all_resp = client.get(
        "/consultant/api/v1/reports/duckdb-filter-demo/sections/section_1",
        params={"filter1": "All", "filter2": "All"},
    )
    default_resp = client.get(
        "/consultant/api/v1/reports/duckdb-filter-demo/sections/section_1",
    )
    grade_resp = client.get(
        "/consultant/api/v1/reports/duckdb-filter-demo/sections/section_1",
        params={"filter1": "Grade 1", "filter2": "36 Month"},
    )

    assert all_resp.status_code == 200, all_resp.json()
    assert default_resp.status_code == 200, default_resp.json()
    assert grade_resp.status_code == 200, grade_resp.json()

    all_chart = all_resp.json()["data"]["section"]["content_items"]["charts"][0]
    default_chart = default_resp.json()["data"]["section"]["content_items"]["charts"][0]
    grade_chart = grade_resp.json()["data"]["section"]["content_items"]["charts"][0]
    default_section = default_resp.json()["data"]["section"]

    assert all_chart["meta"]["filtered_rows_count"] > 0
    assert default_chart["meta"]["filtered_rows_count"] == all_chart["meta"]["filtered_rows_count"]
    assert default_chart["meta"]["selected_filters"] == {"filter1": "ALL", "filter2": "ALL"}
    assert default_section["meta"]["selected_filters"] == {"filter1": "ALL", "filter2": "ALL"}
    assert grade_chart["meta"]["filtered_rows_count"] > 0
    assert grade_chart["meta"]["selected_filters"] == {"filter1": "Grade 1", "filter2": "36 Month"}


def test_upload_table_template_auto_build_report(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    settings.runtime_dir = runtime_dir
    settings.upload_dir = runtime_dir / "uploads"
    settings.parse_dir = runtime_dir / "parsed"
    settings.reports_dir = runtime_dir / "reports"
    settings.meta_file = runtime_dir / "reports_index.json"
    settings.duckdb_file = runtime_dir / "analytics.duckdb"
    settings.ensure_dirs()

    template_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "report20260330"
        / "chapter1_section1.xlsx"
    )
    assert template_path.exists(), f"template not found: {template_path}"

    upload_resp = client.post(
        "/consultant/api/v1/reports/upload-excel",
        files={
            "file": (
                template_path.name,
                template_path.read_bytes(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
        data={"report_key": "table-template-demo"},
    )

    assert upload_resp.status_code == 200, upload_resp.json()

    detail_resp = client.get("/consultant/api/v1/reports/table-template-demo")
    assert detail_resp.status_code == 200
    payload = detail_resp.json()["data"]["payload"]

    chart = payload["chapters"][0]["sections"][0]["content_items"]["charts"][0]
    section = payload["chapters"][0]["sections"][0]
    assert section["section_name"] == section["title"]
    assert chart["chart_type"] == "table"
    assert chart["table_data"]["rows"]
    assert chart["meta"]["filters"]["filter1"]
    assert chart["meta"]["source_rows"]


def test_upload_folder_mixed_templates(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    settings.runtime_dir = runtime_dir
    settings.upload_dir = runtime_dir / "uploads"
    settings.parse_dir = runtime_dir / "parsed"
    settings.reports_dir = runtime_dir / "reports"
    settings.meta_file = runtime_dir / "reports_index.json"
    settings.duckdb_file = runtime_dir / "analytics.duckdb"
    settings.ensure_dirs()

    root = Path(__file__).resolve().parents[2] / "data" / "report20260330"
    table_path = root / "chapter1_section1.xlsx"
    chart_path = root / "chapter1_section3.xlsx"
    assert table_path.exists(), f"template not found: {table_path}"
    assert chart_path.exists(), f"template not found: {chart_path}"

    files = [
        (
            "files",
            (
                table_path.name,
                table_path.read_bytes(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        ),
        (
            "files",
            (
                chart_path.name,
                chart_path.read_bytes(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        ),
    ]

    upload_resp = client.post(
        "/consultant/api/v1/reports/upload-folder",
        files=files,
        data={"report_key": "folder-mixed-demo", "report_name": "Folder Mixed Demo", "mode": "replace"},
    )
    assert upload_resp.status_code == 200, upload_resp.json()
    body = upload_resp.json()
    assert body["data"]["report_key"] == "folder-mixed-demo"
    assert body["data"]["status"] == "queued"
    task_id = body["data"]["task_id"]

    task_data = _wait_upload_folder_task(task_id)
    assert task_data["status"] == "succeeded"
    assert task_data["succeeded_files"] == 2
    assert task_data["failed_files"] == 0
    assert task_data["result"]["report_key"] == "folder-mixed-demo"

    detail_resp = client.get("/consultant/api/v1/reports/folder-mixed-demo")
    assert detail_resp.status_code == 200
    payload = detail_resp.json()["data"]["payload"]
    chapters = payload["chapters"]
    assert chapters

    all_sections = []
    for chapter in chapters:
        all_sections.extend(chapter.get("sections", []))

    section_keys = {sec.get("section_key") for sec in all_sections if isinstance(sec, dict)}
    assert "section_1" in section_keys
    assert "section_3" in section_keys

    section_titles = {
        sec.get("section_key"): sec.get("title")
        for sec in all_sections
        if isinstance(sec, dict)
    }
    section_names = {
        sec.get("section_key"): sec.get("section_name")
        for sec in all_sections
        if isinstance(sec, dict)
    }
    assert section_names.get("section_1") == section_titles.get("section_1")
    assert section_names.get("section_3") == section_titles.get("section_3")

    upload_subdir = settings.upload_dir / "folder-mixed-demo"
    assert upload_subdir.exists()
    assert (upload_subdir / table_path.name).exists()
    assert (upload_subdir / chart_path.name).exists()
