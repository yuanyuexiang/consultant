from pathlib import Path

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

client = TestClient(app)


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
        / "timeseries_chart_data_template.xlsx"
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

    assert "filters" in chart["meta"]
    assert "filter1" in chart["meta"]["filters"]
    assert "filter2" in chart["meta"]["filters"]
    assert "source_rows" in chart["meta"]
    assert chart["echarts"]["xAxis"]["type"] == "time"
