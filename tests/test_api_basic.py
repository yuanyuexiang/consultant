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
    settings.snapshot_dir = runtime_dir / "snapshots"
    settings.meta_file = runtime_dir / "reports_index.json"
    settings.ensure_dirs()

    create_payload = {
        "report_key": "crud-demo",
        "name": "CRUD Demo",
        "type": "analytics",
        "status": "draft",
        "sections": [],
    }
    create_resp = client.post("/consultant/api/v1/reports", json=create_payload)
    assert create_resp.status_code == 200
    create_body = create_resp.json()
    assert create_body["code"] == 0
    assert create_body["data"]["report_key"] == "crud-demo"

    update_resp = client.patch(
        "/consultant/api/v1/reports/crud-demo",
        json={"name": "CRUD Demo Updated", "status": "published"},
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
