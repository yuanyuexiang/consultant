# Report Platform Backend

Python + FastAPI backend for Excel-driven BI report assembly.

## 1. Environment

- Python 3.11+
- uv

## 2. Install

```bash
uv sync
```

## 3. Run

```bash
uv run uvicorn app.main:app --reload
```

## 4. API Docs

- Swagger: http://127.0.0.1:8000/docs
- Management API guide: docs/api-management.md

## 5. Quick Flow (With Sample Excel)

```bash
# 1) upload sample excel
curl -X POST "http://127.0.0.1:8000/v1/reports/upload-excel" \
	-F "file=@data/Slide 4 Origination Trends.xlsx" \
	-F "report_key=data-analytics"

# 2) assemble report
curl -X POST "http://127.0.0.1:8000/v1/reports/assemble" \
	-H "Content-Type: application/json" \
	-d '{"report_key":"data-analytics"}'

# 3) publish snapshot
curl -X POST "http://127.0.0.1:8000/v1/reports/data-analytics/publish" \
	-H "Content-Type: application/json" \
	-d '{"snapshot_id":10001,"comment":"first publish"}'
```

## 6. Test

```bash
uv run pytest
```
