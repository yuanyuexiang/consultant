# Report Platform Backend

Python + FastAPI backend for Excel-driven BI report management.

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

- Swagger: http://127.0.0.1:8000/consultant/docs
- Management API guide: docs/api-management.md
- Next.js admin guide: docs/nextjs-report-admin-guide.md
- Next.js Steps blueprint: docs/nextjs-report-editor-steps-blueprint.md

## 5. Quick Flow (With Sample Excel)

```bash
# 1) upload sample excel
curl -X POST "http://127.0.0.1:8000/consultant/api/v1/reports/upload-excel" \
	-F "file=@data/Slide 4 Origination Trends.xlsx" \
	-F "report_key=data-analytics"

# 2) read current report (save takes effect immediately)
curl "http://127.0.0.1:8000/consultant/api/v1/reports/data-analytics"
```

## 6. Test

```bash
uv run pytest
```
