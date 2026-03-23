# Report Platform Management API

This document is for the frontend management console integration.

## 1. Base Info

- Base URL: `http://127.0.0.1:8000`
- Swagger: `http://127.0.0.1:8000/docs`
- Content-Type:
  - `multipart/form-data` for upload
  - `application/json` for other APIs
- Unified response envelope:

```json
{
  "code": 0,
  "message": "ok",
  "data": {},
  "error": null
}
```

## 2. Upload Sample Excel

Sample file in this repository:

- `data/Slide 4 Origination Trends.xlsx`

Upload command:

```bash
curl -X POST "http://127.0.0.1:8000/v1/reports/upload-excel" \
  -F "file=@data/Slide 4 Origination Trends.xlsx" \
  -F "report_key=data-analytics"
```

Success response example:

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "report_key": "data-analytics",
    "source_file": "Slide 4 Origination Trends.xlsx",
    "parsed_charts": 12,
    "parsed_points": 252
  },
  "error": null
}
```

## 3. Assemble Report

- Endpoint: `POST /v1/reports/assemble`
- Body:

```json
{
  "report_key": "data-analytics"
}
```

Command:

```bash
curl -X POST "http://127.0.0.1:8000/v1/reports/assemble" \
  -H "Content-Type: application/json" \
  -d '{"report_key":"data-analytics"}'
```

Success response example:

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "report_key": "data-analytics",
    "snapshot_id": 10001,
    "payload_hash": "sha256:..."
  },
  "error": null
}
```

## 4. Publish Report

- Endpoint: `POST /v1/reports/{report_key}/publish`
- Path param: `report_key`
- Body:

```json
{
  "snapshot_id": 10001,
  "comment": "first publish"
}
```

Command:

```bash
curl -X POST "http://127.0.0.1:8000/v1/reports/data-analytics/publish" \
  -H "Content-Type: application/json" \
  -d '{"snapshot_id":10001,"comment":"first publish"}'
```

Success response example:

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "report_key": "data-analytics",
    "published_version": 1,
    "snapshot_id": 10001
  },
  "error": null
}
```

## 5. Query APIs

### 5.1 List Reports

- Endpoint: `GET /v1/reports`

```bash
curl "http://127.0.0.1:8000/v1/reports"
```

### 5.2 Get Published Report Payload

- Endpoint: `GET /v1/reports/{report_key}`

```bash
curl "http://127.0.0.1:8000/v1/reports/data-analytics"
```

### 5.3 Get One Section

- Endpoint: `GET /v1/reports/{report_key}/sections/{section_key}`

```bash
curl "http://127.0.0.1:8000/v1/reports/data-analytics/sections/origination_trends"
```

## 6. Recommended Call Sequence

1. Upload Excel
2. Assemble report
3. Publish snapshot
4. Query report/section for frontend rendering

## 7. Common Error Cases

### 7.1 Wrong File Type

- Condition: upload non-`.xlsx`
- Example:

```json
{
  "code": 1001,
  "message": "invalid request",
  "data": null,
  "error": {
    "field": "file",
    "detail": "unsupported excel template"
  }
}
```

### 7.2 Parsed Data Missing

- Condition: call assemble before upload
- Example:

```json
{
  "code": 1004,
  "message": "invalid request",
  "data": null,
  "error": {
    "field": "report_key",
    "detail": "parsed data not found for data-analytics"
  }
}
```

### 7.3 Snapshot Not Found

- Condition: publish with wrong `snapshot_id`
- Example:

```json
{
  "code": 1004,
  "message": "invalid request",
  "data": null,
  "error": {
    "field": "snapshot_id",
    "detail": "snapshot not found: data-analytics/10001"
  }
}
```
