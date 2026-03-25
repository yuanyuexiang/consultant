# Report Platform Management API

This document is for the frontend management console integration.

## 1. Base Info

- Base URL: `http://127.0.0.1:8000`
- API Prefix: `/consultant/api`
- Swagger: `http://127.0.0.1:8000/consultant/docs`
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
curl -X POST "http://127.0.0.1:8000/consultant/api/v1/reports/upload-excel" \
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

## 3. Query APIs

### 3.1 List Reports

- Endpoint: `GET /v1/reports`

```bash
curl "http://127.0.0.1:8000/consultant/api/v1/reports"
```

### 3.2 Get Report Payload

- Endpoint: `GET /v1/reports/{report_key}`

```bash
curl "http://127.0.0.1:8000/consultant/api/v1/reports/data-analytics"
```

### 3.3 Get One Section

- Endpoint: `GET /v1/reports/{report_key}/sections/{section_key}`

```bash
curl "http://127.0.0.1:8000/consultant/api/v1/reports/data-analytics/sections/origination_trends"
```

## 4. Report CRUD APIs

### 4.1 Create Report

- Endpoint: `POST /v1/reports`

```bash
curl -X POST "http://127.0.0.1:8000/consultant/api/v1/reports" \
  -H "Content-Type: application/json" \
  -d '{
    "report_key": "crud-demo",
    "name": "CRUD Demo",
    "type": "analytics",
    "status": "active",
    "chapters": [
      {
        "chapter_key": "chapter_1",
        "title": "Overview",
        "subtitle": null,
        "order": 1,
        "status": "draft",
        "sections": []
      }
    ]
  }'
```

### 4.2 Update Report

- Endpoint: `PATCH /v1/reports/{report_key}`

```bash
curl -X PATCH "http://127.0.0.1:8000/consultant/api/v1/reports/crud-demo" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "CRUD Demo Updated",
    "status": "active",
    "chapters": [
      {
        "chapter_key": "chapter_1",
        "title": "Overview Updated",
        "subtitle": null,
        "order": 1,
        "status": "active",
        "sections": []
      }
    ]
  }'
```

Section 描述文本约定：

- 每个 section 使用 `content` 字段存放描述性文字（后端会保证该字段存在）。
- `content_items` 用于结构化内容（charts、text blocks）。

### 4.3 Delete Report

- Endpoint: `DELETE /v1/reports/{report_key}`

```bash
curl -X DELETE "http://127.0.0.1:8000/consultant/api/v1/reports/crud-demo"
```

## 5. Recommended Call Sequence

1. Upload Excel
2. Save report (create/update)
3. Query report/section for frontend rendering

## 6. Common Error Cases

### 6.1 Wrong File Type

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

### 6.2 Parsed Data Missing

- Condition: query section that does not exist
- Example:

```json
{
  "code": 1004,
  "message": "invalid request",
  "data": null,
  "error": {
    "field": "section_key",
    "detail": "section not found: non-exist-section"
  }
}
```

### 6.3 Report Not Found

- Condition: query/update/delete with wrong `report_key`
