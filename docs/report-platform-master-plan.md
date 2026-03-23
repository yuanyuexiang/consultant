# BI 报告平台整合方案（Excel 驱动版）

## 1. 项目目标

构建一个由 Excel 文件驱动的可配置报告平台，实现：

1. report 结构化配置（report/section/chart）
2. 通过 Excel 上传解析 dataset 数据
3. 自动填充 ECharts option 并生成报告 JSON
4. 前端只读已发布快照 JSON

## 1.1 MVP 主线（当前版本统一口径）

第一版按最简路径落地：

1. 上传标准化 Excel 文件。
2. 后端解析 Excel，提取 report/section/chart 属性与 dataset 数据。
3. 后端按规则组装生成最终报告 JSON。
4. 前端读取最新发布的报告快照。

说明：第一版不引入数据库依赖，不做复杂配置编辑器，不做动态权限。

## 2. 统一实体模型

## 2.0 业务语义约定（后端实现口径）

一个报告（report）由多个段落（section）组成。

每个段落（section）由以下部分组成：

1. title（标题）
2. subtitle（子标题）
3. content（内容模板）

其中 content 可以包含多个图表（chart）。

每个图表（chart）至少包含两类信息：

1. echarts option（展示配置）
2. dataset 数据（从 Excel 解析出的序列或表格）

后端开发必须遵循以下边界：

1. chart 的 option 与数据结果分开建模，不互相硬编码。
2. section 只负责编排内容，不直接承担指标计算。
3. report 只作为容器，不直接存放图表计算逻辑。
4. 数据转换逻辑统一下沉到解析与组装流程中。

建议输出契约：

1. report -> sections[]
2. section -> title, subtitle, content_items
3. content_items -> charts[]
4. chart -> chart_id, chart_type, title, subtitle, echarts, table_data/meta

## 2.1 report

作用：报告容器。

核心字段建议：

1. id
2. report_key
3. name
4. type
5. status
6. published_version
7. created_at
8. updated_at

## 2.2 section

作用：报告段落（页面内容分区）。

核心字段建议：

1. id
2. report_id
3. section_key
4. title
5. subtitle
6. order_no
7. layout
8. visible
9. content_template

## 2.3 chart

作用：图表定义与展示配置。

核心字段建议：

1. id
2. section_id
3. chart_key
4. chart_type
5. title
6. subtitle
7. order_no
8. option_template_json
9. dataset_binding_json

## 2.4 dataset

作用：图表绑定的数据（由 Excel 解析得到）。

核心字段建议：

1. dataset_key
2. source_sheet
3. formatter
4. unit
5. precision
6. series_name
7. point_time
8. metric_value

## 2.5 render_snapshot

作用：发布后的可读快照（前端读取对象）。

核心字段建议：

1. snapshot_id
2. report_key
3. version
4. payload_json
5. payload_hash
6. source_file
7. generated_at

## 3. 分层架构

## 3.1 Parse 层

职责：解析 Excel 原始内容。

规则：

1. 按固定模板读取 sheet 和字段。
2. 保留原始时间与数值，不做补零。
3. 输出标准化中间结构。

## 3.2 Normalize 层

职责：规范化为统一图表数据结构。

统一输出字段：

1. report_key
2. section_key
3. chart_id
4. series_name
5. point_time
6. metric_value
7. formatter

规则：

1. 时间粒度统一（建议 month_end）
2. 缺失值保持 null

## 3.3 Assembly 层

职责：将 option 模板与规范化数据组装为报告 JSON。

规则：

1. 输出结构兼容当前 data/echarts 协议。
2. chart 配置与数据结果解耦。
3. 组装完成写入 render_snapshot。

组装输入最小集合：

1. report 字段（id、name、type、status）
2. section 字段（title、subtitle、content_template、order）
3. chart 字段（chart_type、title、subtitle、option_template_json）
4. dataset 字段（series、point_time、metric_value）

组装输出：

1. 单个 report 完整 JSON（结构与 data/echarts/*.echarts.json 对齐）
2. 写入 render_snapshot 作为前端读取源

## 3.4 Publish 层

职责：版本发布与追溯。

规则：

1. 发布前做基础结构检查。
2. 发布后更新当前 published_version。
3. 保留 source_file 与 payload_hash 便于追溯。

## 4. Excel 模板约定

建议使用 4 个 sheet：

1. report_meta：report_key, name, type, status
2. sections：section_key, title, subtitle, order_no, layout
3. charts：chart_id, section_key, chart_type, title, subtitle, formatter, option_template_json
4. chart_points：chart_id, series_name, point_time, metric_value

可选 sheet：

1. chart_table_rows：chart_id, row_no, col_key, col_value

## 5. API 设计

统一 API（展示端与管理端共用）：

1. POST /v1/reports/upload-excel （上传并解析 Excel）
2. POST /v1/reports/assemble （按最新解析结果组装报告）
3. POST /v1/reports/{report_key}/publish （发布当前组装结果）
4. GET /v1/reports （查询报告列表）
5. GET /v1/reports/{report_key} （查询报告详情）
6. GET /v1/reports/{report_key}/sections/{section_key}

当前阶段约定：

1. 展示端与管理端共用同一套 API。
2. 暂不引入鉴权与角色控制。
3. 先完成上传、解析、组装、发布闭环。

## 5.1 接口契约样例（后端开发可直接落地）

统一响应结构（成功）：

```json
{
  "code": 0,
  "message": "ok",
  "data": {}
}
```

统一响应结构（失败）：

```json
{
  "code": 1001,
  "message": "invalid request",
  "error": {
    "field": "file",
    "detail": "unsupported excel template"
  }
}
```

上传 Excel：POST /v1/reports/upload-excel

请求：multipart/form-data

字段：

1. file（xlsx）
2. report_key（可选，覆盖模板中的 report_key）

响应体：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "report_key": "data-analytics",
    "source_file": "analytics_20260323.xlsx",
    "parsed_charts": 12,
    "parsed_points": 252
  }
}
```

组装报告：POST /v1/reports/assemble

请求体：

```json
{
  "report_key": "data-analytics"
}
```

响应体：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "report_key": "data-analytics",
    "snapshot_id": 10001,
    "payload_hash": "sha256:xxxxx"
  }
}
```

发布报告：POST /v1/reports/{report_key}/publish

请求体：

```json
{
  "snapshot_id": 10001,
  "comment": "first publish"
}
```

响应体：

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "report_key": "data-analytics",
    "published_version": 1,
    "snapshot_id": 10001
  }
}
```

## 6. 作业流程

1. 上传 Excel 文件
2. 解析模板并做字段校验
3. 规范化数据结构
4. 组装报告 JSON
5. 执行质量校验
6. 生成 render_snapshot
7. 发布 snapshot 为当前版本

## 7. 质量门禁

1. 时间轴单调递增
2. xAxis 与 series 点位长度一致
3. 缺失值必须为 null
4. formatter 与指标口径一致
5. payload 可追溯到 source_file 与 payload_hash

第一版最小校验（必须实现）：

1. section 的 order_no 在同一 report 内唯一。
2. 每个 chart 至少有一个 series。
3. 每个 series 的数据点数量与 xAxis 一致。
4. 缺失数据必须是 null，不允许用 0 补齐。

## 8. Analytics 样板映射（orig_1 ~ orig_12）

section_key: origination_trends

系列统一建议：

1. Platform
2. Tapes w High Grade Mix
3. Tapes w Platform Mix

指标映射：

1. orig_1 -> Orig_Bal -> thousands
2. orig_2 -> Avg_Orig_Bal -> thousands
3. orig_3 -> Term -> decimal
4. orig_4 -> IR -> percentage
5. orig_5 -> APR -> percentage
6. orig_6 -> FICO -> decimal
7. orig_7 -> PTI -> percentage
8. orig_8 -> DTI -> percentage
9. orig_9 -> Annual_Inc -> thousands
10. orig_10 -> Debt_Consolidation_Pct -> percentage
11. orig_11 -> Existing_Borrower_Pct -> percentage
12. orig_12 -> Grade_1_Pct -> percentage

## 9. 里程碑

M1（本周）：

1. 完成 Excel 模板定义与上传接口
2. 完成解析与组装脚本初版
3. 完成 report JSON 生成与前端联调

M2（下周）：

1. 接入 platform 和 financial 模板
2. 完成发布链路
3. 完成新旧 JSON 对账报告

## 10. 待确认清单

1. Excel 模板字段最终清单
2. 时间字段格式（YYYY-MM 或 YYYY-MM-DD）
3. 百分比和千位单位展示规则
4. 三个 series 的业务分群规则
5. 上传频率与发布频率

## 11. 现有实现关联

1. 当前前端消费结构：data/echarts/*.echarts.json
2. 当前 report 样例：data/echarts/rpt_analytics.echarts.json

## 12. Report JSON 协议（正式）

本节定义后端最终输出给前端的报告 JSON 结构，作为前后端固定契约。

### 12.1 顶层字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| id | string | 是 | 报告唯一标识，例如 rpt_analytics |
| report_key | string | 是 | 业务键，例如 data-analytics |
| name | string | 是 | 报告名称 |
| type | string | 是 | 报告类型 |
| status | string | 是 | 报告状态，建议 published/draft |
| published_version | number | 是 | 当前发布版本号 |
| generated_at | string | 否 | 生成时间，ISO8601 |
| source_file | string | 否 | 来源 Excel 文件名 |
| payload_hash | string | 否 | 报告内容哈希 |
| sections | array | 是 | 段落列表 |

### 12.2 section 字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| id | string | 否 | section 唯一标识 |
| section_key | string | 是 | 段落业务键 |
| title | string | 是 | 段落标题 |
| subtitle | string/null | 否 | 段落子标题 |
| status | string | 否 | 段落状态 |
| order | number | 是 | 段落顺序 |
| layout | string | 否 | 布局方式 |
| content | string/null | 否 | 内容模板，例如 {{chart:orig_1}} |
| content_items | object | 是 | 内容对象 |

### 12.3 chart 字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| chart_id | string | 是 | 图表唯一标识 |
| chart_type | string | 是 | 图表类型，line/table 等 |
| title | string | 是 | 图表标题 |
| subtitle | string/null | 否 | 图表子标题 |
| echarts | object/null | 条件必填 | line 图必填，table 图可为 null |
| table_data | object/null | 条件必填 | table 图必填，line 图可为 null |
| meta | object | 是 | 元信息（formatter、metric_name 等） |

### 12.4 line 图约定

1. `chart_type = line`
2. `echarts.xAxis.data` 必须存在且有序
3. `echarts.series[].data.length` 必须与 `xAxis.data.length` 一致
4. 缺失点使用 `null`，禁止补 `0`

### 12.5 table 图约定

1. `chart_type = table`
2. `table_data.columns` 必须包含 key/title/align
3. `table_data.rows` 为对象数组，键名与 columns.key 一致

### 12.6 最小示例

```json
{
  "id": "rpt_analytics",
  "report_key": "data-analytics",
  "name": "Data Analytics",
  "type": "analytics",
  "status": "published",
  "published_version": 1,
  "sections": [
    {
      "section_key": "origination_trends",
      "title": "Origination Trends",
      "subtitle": null,
      "order": 1,
      "content_items": {
        "charts": [
          {
            "chart_id": "orig_1",
            "chart_type": "line",
            "title": "Total Origination Balance",
            "subtitle": null,
            "echarts": {
              "xAxis": { "type": "category", "data": ["2016-06", "2016-12"] },
              "yAxis": { "type": "value", "name": "Origination Balance (k)" },
              "series": [
                { "name": "Platform", "type": "line", "data": [41366576.93, 80030708.74] }
              ]
            },
            "table_data": null,
            "meta": { "formatter": "thousands", "metric_name": "Orig_Bal", "display_precision": 3 }
          }
        ],
        "kind": null,
        "items": null
      }
    }
  ]
}
```
