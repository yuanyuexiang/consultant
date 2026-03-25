# Report 管理端开发指南（Next.js）

本指南面向前端管理端开发，目标是基于现有后端 API，完成 report 的上传、编辑保存、查询全流程。

## 1. 目标与范围

- 框架：Next.js（建议 App Router + TypeScript）
- 业务范围：report 管理，不含权限系统
- 对接后端：`/consultant/api/v1/reports/*`
- 管理动作：
  1. 上传 Excel
  2. 保存 report 编辑
  3. 查看 report 列表与详情
  4. 查看 section 级内容
  6. report 级增删改查（Create/Update/Delete/Get）

## 2. 建议技术栈

- Next.js 15+
- TypeScript
- UI：Ant Design 或 shadcn/ui（二选一，统一风格）
- 状态管理：TanStack Query（强烈建议）
- 表单：React Hook Form + Zod
- HTTP：axios 或 fetch（统一封装）

## 3. 目录建议

```txt
src/
  app/
    (admin)/
      reports/
        page.tsx                  # 报告列表
        [reportKey]/
          page.tsx                # 报告详情
          sections/[sectionKey]/
            page.tsx              # section 详情
        upload/page.tsx           # 上传页
  features/reports/
    api.ts                        # report 相关 API 调用
    hooks.ts                      # React Query hooks
    types.ts                      # DTO 与领域类型
    utils.ts                      # 响应转换工具
    components/
      report-list-table.tsx
      report-actions.tsx
      upload-form.tsx
      save-dialog.tsx
  lib/
    http.ts                       # axios/fetch 封装
    env.ts                        # 环境变量读取
```

## 4. 环境变量与网关

建议通过 Next.js 环境变量管理后端地址：

```bash
NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000
NEXT_PUBLIC_API_PREFIX=/consultant/api
```

统一拼接：

- 最终请求前缀：`${NEXT_PUBLIC_API_BASE_URL}${NEXT_PUBLIC_API_PREFIX}`

## 5. 后端契约要点

统一响应结构：

```json
{
  "code": 0,
  "message": "ok",
  "data": {},
  "error": null
}
```

前端建议规则：

1. `HTTP 2xx + code===0` 视为成功。
2. `HTTP 4xx/5xx` 或 `code!==0` 统一进入错误处理。
3. 优先展示 `error.detail`，其次 `message`。

## 6. 页面与交互流程

### 6.1 报告列表页 `/reports`

展示字段建议：

- report_key
- name
- type
- status
- updated_at

操作按钮建议：

- 查看详情
- 保存（Save）

调用接口：

- `GET /v1/reports`

### 6.2 上传页 `/reports/upload`

表单字段：

- file（.xlsx）
- report_key（可选）

调用接口：

- `POST /v1/reports/upload-excel`

交互建议：

1. 上传中禁用按钮，展示进度。
2. 成功后跳转到对应 report 详情页。
3. 展示 `parsed_charts`、`parsed_points`。

### 6.3 报告详情页 `/reports/[reportKey]`

调用接口：

- `GET /v1/reports/{report_key}`

展示模块：

1. 报告基础信息（id/name/type/status）
2. chapters 列表
3. 每个 chapter 下 sections 与 chart 数量
4. 操作区：Save

### 6.4 Section 详情页 `/reports/[reportKey]/sections/[sectionKey]`

调用接口：

- `GET /v1/reports/{report_key}/sections/{section_key}`

展示重点：

- section 标题与元信息
- `content_items.charts` 的结构化展示
- table/line 的数据预览

## 7. 核心动作实现

### 7.0 Report CRUD（管理端）

- 新增：`POST /v1/reports`
- 修改：`PATCH /v1/reports/{report_key}`
- 删除：`DELETE /v1/reports/{report_key}`
- 查询：`GET /v1/reports`、`GET /v1/reports/{report_key}`

建议：

1. 新增和修改成功后刷新列表缓存。
2. 删除前弹二次确认，并提示不可恢复风险。
3. 删除成功后主动跳转回列表页。
4. 编辑请求优先使用 `chapters` 作为结构输入，`sections` 仅用于兼容旧数据迁移。

### 7.1 保存动作

- 接口：`PATCH /v1/reports/{report_key}`

建议：

1. 保存后立即生效（无双态流程）。
2. section 描述文本写入 `content` 字段。
3. 保存成功后刷新列表与详情。

## 8. TypeScript 类型建议

```ts
export type ApiResponse<T> = {
  code: number;
  message: string;
  data: T | null;
  error?: { field?: string; detail: string } | null;
};

export type ReportListItem = {
  report_key: string;
  id: string;
  name: string;
  type: string;
  status: string;
  updated_at: string;
};
```

## 9. API 封装建议

建议把 API 层与 UI 分离：

- `features/reports/api.ts` 只负责请求与返回类型。
- `features/reports/hooks.ts` 负责缓存、重试、失效刷新。
- 页面组件只消费 hooks。

示例（伪代码）：

```ts
export async function getReports() {
  return http.get<ApiResponse<{ items: ReportListItem[] }>>("/v1/reports");
}

export async function updateReport(report_key: string, payload: Record<string, unknown>) {
  return http.patch<ApiResponse<{ report_key: string; payload_hash: string; saved_at: string }>>(
    `/v1/reports/${report_key}`,
    payload,
  );
}
```

## 10. 错误处理与用户提示

建议统一错误提示函数：

1. 上传失败：展示模板错误详情。
2. 保存失败：展示校验失败字段。
3. 查询失败：提示 report_key 或 section_key 错误。

建议文案：

- `操作失败：{error.detail}`

## 11. 联调顺序（推荐）

1. 跑通 `GET /v1/reports`（确认 API 连通）
2. 跑通上传
3. 跑通新增/修改保存
4. 跑通详情与 section

## 12. 本地调试脚本（示例）

```bash
# 启动后端
uv run uvicorn app.main:app --reload

# 启动前端（示例）
pnpm dev
```

## 13. 验收清单

1. 列表页可见所有 report。
2. 上传 Excel 后有成功提示与解析统计。
3. 保存后报告详情可直接读取最新结果。
4. section 的 `content` 字段存在并可展示描述文字。
5. 详情页可渲染 chapters/sections 与 chart 基础信息。
6. section 页面能查看指定 section payload。

## 14. Steps 向导蓝图

如果 report 编辑复杂度较高，建议优先使用 Antd Steps 方案，详见：

- docs/nextjs-report-editor-steps-blueprint.md
