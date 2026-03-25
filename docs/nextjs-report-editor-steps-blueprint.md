# Next.js 管理端 Report 编辑器（Antd Steps）实施蓝图

本蓝图基于当前后端能力与管理端目标，给出一套可直接落地的 Steps 向导式编辑方案（当前无草稿/发布二态）。

## 1. 设计目标

- 将复杂 report 编辑拆成线性步骤，降低一次性认知负担。
- 每步可单独校验、保存草稿、回退修改。
- 保存即生效，无单独发布步骤。
- 兼容现有 API（包含 report CRUD 与上传/组装/发布）。

## 2. 页面路由建议

- 新建页：/reports/new
- 编辑页：/reports/[reportKey]/edit
- 预览页：/reports/[reportKey]

推荐把 Steps 放在新建页和编辑页共用组件中。

## 3. Steps 分解（4 步）

### Step 1: 基础信息

目标：创建或更新 report 基本元数据。

字段：

- report_key（新建必填，编辑只读）
- name（必填）
- type（必填）
- status（draft/published）

后端映射：

- 新建：POST /consultant/api/v1/reports
- 编辑：PATCH /consultant/api/v1/reports/{report_key}

校验：

- report_key 格式：小写+中划线
- name 非空且长度限制
- type 非空

### Step 2: Section 编排

目标：维护 sections 结构与顺序。

操作：

- 新增 section
- 删除 section
- 修改 title/subtitle/layout/content
- 拖拽排序并自动重算 order

后端映射：

- PATCH /consultant/api/v1/reports/{report_key}
- 通过 sections 全量回写

校验：

- section_key 唯一
- order 唯一
- title 非空

### Step 3: 图表配置

目标：在 section 维度维护 charts 内容。

操作：

- 新增/删除 chart
- 编辑 chart title/subtitle/meta
- line/table 两类差异化表单

后端映射：

- PATCH /consultant/api/v1/reports/{report_key}
- 通过 sections[].content_items.charts 全量回写

校验：

- chart_id 唯一
- chart_type 必填
- line 图必须有 echarts 基础结构

### Step 4: 组装与预览

目标：触发组装，拿到 snapshot 并展示预览。

操作：

- 点击 Assemble
- 拉取 report payload
- 按 section / chart 做可视化预览

后端映射：

- POST /consultant/api/v1/reports/assemble
- GET /consultant/api/v1/reports/{report_key}
- GET /consultant/api/v1/reports/{report_key}/sections/{section_key}

门禁：

- 未组装成功，禁止进入发布步骤

### Step 4: 保存确认

目标：确认当前编辑与组装结果并保存。

后端映射：

- PATCH /consultant/api/v1/reports/{report_key}

门禁：

- 推荐先完成组装预览再保存

## 4. 前端状态模型（建议）

核心状态：

- draftPayload: 当前编辑中的 report
- savedSnapshotId: 最近一次保存产生的快照（可选）
- assembledSnapshotId: 最近一次组装请求完成标记（可选）
- publishReady: 是否满足发布条件

建议状态来源：

- 表单局部状态：React Hook Form
- 服务端状态：TanStack Query
- 跨步骤共享状态：Zustand 或 Context

## 5. 组件拆分建议

- ReportEditorSteps: Steps 容器和导航逻辑
- StepBasicInfoForm
- StepSectionsBoard
- StepChartsBoard
- StepAssemblePreview
- StepPublishConfirm

通用组件：

- UnsavedChangesGuard
- ReportStatusBadge
- SectionTreePanel
- ChartConfigPanel

## 6. 关键交互规则

- 允许点击已完成步骤回跳，不允许跳过必经步骤。
- 每步都提供 Save Draft 按钮。
- 切换步骤前自动校验并提示未保存内容。
- 组装成功后自动刷新预览。
- 保存成功后跳转到 report 详情页。

## 7. API 调用顺序（编辑场景）

- 初次创建：POST /reports
- 多次编辑：PATCH /reports/{report_key}
- 组装：POST /reports/assemble
- 预览：GET /reports/{report_key}

注意：所有实际路径都要带 /consultant/api 前缀。

## 8. 错误处理策略

- 服务端返回 code != 0：展示 error.detail
- 409（重复 report_key）：聚焦到 report_key 输入框
- 404（report 不存在）：提示返回列表
- 422（组装校验失败）：展示具体字段错误并定位到对应步骤

## 9. 可直接抄用的数据类型（前端）

```ts
export type StepKey =
  | "basic"
  | "sections"
  | "charts"
  | "assemble";

export type EditorState = {
  currentStep: StepKey;
  reportKey: string;
  draftPayload: Record<string, unknown> | null;
  assembledSnapshotId: number | null;
  payloadHash: string | null;
  dirty: boolean;
};
```

## 10. 迭代建议

第 1 期（最快上线）：

- 完成 5 步流程
- 完成 report 级 CRUD + assemble + save
- 完成基础错误提示

第 2 期（体验优化）：

- section 拖拽排序
- chart 表单 schema 化
- 发布前 diff 对比

第 3 期（治理增强）：

- 审批流
- 操作审计
- 细粒度权限
