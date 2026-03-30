from typing import Any

from pydantic import BaseModel, Field


class UploadExcelData(BaseModel):
    report_key: str
    source_file: str
    parsed_charts: int
    parsed_points: int


class UploadFolderFileResult(BaseModel):
    source_file: str
    chapter_key: str | None = None
    section_key: str | None = None
    parsed_charts: int = 0
    parsed_points: int = 0
    status: str
    detail: str | None = None


class UploadFolderData(BaseModel):
    report_key: str
    total_files: int
    succeeded_files: int
    failed_files: int
    files: list[UploadFolderFileResult] = Field(default_factory=list)


class UploadFolderTaskAcceptedData(BaseModel):
    task_id: str
    report_key: str
    status: str
    total_files: int
    submitted_at: str


class UploadFolderTaskStatusData(BaseModel):
    task_id: str
    report_key: str
    status: str
    phase: str
    total_files: int
    processed_files: int
    succeeded_files: int
    failed_files: int
    submitted_at: str
    started_at: str | None = None
    finished_at: str | None = None
    files: list[UploadFolderFileResult] = Field(default_factory=list)
    detail: str | None = None
    result: UploadFolderData | None = None


class ReportListItem(BaseModel):
    report_key: str
    id: str
    name: str
    type: str
    status: str
    updated_at: str | None = None


class ReportListData(BaseModel):
    items: list[ReportListItem] = Field(default_factory=list)


class ReportPayloadData(BaseModel):
    payload: dict[str, Any]


class SectionPayloadData(BaseModel):
    section_key: str
    section: dict[str, Any]


class ReportCreateRequest(BaseModel):
    report_key: str
    id: str | None = None
    name: str
    type: str = "analytics"
    status: str = "active"
    chapters: list[dict[str, Any]] = Field(default_factory=list)
    sections: list[dict[str, Any]] = Field(default_factory=list)


class ReportUpdateRequest(BaseModel):
    name: str | None = None
    type: str | None = None
    status: str | None = None
    chapters: list[dict[str, Any]] | None = None
    sections: list[dict[str, Any]] | None = None


class ReportMutationData(BaseModel):
    report_key: str
    payload_hash: str
    saved_at: str


class DeleteReportData(BaseModel):
    report_key: str
    deleted: bool
