from typing import Any

from pydantic import BaseModel, Field


class UploadExcelData(BaseModel):
    report_key: str
    source_file: str
    parsed_charts: int
    parsed_points: int


class AssembleRequest(BaseModel):
    report_key: str


class AssembleData(BaseModel):
    report_key: str
    payload_hash: str


class ReportListItem(BaseModel):
    report_key: str
    id: str
    name: str
    type: str
    status: str


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
