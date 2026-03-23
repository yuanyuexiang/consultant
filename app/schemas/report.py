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
    snapshot_id: int
    payload_hash: str


class PublishRequest(BaseModel):
    snapshot_id: int
    comment: str | None = None


class PublishData(BaseModel):
    report_key: str
    published_version: int
    snapshot_id: int


class ReportListItem(BaseModel):
    report_key: str
    id: str
    name: str
    type: str
    status: str
    published_version: int = 0


class ReportListData(BaseModel):
    items: list[ReportListItem] = Field(default_factory=list)


class ReportPayloadData(BaseModel):
    payload: dict[str, Any]


class SectionPayloadData(BaseModel):
    section_key: str
    section: dict[str, Any]
