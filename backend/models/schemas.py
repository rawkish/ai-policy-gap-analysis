"""
Pydantic request/response schemas.
"""
from __future__ import annotations
from typing import Literal, Optional
from pydantic import BaseModel, Field


# ─── Ingest ───────────────────────────────────────────────────────────────────

class IngestResponse(BaseModel):
    filename: str
    chunks_stored: int
    status: Literal["success", "error"]
    message: str = ""


# ─── Documents ────────────────────────────────────────────────────────────────

class DocumentInfo(BaseModel):
    filename: str
    chunk_count: int
    ingested_at: Optional[str] = None


class DocumentsResponse(BaseModel):
    documents: list[DocumentInfo]


# ─── Collections ──────────────────────────────────────────────────────────────

class CollectionInfo(BaseModel):
    name: str


class CollectionsResponse(BaseModel):
    collections: list[str]
    default: str


class CreateCollectionRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)


class CreateCollectionResponse(BaseModel):
    name: str
    created: bool


# ─── Control Areas ────────────────────────────────────────────────────────────

class ControlArea(BaseModel):
    id: str
    name: str
    label: str
    placeholder: str
    category: str = "general"


class ControlAreasResponse(BaseModel):
    control_areas: list[ControlArea]


# ─── Analysis Request ─────────────────────────────────────────────────────────

class FieldInput(BaseModel):
    control_area_id: str
    control_area_name: str
    description: str = Field(..., min_length=1, max_length=5000)


class AnalyseRequest(BaseModel):
    fields: list[FieldInput] = Field(..., min_length=1)
    collection_name: str = Field(..., min_length=1)


# ─── Analysis Response ────────────────────────────────────────────────────────

# The three possible status values — must match exactly what the LLM is
# instructed to return and what the frontend renders.
StatusLiteral = Literal[
    "Compliant",
    "Partially Implemented",
    "Gap Identified",
]


class ComplianceResult(BaseModel):
    control_area_id: str
    control_area_name: str
    status: StatusLiteral
    summary: str
    gap_detail: Optional[str] = None          # only when status != Compliant
    policy_reference: list[str] = []          # specific policy requirement(s) cited
    injection_detected: bool = False
    error: Optional[str] = None


class AnalyseResponse(BaseModel):
    results: list[ComplianceResult]


# ─── Health ───────────────────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    name: str
    status: Literal["ok", "error"]
    detail: str = ""


class HealthResponse(BaseModel):
    overall: Literal["ok", "degraded", "error"]
    services: list[ServiceStatus]
