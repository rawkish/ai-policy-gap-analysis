"""
Pydantic request/response schemas.
"""
from __future__ import annotations
from typing import Literal, Optional
from pydantic import BaseModel, Field




class IngestResponse(BaseModel):
    filename: str
    chunks_stored: int
    status: Literal["success", "error"]
    message: str = ""




class DocumentInfo(BaseModel):
    filename: str
    chunk_count: int
    ingested_at: Optional[str] = None


class DocumentsResponse(BaseModel):
    documents: list[DocumentInfo]




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




class ControlArea(BaseModel):
    id: str
    name: str
    label: str
    placeholder: str
    category: str = "general"


class ControlAreasResponse(BaseModel):
    control_areas: list[ControlArea]




class FieldInput(BaseModel):
    control_area_id: str
    control_area_name: str
    description: str = Field(..., min_length=1, max_length=5000)


class AnalyseRequest(BaseModel):
    fields: list[FieldInput] = Field(..., min_length=1)
    collection_name: str = Field(..., min_length=1)






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
    gap_detail: Optional[str] = None          
    policy_reference: list[str] = []          
    injection_detected: bool = False
    error: Optional[str] = None


class AnalyseResponse(BaseModel):
    results: list[ComplianceResult]




class ServiceStatus(BaseModel):
    name: str
    status: Literal["ok", "error"]
    detail: str = ""


class HealthResponse(BaseModel):
    overall: Literal["ok", "degraded", "error"]
    services: list[ServiceStatus]


# ── BRD Analysis ──────────────────────────────────────────────────────────────

class ClassificationDetail(BaseModel):
    """Per-control-area chunk counts after classification."""
    control_area_id: str
    control_area_name: str
    policy_chunk_count: int = 0
    brd_chunk_count: int = 0


class BrdAnalyseRequest(BaseModel):
    collection_name: str = Field(..., min_length=1)
    parallel: bool = Field(default=False, description="Run LLM calls concurrently if True.")
    active_areas: Optional[list[str]] = Field(default=None, description="List of control area IDs to analyse. If empty, runs all.")


class BrdControlResult(BaseModel):
    """Compliance result for one control area (BRD mode)."""
    control_area_id: str
    control_area_name: str
    status: StatusLiteral
    summary: str
    gap_detail: Optional[str] = None
    policy_references: list[str] = []
    brd_references: list[str] = []
    policy_chunk_count: int = 0
    brd_chunk_count: int = 0
    error: Optional[str] = None


class BrdAnalyseResponse(BaseModel):
    results: list[BrdControlResult]
    classification: list[ClassificationDetail] = []


# ── Collection management ─────────────────────────────────────────────────────

class DeleteCollectionResponse(BaseModel):
    name: str
    deleted: bool


# ── Classified Chunks (human-in-the-loop) ─────────────────────────────────────

class ClassifiedChunkResponse(BaseModel):
    """A single chunk with its classification info, exposed to the frontend."""
    uuid: str
    text: str
    heading: str
    source_file: str
    control_areas: list[str] = []      # e.g. ["authentication", "authorization"]
    confidence: float = 0.0            # 0.0–1.0 (1.0 = human-verified)
    doc_type: str = "policy"


class ClassifiedChunksResponse(BaseModel):
    chunks: list[ClassifiedChunkResponse]
    control_area_summary: dict[str, int] = {}  # {"authentication": 5, ...}


class UpdateClassificationRequest(BaseModel):
    collection_name: str = Field(..., min_length=1)
    control_areas: list[str]           # new assignment, e.g. ["authentication"]
