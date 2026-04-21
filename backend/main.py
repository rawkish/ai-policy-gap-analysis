"""
FastAPI application — main entry point.
"""
from __future__ import annotations
import logging
import sys
import os
import json
from contextlib import asynccontextmanager

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from config import settings
from models.schemas import (
    IngestResponse,
    DocumentsResponse, DocumentInfo,
    CollectionsResponse, CreateCollectionRequest, CreateCollectionResponse,
    ControlAreasResponse, ControlArea,
    AnalyseRequest, AnalyseResponse,
    HealthResponse, ServiceStatus,
    BrdAnalyseRequest, BrdAnalyseResponse, DeleteCollectionResponse,
    ClassifiedChunkResponse, ClassifiedChunksResponse, UpdateClassificationRequest,
)
from pipelines.ingestion import run_ingestion
from pipelines.analysis import run_analysis
from pipelines.brd_analysis import run_brd_analysis, classify_brd_with_policy_centroids
from services.weaviate_client import (
    ensure_collection, create_collection, delete_collection, list_collections,
    list_documents, delete_document, store_chunks,
    fetch_classified_chunks, update_chunk_classification,
    health_check as weaviate_health,
    close_client, sync_canaries,
)
from services.llm_client import health_check as ollama_health

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)



@asynccontextmanager
async def lifespan(app: FastAPI):

    
    logger.info("=== Startup: ensuring default collection '%s' ===", settings.default_collection)
    try:
        newly_created = ensure_collection(settings.default_collection)
    except Exception as exc:
        logger.error("Failed to ensure default collection: %s", exc)
        newly_created = False

    
    
    try:
        report = sync_canaries()
        total_new = sum(report.values())
        if total_new:
            logger.info(
                "=== Canary sync: inserted %d new canary/ies across %d collection(s): %s ===",
                total_new, len(report), report,
            )
        else:
            logger.info("=== Canary sync: all collections already up to date. ===")
    except Exception as exc:
        logger.error("Canary sync failed: %s", exc)

    
    sample_path = os.path.join(settings.assets_dir, settings.sample_pdf_name)
    if newly_created and os.path.isfile(sample_path):
        logger.info("Auto-ingesting sample PDF: %s", sample_path)
        try:
            with open(sample_path, "rb") as f:
                pdf_bytes = f.read()
            result = await run_ingestion(
                pdf_bytes,
                filename=settings.sample_pdf_name,
                collection_name=settings.default_collection,
            )
            logger.info(
                "Sample PDF ingestion: %s (%d chunks)",
                result["status"], result["chunks_stored"],
            )
        except Exception as exc:
            logger.error("Sample PDF auto-ingest failed: %s", exc)
    elif not os.path.isfile(sample_path):
        logger.warning("Sample PDF not found at %s — skipping auto-ingest.", sample_path)

    yield

    close_client()




app = FastAPI(
    title="AI Policy Compliance Assessment",
    version="1.0.0",
    description="Upload policy PDFs, then analyse application descriptions for compliance.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)



CONTROL_AREAS: list[ControlArea] = [
    ControlArea(
        id="authorization",
        name="Authorization",
        label="Authorization",
        placeholder=(
            "Describe how your application controls who can access what. "
            "Include how roles are assigned, how access is reviewed, and how "
            "privileged accounts are managed."
        ),
        category="Security",
    ),
    ControlArea(
        id="authentication",
        name="Authentication",
        label="Authentication",
        placeholder=(
            "Describe how users and service accounts authenticate. Include "
            "details on MFA, password requirements, SSO, and how default "
            "credentials are handled."
        ),
        category="Security",
    ),
    ControlArea(
        id="logging_monitoring",
        name="Logging & Monitoring",
        label="Logging & Monitoring",
        placeholder=(
            "Describe what your application logs, where logs are stored, how "
            "long they are kept, and how they are reviewed."
        ),
        category="Operations",
    ),
    ControlArea(
        id="certification_compliance",
        name="Certification & Compliance",
        label="Certification & Compliance",
        placeholder=(
            "Describe what security standards or certifications your application "
            "or organisation complies with, and how compliance is audited and "
            "maintained."
        ),
        category="Governance",
    ),
    ControlArea(
        id="application_patching",
        name="Application Patching",
        label="Application Patching",
        placeholder=(
            "Describe your patch management process, including how quickly "
            "different severity patches are applied and how legacy systems are "
            "handled."
        ),
        category="Operations",
    ),
    ControlArea(
        id="system_hardening",
        name="System Hardening",
        label="System Hardening",
        placeholder=(
            "Describe how your systems are hardened. Include configuration "
            "baselines used, how unused services are managed, encryption in "
            "transit, and admin interface controls."
        ),
        category="Security",
    ),
    ControlArea(
        id="session_management",
        name="Session Management",
        label="Session Management",
        placeholder=(
            "Describe how user sessions are managed. Include session "
            "timeout rules, how tokens are protected, and what happens when "
            "a user logs out."
        ),
        category="Security",
    ),
]



@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health():
    weaviate_ok, weaviate_detail = weaviate_health()
    ollama_ok, ollama_detail = ollama_health()
    services = [
        ServiceStatus(name="weaviate", status="ok" if weaviate_ok else "error", detail=weaviate_detail),
        ServiceStatus(name="ollama",   status="ok" if ollama_ok   else "error", detail=ollama_detail),
    ]
    overall = "ok" if all(s.status == "ok" for s in services) else "degraded"
    return HealthResponse(overall=overall, services=services)




@app.get("/api/collections", response_model=CollectionsResponse, tags=["Collections"])
async def get_collections():
    """List all existing Weaviate collections."""
    cols = list_collections()
    return CollectionsResponse(collections=cols, default=settings.default_collection)


@app.post("/api/collections", response_model=CreateCollectionResponse, tags=["Collections"])
async def post_create_collection(body: CreateCollectionRequest):
    """Create a new collection (+ injects the canary chunk automatically)."""
    try:
        result = create_collection(body.name)
        return CreateCollectionResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))




@app.get("/api/control-areas", response_model=ControlAreasResponse, tags=["Control Areas"])
async def get_control_areas():
    return ControlAreasResponse(control_areas=CONTROL_AREAS)




@app.post("/api/ingest", response_model=list[IngestResponse], tags=["Ingestion"])
async def ingest_documents(
    files: list[UploadFile] = File(...),
    collection_name: str = Form(...),
    control_areas: str = Form(None),
):
    """Upload PDFs and ingest them into the specified collection."""
    if not files:
        raise HTTPException(status_code=400, detail="No files provided.")
    if not collection_name.strip():
        raise HTTPException(status_code=400, detail="collection_name is required.")

    active_areas = CONTROL_AREAS
    if control_areas:
        try:
            parsed_areas = json.loads(control_areas)
            active_areas = [ControlArea(**a) for a in parsed_areas]
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid control_areas JSON: {exc}")

    results = []
    for upload in files:
        if not (upload.filename or "").lower().endswith(".pdf"):
            results.append(IngestResponse(
                filename=upload.filename or "unknown",
                chunks_stored=0,
                status="error",
                message="Only PDF files are supported.",
            ))
            continue

        file_bytes = await upload.read()
        result = await run_ingestion(
            file_bytes,
            filename=upload.filename,
            collection_name=collection_name.strip(),
            active_areas=active_areas,
        )
        results.append(IngestResponse(**result))

    return results




@app.get("/api/documents", response_model=DocumentsResponse, tags=["Documents"])
async def get_documents(
    collection_name: str = Query(...),
    doc_type: str = Query(default=None, description="Filter by doc_type: 'policy' or 'brd'"),
):
    """List documents stored in a specific collection, optionally filtered by doc_type."""
    docs = list_documents(collection_name, doc_type=doc_type)
    return DocumentsResponse(documents=[DocumentInfo(**d) for d in docs])


@app.delete("/api/documents/{filename:path}", tags=["Documents"])
async def remove_document(filename: str, collection_name: str = Query(...)):
    """Delete all chunks for a document from a specific collection."""
    deleted = delete_document(filename, collection_name=collection_name)
    return {"filename": filename, "chunks_deleted": deleted}




@app.post("/api/analyse", response_model=AnalyseResponse, tags=["Analysis"])
async def analyse(request: AnalyseRequest):
    """Run compliance analysis against the specified collection."""
    if not request.fields:
        raise HTTPException(status_code=400, detail="No fields provided.")
    results = await run_analysis(request.fields, collection_name=request.collection_name)
    return AnalyseResponse(results=results)


# ── Collection Delete ─────────────────────────────────────────────────────────

@app.delete("/api/collections/{collection_name}", response_model=DeleteCollectionResponse, tags=["Collections"])
async def remove_collection(collection_name: str):
    """Delete an entire Weaviate collection and all its data."""
    try:
        delete_collection(collection_name)
        return DeleteCollectionResponse(name=collection_name, deleted=True)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ── BRD Ingestion ─────────────────────────────────────────────────────────────

@app.post("/api/ingest-brd", response_model=list[IngestResponse], tags=["BRD"])
async def ingest_brd(
    files: list[UploadFile] = File(...),
    collection_name: str = Form(...),
):
    """Upload BRD PDFs and ingest them into the specified collection with doc_type='brd'."""
    if not files:
        raise HTTPException(status_code=400, detail="No files provided.")
    if not collection_name.strip():
        raise HTTPException(status_code=400, detail="collection_name is required.")

    results = []
    for upload in files:
        if not (upload.filename or "").lower().endswith(".pdf"):
            results.append(IngestResponse(
                filename=upload.filename or "unknown",
                chunks_stored=0,
                status="error",
                message="Only PDF files are supported.",
            ))
            continue

        file_bytes = await upload.read()
        result = await run_ingestion(
            file_bytes,
            filename=upload.filename,
            collection_name=collection_name.strip(),
            doc_type="brd",
        )
        results.append(IngestResponse(**result))

    # After BRD ingestion, classify using policy centroids
    try:
        classify_brd_with_policy_centroids(collection_name.strip())
    except ValueError as exc:
        logger.warning("BRD centroid classification skipped: %s", exc)

    return results


# ── BRD Analysis ──────────────────────────────────────────────────────────────

@app.post("/api/analyse-brd", response_model=BrdAnalyseResponse, tags=["BRD"])
async def analyse_brd(request: BrdAnalyseRequest):
    """
    Run BRD-based compliance analysis.
    Compares BRD chunks against policy chunks in the specified collection.
    """
    try:
        result = await run_brd_analysis(
            collection_name=request.collection_name,
            parallel=request.parallel,
            active_areas=request.active_areas,
        )
        return BrdAnalyseResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("BRD analysis failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Debug Classification ──────────────────────────────────────────────────────

@app.get("/api/debug-classify", tags=["BRD"])
async def debug_classify(text: str = Query(..., description="Text to classify")):
    """Classify a single piece of text and return per-control-area similarity scores."""
    from pipelines.classifier import build_anchors, debug_classify_chunk
    from pipelines.brd_analysis import _get_control_areas
    anchors = build_anchors(_get_control_areas())
    scores = debug_classify_chunk(text, anchors)
    return {"text_preview": text[:120], "scores": scores}


# ── Classified Chunks (human-in-the-loop) ───────────────────────────────────────

@app.get("/api/classified-chunks", response_model=ClassifiedChunksResponse, tags=["Classification"])
async def get_classified_chunks(
    collection_name: str = Query(...),
    doc_type: str = Query(default=None, description="Filter by 'policy' or 'brd'"),
):
    """Fetch all chunks with their classification info for manual review."""
    chunks = fetch_classified_chunks(collection_name, doc_type=doc_type)
    # Build summary
    summary: dict[str, int] = {}
    for c in chunks:
        for area in c["control_areas"]:
            summary[area] = summary.get(area, 0) + 1
    return ClassifiedChunksResponse(
        chunks=[ClassifiedChunkResponse(**c) for c in chunks],
        control_area_summary=summary,
    )


@app.patch("/api/chunks/{chunk_uuid}/classification", tags=["Classification"])
async def update_classification(
    chunk_uuid: str,
    request: UpdateClassificationRequest,
):
    """Update the control area classification of a single chunk (human correction)."""
    try:
        update_chunk_classification(
            collection_name=request.collection_name,
            chunk_uuid=chunk_uuid,
            control_areas=request.control_areas,
        )
        return {"uuid": chunk_uuid, "control_areas": request.control_areas, "confidence": 1.0}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))



FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")




if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
