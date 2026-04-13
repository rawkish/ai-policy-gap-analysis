"""
FastAPI application — main entry point.
"""
from __future__ import annotations
import logging
import sys
import os
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
)
from pipelines.ingestion import run_ingestion
from pipelines.analysis import run_analysis
from services.weaviate_client import (
    ensure_collection, create_collection, list_collections,
    list_documents, delete_document,
    health_check as weaviate_health,
    close_client,
)
from services.llm_client import health_check as ollama_health

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)



@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup:
      1. Ensure the default collection exists (creates it + canary if new).
      2. Auto-ingest the sample PDF into the default collection if brand new.
    Shutdown:
      Close the Weaviate client connection.
    """
    
    logger.info("=== Startup: ensuring default collection '%s' ===", settings.default_collection)
    try:
        newly_created = ensure_collection(settings.default_collection)
    except Exception as exc:
        logger.error("Failed to ensure default collection: %s", exc)
        newly_created = False

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
):
    """Upload PDFs and ingest them into the specified collection."""
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
        )
        results.append(IngestResponse(**result))

    return results




@app.get("/api/documents", response_model=DocumentsResponse, tags=["Documents"])
async def get_documents(collection_name: str = Query(...)):
    """List documents stored in a specific collection."""
    docs = list_documents(collection_name)
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







FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")




if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
