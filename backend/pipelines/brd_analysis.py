"""
BRD Analysis pipeline — end-to-end flow:

  1. Fetch policy chunks with their *stored* control_area (human-verified)
  2. Group policy chunks by control area
  3. Compute centroids from policy clusters (used for BRD classification if needed)
  4. Fetch BRD chunks with their stored control_area
  5. Group BRD chunks by control area
  6. For each control area, send grouped policy + BRD chunks to LLM
  7. Return structured results

Supports sequential (default) or parallel LLM execution.
"""
from __future__ import annotations
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from config import settings
from models.schemas import BrdControlResult, ClassificationDetail
from pipelines.classifier import (
    build_anchors, classify_chunks, ClassifiedChunk,
    compute_policy_centroids, classify_chunks_by_centroids,
)
from services.llm_client import analyse_brd_compliance
from services.weaviate_client import (
    fetch_all_chunks, batch_update_classifications,
)

logger = logging.getLogger(__name__)


CONTROL_AREAS = [
    {
        "id": "authorization",
        "name": "Authorization",
        "placeholder": (
            "Describe how your application controls who can access what. "
            "Include how roles are assigned, how access is reviewed, and how "
            "privileged accounts are managed."
        ),
    },
    {
        "id": "authentication",
        "name": "Authentication",
        "placeholder": (
            "Describe how users and service accounts authenticate. Include "
            "details on MFA, password requirements, SSO, and how default "
            "credentials are handled."
        ),
    },
    {
        "id": "logging_monitoring",
        "name": "Logging & Monitoring",
        "placeholder": (
            "Describe what your application logs, where logs are stored, how "
            "long they are kept, and how they are reviewed."
        ),
    },
    {
        "id": "certification_compliance",
        "name": "Certification & Compliance",
        "placeholder": (
            "Describe what security standards or certifications your application "
            "or organisation complies with, and how compliance is audited and "
            "maintained."
        ),
    },
    {
        "id": "application_patching",
        "name": "Application Patching",
        "placeholder": (
            "Describe your patch management process, including how quickly "
            "different severity patches are applied and how legacy systems are "
            "handled."
        ),
    },
    {
        "id": "system_hardening",
        "name": "System Hardening",
        "placeholder": (
            "Describe how your systems are hardened. Include configuration "
            "baselines used, how unused services are managed, encryption in "
            "transit, and admin interface controls."
        ),
    },
    {
        "id": "session_management",
        "name": "Session Management",
        "placeholder": (
            "Describe how user sessions are managed. Include session "
            "timeout rules, how tokens are protected, and what happens when "
            "a user logs out."
        ),
    },
]


def _get_control_areas() -> list[dict]:
    """Return control area definitions as plain dicts."""
    return CONTROL_AREAS


def _group_chunks_by_control_area(chunks: list[dict]) -> dict[str, list[dict]]:
    """
    Group chunks by their stored control_area property.
    A chunk with control_area="auth,authz" goes into both groups.
    """
    groups: dict[str, list[dict]] = {}
    for chunk in chunks:
        ca_str = chunk.get("control_area", "") or ""
        areas = [x.strip() for x in ca_str.split(",") if x.strip()]
        for area_id in areas:
            groups.setdefault(area_id, []).append(chunk)
    return groups


def classify_brd_with_policy_centroids(
    collection_name: str,
) -> dict:
    """
    Classify BRD chunks using centroids computed from human-verified policy chunks.
    Persists the classification on each BRD chunk.

    Returns {area_id: chunk_count} summary.
    """
    # 1. Fetch policy chunks with vectors + control_area
    policy_raw = fetch_all_chunks(collection_name, doc_type="policy")
    if not policy_raw:
        raise ValueError(f"No policy documents in '{collection_name}'. Upload policy PDFs first.")

    # 2. Group policy chunks by their stored control_area
    policy_by_area = _group_chunks_by_control_area(policy_raw)

    # 3. Compute centroids
    centroids = compute_policy_centroids(policy_by_area)
    if not centroids:
        raise ValueError("No classified policy chunks found. Classify policy chunks first.")

    # 4. Fetch BRD chunks
    brd_raw = fetch_all_chunks(collection_name, doc_type="brd")
    if not brd_raw:
        raise ValueError(f"No BRD documents in '{collection_name}'. Upload a BRD PDF first.")

    # 5. Classify BRD chunks against policy centroids
    area_names = {ca["id"]: ca["name"] for ca in CONTROL_AREAS}
    brd_classified = classify_chunks_by_centroids(brd_raw, centroids, area_names)

    # 6. Build uuid → classification updates
    uuid_to_class: dict[str, dict] = {}
    for area_id, classified_chunks in brd_classified.items():
        for cc in classified_chunks:
            # Find matching raw chunk by text
            for raw in brd_raw:
                if raw["text"] == cc.text and raw.get("source_file") == cc.source_file:
                    uid = raw.get("uuid", "")
                    if uid:
                        if uid not in uuid_to_class:
                            uuid_to_class[uid] = {
                                "uuid": uid,
                                "control_areas": [],
                                "confidence": cc.similarity,
                            }
                        if area_id not in uuid_to_class[uid]["control_areas"]:
                            uuid_to_class[uid]["control_areas"].append(area_id)
                        uuid_to_class[uid]["confidence"] = max(
                            uuid_to_class[uid]["confidence"], cc.similarity
                        )
                    break

    # 7. Persist
    updates = [
        {"uuid": uid, "control_area": ",".join(info["control_areas"]), "confidence": info["confidence"]}
        for uid, info in uuid_to_class.items()
    ]
    if updates:
        batch_update_classifications(collection_name, updates)
        logger.info("Persisted BRD classifications for %d chunks.", len(updates))

    return {area_id: len(chunks) for area_id, chunks in brd_classified.items()}


def _analyse_one_control_area(
    control_area_id: str,
    control_area_name: str,
    policy_chunks: list[dict],
    brd_chunks: list[dict],
) -> BrdControlResult:
    """
    Run LLM analysis for a single control area.
    Handles edge cases (no policy chunks, no BRD chunks).
    """
    policy_count = len(policy_chunks)
    brd_count = len(brd_chunks)

    if policy_count == 0:
        return BrdControlResult(
            control_area_id=control_area_id,
            control_area_name=control_area_name,
            status="Gap Identified",
            summary="No policy content found for this control area.",
            gap_detail="The uploaded policy document does not contain any content classified under this control area.",
            policy_chunk_count=0,
            brd_chunk_count=brd_count,
        )

    if brd_count == 0:
        return BrdControlResult(
            control_area_id=control_area_id,
            control_area_name=control_area_name,
            status="Gap Identified",
            summary="The BRD does not contain any content addressing this control area.",
            gap_detail="The policy defines requirements for this area, but the BRD has no matching specifications.",
            policy_chunk_count=policy_count,
            brd_chunk_count=0,
        )

    try:
        policy_dicts = [
            {"text": c["text"], "source_file": c.get("source_file", ""), "heading": c.get("heading", "")}
            for c in policy_chunks
        ]
        brd_dicts = [
            {"text": c["text"], "source_file": c.get("source_file", ""), "heading": c.get("heading", "")}
            for c in brd_chunks
        ]

        llm_result = analyse_brd_compliance(
            control_area=control_area_name,
            policy_chunks=policy_dicts,
            brd_chunks=brd_dicts,
        )

        return BrdControlResult(
            control_area_id=control_area_id,
            control_area_name=control_area_name,
            policy_chunk_count=policy_count,
            brd_chunk_count=brd_count,
            **llm_result,
        )

    except Exception as exc:
        logger.exception("LLM analysis failed for %s: %s", control_area_name, exc)
        return BrdControlResult(
            control_area_id=control_area_id,
            control_area_name=control_area_name,
            status="Gap Identified",
            summary="AI analysis could not be completed due to a technical error.",
            error=str(exc),
            policy_chunk_count=policy_count,
            brd_chunk_count=brd_count,
        )


async def run_brd_analysis(
    collection_name: str,
    parallel: bool | None = None,
    active_areas: list[str] | None = None,
) -> dict:
    """
    Full BRD analysis pipeline using stored classifications.
    """
    if parallel is None:
        parallel = settings.parallel_analysis

    control_areas = _get_control_areas()
    if active_areas:
        control_areas = [ca for ca in control_areas if ca["id"] in active_areas]

    # Fetch chunks with stored control_area (no re-classification)
    policy_raw = fetch_all_chunks(collection_name, doc_type="policy")
    brd_raw = fetch_all_chunks(collection_name, doc_type="brd")

    if not policy_raw:
        raise ValueError(f"No policy documents in '{collection_name}'. Upload policy PDFs first.")
    if not brd_raw:
        raise ValueError(f"No BRD documents in '{collection_name}'. Upload a BRD PDF first.")

    # Group by stored control_area
    policy_by_area = _group_chunks_by_control_area(policy_raw)
    brd_by_area = _group_chunks_by_control_area(brd_raw)

    # Build classification summary
    classification_summary = []
    for ca in control_areas:
        classification_summary.append(ClassificationDetail(
            control_area_id=ca["id"],
            control_area_name=ca["name"],
            policy_chunk_count=len(policy_by_area.get(ca["id"], [])),
            brd_chunk_count=len(brd_by_area.get(ca["id"], [])),
        ))

    # Run LLM analysis per control area
    results: list[BrdControlResult] = []

    if parallel:
        logger.info("Running LLM analysis in PARALLEL mode for %d control areas…", len(control_areas))
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=min(4, len(control_areas))) as pool:
            futures = [
                loop.run_in_executor(
                    pool,
                    _analyse_one_control_area,
                    ca["id"], ca["name"],
                    policy_by_area.get(ca["id"], []),
                    brd_by_area.get(ca["id"], []),
                )
                for ca in control_areas
            ]
            results = list(await asyncio.gather(*futures))
    else:
        logger.info("Running LLM analysis in SEQUENTIAL mode for %d control areas…", len(control_areas))
        for ca in control_areas:
            logger.info("  Analysing: %s", ca["name"])
            result = _analyse_one_control_area(
                ca["id"], ca["name"],
                policy_by_area.get(ca["id"], []),
                brd_by_area.get(ca["id"], []),
            )
            results.append(result)

    return {
        "results": results,
        "classification": classification_summary,
    }
