"""
Ingestion pipeline: PDF → chunks → embeddings → Weaviate storage → auto-classification.
Now accepts collection_name to target the correct Weaviate collection.
After storage, auto-classifies chunks into control areas and persists the assignments.
"""
from __future__ import annotations
import logging
import os
import tempfile

from services.pdf_parser import parse_pdf
from services.embedder import embed_texts
from services.weaviate_client import store_chunks, batch_update_classifications

logger = logging.getLogger(__name__)


def _auto_classify_chunks(items_with_uuids: list[dict], active_areas: list = None) -> list[dict]:
    """
    Run the multi-anchor classifier on freshly ingested chunks.
    Returns list of {uuid, control_area, confidence} for batch update.

    Also updates items_with_uuids in-place with classification info.
    """
    from pipelines.classifier import build_anchors, classify_chunks
    from pipelines.brd_analysis import _get_control_areas

    control_areas = active_areas if active_areas else _get_control_areas()
    anchors = build_anchors(control_areas)
    classified = classify_chunks(items_with_uuids, anchors)

    # Build uuid → classification mapping
    uuid_to_class: dict[str, dict] = {}
    for area_id, chunks in classified.items():
        for chunk in chunks:
            # Find the matching item by text (since ClassifiedChunk doesn't have uuid)
            for item in items_with_uuids:
                if item["text"] == chunk.text and item.get("source_file") == chunk.source_file:
                    uid = item.get("uuid", "")
                    if uid:
                        if uid not in uuid_to_class:
                            uuid_to_class[uid] = {
                                "uuid": uid,
                                "control_areas": [],
                                "confidence": chunk.similarity,
                            }
                        if area_id not in uuid_to_class[uid]["control_areas"]:
                            uuid_to_class[uid]["control_areas"].append(area_id)
                        # Keep max similarity
                        uuid_to_class[uid]["confidence"] = max(
                            uuid_to_class[uid]["confidence"], chunk.similarity
                        )
                    break

    # Format for batch_update_classifications
    updates = []
    for uid, info in uuid_to_class.items():
        updates.append({
            "uuid": uid,
            "control_area": ",".join(info["control_areas"]),
            "confidence": info["confidence"],
        })

    return updates


async def run_ingestion(
    file_bytes: bytes,
    filename: str,
    collection_name: str,
    doc_type: str = "policy",
    active_areas: list = None,
) -> dict:
    """
    Full ingestion pipeline for a single PDF.
    Returns { filename, chunks_stored, status, message, classified_chunks }
    doc_type: "policy" (default) or "brd".
    """
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        logger.info("Parsing PDF: %s → collection: %s (doc_type=%s)", filename, collection_name, doc_type)
        chunks = parse_pdf(tmp_path, source_file=filename)

        if not chunks:
            return {
                "filename": filename,
                "chunks_stored": 0,
                "status": "error",
                "message": "No text could be extracted from the PDF.",
            }

        logger.info("Embedding %d chunks …", len(chunks))
        texts = [c.text for c in chunks]
        vectors = embed_texts(texts)

        items = [
            {
                "text":          c.text,
                "source_file":   c.source_file,
                "heading":       c.heading,
                "heading_level": c.heading_level,
                "chunk_index":   c.chunk_index,
                "vector":        v,
            }
            for c, v in zip(chunks, vectors)
        ]

        logger.info("Storing %d chunks in collection '%s' …", len(items), collection_name)
        uuids = store_chunks(items, collection_name=collection_name, doc_type=doc_type)

        # Attach UUIDs to items for classification
        for item, uid in zip(items, uuids):
            item["uuid"] = uid

        # Auto-classify policy chunks using multi-anchor classifier
        # BRD chunks are classified separately via policy centroids
        classified_count = 0
        if doc_type == "policy":
            logger.info("Auto-classifying %d policy chunks …", len(items))
            updates = _auto_classify_chunks(items, active_areas)
            if updates:
                batch_update_classifications(collection_name, updates)
                classified_count = len(updates)
                logger.info("Persisted classifications for %d chunks.", classified_count)

        return {
            "filename":      filename,
            "chunks_stored": len(uuids),
            "status":        "success",
            "message":       f"Successfully stored {len(uuids)} chunks{f' and classified {classified_count}' if classified_count else ''}.",
        }

    except Exception as exc:
        logger.exception("Ingestion failed for %s: %s", filename, exc)
        return {
            "filename":      filename,
            "chunks_stored": 0,
            "status":        "error",
            "message":       str(exc),
        }
    finally:
        os.unlink(tmp_path)
