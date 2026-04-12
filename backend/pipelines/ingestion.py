"""
Ingestion pipeline: PDF → chunks → embeddings → Weaviate storage.
Now accepts collection_name to target the correct Weaviate collection.
"""
from __future__ import annotations
import logging
import os
import tempfile

from services.pdf_parser import parse_pdf
from services.embedder import embed_texts
from services.weaviate_client import store_chunks

logger = logging.getLogger(__name__)


async def run_ingestion(
    file_bytes: bytes,
    filename: str,
    collection_name: str,
) -> dict:
    """
    Full ingestion pipeline for a single PDF.
    Returns { filename, chunks_stored, status, message }
    """
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        logger.info("Parsing PDF: %s → collection: %s", filename, collection_name)
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
        stored = store_chunks(items, collection_name=collection_name)

        return {
            "filename":      filename,
            "chunks_stored": stored,
            "status":        "success",
            "message":       f"Successfully stored {stored} chunks.",
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
