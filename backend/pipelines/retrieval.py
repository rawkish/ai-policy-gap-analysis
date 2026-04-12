"""
Retrieval pipeline: query → embedding → vector search → injection check.
Now accepts collection_name to target the correct Weaviate collection.
"""
from __future__ import annotations
import logging
import re

from services.embedder import embed_query
from services.weaviate_client import vector_search
from config import settings

logger = logging.getLogger(__name__)

_INJECTION_PATTERNS = [
    r"ignore\s+(?:all\s+)?(?:previous|above|prior)\s+instructions?",
    r"disregard\s+(?:the\s+)?(?:system|above|previous)",
    r"you\s+are\s+now\s+(?:a\s+)?(?:different|new|another)",
    r"reveal\s+(?:your\s+)?(?:system\s+)?(?:prompt|instructions?)",
    r"pretend\s+(?:you\s+)?(?:have\s+no|are\s+)?(?:restrictions?|a\s+)",
    r"<\s*(?:system|assistant|user|human)\s*>",
    r"\[\s*(?:system|assistant|user|human)\s*\]",
    r"###\s*(?:system|assistant|instruction)",
]
_INJECTION_RE = re.compile("|".join(_INJECTION_PATTERNS), re.IGNORECASE)

MAX_DESCRIPTION_LENGTH = 3000

# ─── Layer 2: canary distance threshold ──────────────────────────────────────
# Cosine distance range: 0 = identical, 2 = maximally dissimilar.
# Adversarial inputs that closely mirror the canary text → distance < 0.35.
# Legitimate queries that incidentally surface the canary → distance ≥ 0.50.
# Threshold of 0.40 gives comfortable headroom on both sides.
CANARY_DISTANCE_THRESHOLD: float = 0.40


# ─── Public API ───────────────────────────────────────────────────────────────

def sanitize_input(text: str) -> tuple[str, bool]:
    """
    Sanitize user input. Returns (cleaned_text, injection_detected).
    Only flags inputs that match the tightened adversarial command patterns.
    """
    if len(text) > MAX_DESCRIPTION_LENGTH:
        text = text[:MAX_DESCRIPTION_LENGTH]

    match = _INJECTION_RE.search(text)
    if match:
        logger.warning(
            "Injection pattern matched in user input — pattern: %.120s",
            match.group(0),
        )
        return text, True

    return text, False


def retrieve_policy(
    query: str,
    collection_name: str,
    top_k: int | None = None,
) -> tuple[list[dict], bool]:
    """
    Embed the query, search the specified Weaviate collection, and apply the
    two-layer injection guard.

    Returns:
        (clean_chunks[:top_k], injection_detected)
    """
    if top_k is None:
        top_k = settings.top_k

    # Fetch one extra slot so the canary can appear without displacing results
    query_vector = embed_query(query)
    hits = vector_search(query_vector, collection_name=collection_name, top_k=top_k + 1)

    injection_detected = False
    clean_hits = []

    for hit in hits:
        if hit.get("is_injection_canary"):
            distance = hit.get("distance") or 1.0
            if distance <= CANARY_DISTANCE_THRESHOLD:
                logger.warning(
                    "CANARY CHUNK RETRIEVED (distance=%.3f ≤ threshold=%.2f) "
                    "in collection '%s' — likely prompt injection: %.80s",
                    distance, CANARY_DISTANCE_THRESHOLD, collection_name, query,
                )
                injection_detected = True
            else:
                logger.debug(
                    "Canary surfaced but distance=%.3f > threshold=%.2f — "
                    "treating as benign retrieval noise.",
                    distance, CANARY_DISTANCE_THRESHOLD,
                )
            # Either way don't include the canary chunk in the results
        else:
            clean_hits.append(hit)

    return clean_hits[:top_k], injection_detected
