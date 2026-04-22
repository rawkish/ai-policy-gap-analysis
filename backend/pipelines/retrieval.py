"""
Retrieval pipeline: query → embedding → vector search → injection check.
Now accepts collection_name to target the correct Weaviate collection.
"""
from __future__ import annotations
import logging
import re

from services.embedder import embed_query
from services.weaviate_client import vector_search, canary_search
from config import settings

logger = logging.getLogger(__name__)

_INJECTION_PATTERNS = [
    # Prompt Injections
    r"ignore\s+(?:all\s+)?(?:previous|above|prior)\s+instructions?",
    r"disregard\s+(?:the\s+)?(?:system|above|previous)",
    r"you\s+are\s+now\s+(?:a\s+)?(?:different|new|another)",
    r"reveal\s+(?:your\s+)?(?:system\s+)?(?:prompt|instructions?)",
    r"pretend\s+(?:you\s+)?(?:have\s+no|are\s+)?(?:restrictions?|a\s+)",
    r"<\s*(?:system|assistant|user|human)\s*>",
    r"\[\s*(?:system|assistant|user|human)\s*\]",
    r"###\s*(?:system|assistant|instruction)",
    # XSS (Cross-Site Scripting) Injections
    r"<\s*script\b[^>]*>",
    r"javascript\s*:",
    r"\bon(?:error|load|mouseover|click|focus|blur|keydown|keyup)\s*=",
    r"<\s*(?:iframe|object|embed|applet|svg|math|body)\b",
    # Command / OS Injections
    r"(?:;|\||&&|\|\||`|\$\()\s*(?:ls|cat|whoami|id|pwd|echo|bash|sh|ping|curl|wget|nc|awk|sed|grep|net|type)\b",
    r"/(?:etc|bin|usr|var|tmp|dev)/(?:passwd|shadow|sh|bash|zsh|null)",
    r"(?:cmd\.exe|powershell(\.exe)?|wscript\.exe|cscript\.exe)",
    # SQL Injections
    r"(?:UNION\s+ALL\s+SELECT|UNION\s+SELECT|SELECT\s+.*?\s+FROM|INSERT\s+INTO|UPDATE\s+.*?\s+SET|DELETE\s+FROM|DROP\s+(?:TABLE|DATABASE|INDEX|VIEW)|TRUNCATE\s+TABLE|EXEC(?:UTE)?\s+xp_)",
    r"(?:'|\"|`)\s*(?:OR|AND)\s*(?:'|\"|`|-|\d+)\s*=\s*(?:'|\"|`|-|\d+)",
]
_INJECTION_RE = re.compile("|".join(_INJECTION_PATTERNS), re.IGNORECASE)

MAX_DESCRIPTION_LENGTH = 3000






CANARY_DISTANCE_THRESHOLD: float = 0.50




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


def check_canary_proximity(
    raw_text: str,
    collection_name: str,
) -> tuple[bool, list[str]]:
    """
    Embed the RAW user description (NO control-area prefix) and check it
    against canary chunks only.  This is the primary injection guard —
    it prevents control area names from diluting adversarial signal.

    Uses a filtered vector search that only examines canary vectors,
    making it very cheap (~8 comparisons per call).

    Returns:
        (injection_detected, list_of_triggered_strategy_names)
    """
    query_vector = embed_query(raw_text)
    canary_hits = canary_search(query_vector, collection_name)

    triggered: list[str] = []
    for hit in canary_hits:
        distance = hit.get("distance") or 1.0
        strategy = hit.get("injection_marker") or "unknown"
        if distance <= CANARY_DISTANCE_THRESHOLD:
            triggered.append(f"{strategy} (d={distance:.3f})")
        else:
            logger.debug(
                "Canary [%s] distance=%.3f > threshold=%.2f — benign.",
                strategy, distance, CANARY_DISTANCE_THRESHOLD,
            )

    if triggered:
        logger.warning(
            "CANARY PROXIMITY CHECK TRIGGERED in '%s' — %d strateg%s: [%s]. "
            "Raw input: %.120s",
            collection_name,
            len(triggered),
            "y" if len(triggered) == 1 else "ies",
            ", ".join(triggered),
            raw_text,
        )

    return bool(triggered), triggered


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

    
    
    from services.weaviate_client import CANARY_REGISTRY
    canary_count = len(CANARY_REGISTRY)

    query_vector = embed_query(query)
    hits = vector_search(query_vector, collection_name=collection_name, top_k=top_k + canary_count)

    injection_detected = False
    triggered_strategies: list[str] = []
    clean_hits = []

    for hit in hits:
        if hit.get("is_injection_canary"):
            distance = hit.get("distance") or 1.0
            strategy = hit.get("injection_marker") or "unknown"
            if distance <= CANARY_DISTANCE_THRESHOLD:
                triggered_strategies.append(strategy)
                injection_detected = True
            else:
                logger.debug(
                    "Canary [%s] surfaced but distance=%.3f > threshold=%.2f — benign noise.",
                    strategy, distance, CANARY_DISTANCE_THRESHOLD,
                )
            
        else:
            clean_hits.append(hit)

    if triggered_strategies:
        logger.warning(
            "CANARY TRIGGERED in '%s' — %d strateg%s matched (distance ≤ %.2f): [%s]. "
            "Query: %.100s",
            collection_name,
            len(triggered_strategies),
            "y" if len(triggered_strategies) == 1 else "ies",
            CANARY_DISTANCE_THRESHOLD,
            ", ".join(triggered_strategies),
            query,
        )

    return clean_hits[:top_k], injection_detected
