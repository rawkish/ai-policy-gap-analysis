"""
Control-area classifier — assigns text chunks to one of the 7 control areas
using cosine similarity against anchor embeddings.

Supports:
  - Multiple anchors per control area (soft ensemble via max similarity)
  - Margin-based dual assignment for genuinely ambiguous chunks
  - Minimum-score filtering to discard irrelevant chunks
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from config import settings
from services.embedder import embed_texts

logger = logging.getLogger(__name__)


@dataclass
class ControlAnchor:
    """A control area with its precomputed anchor embeddings (one per phrase)."""
    id: str
    name: str
    phrases: list[str]                        # multiple anchor phrases
    vectors: list[list[float]] = field(default_factory=list, repr=False)


@dataclass
class ClassifiedChunk:
    """A text chunk annotated with its assigned control-area ID(s)."""
    text: str
    source_file: str
    heading: str
    control_area_ids: list[str]
    similarity: float   # max similarity to primary control area


# ── Anchor phrase definitions ─────────────────────────────────────────────────
# Each control area gets 4–6 rich, diverse phrases covering its vocabulary.
# More phrases = better coverage of the conceptual sub-topics.
# Phrases intentionally vary style: formal policy language, technical terms,
# and question-form queries — so at least one phrase aligns with each chunk.

CONTROL_AREA_ANCHORS: list[dict] = [
    {
        "id": "authorization",
        "name": "Authorization",
        "phrases": [
            "Authorization and access control: role-based access control (RBAC), "
            "user permissions, privilege management, least-privilege principle, "
            "access review, entitlement management.",
            "Who is allowed to access which resources and data? How are access rights "
            "granted, modified, and revoked? How are privileged accounts managed?",
            "Access control policy: segregation of duties, need-to-know access, "
            "administrative access controls, access approval workflows.",
            "Authorization controls including user roles, group memberships, permission "
            "levels, access tokens, and resource-level permissions.",
        ],
    },
    {
        "id": "authentication",
        "name": "Authentication",
        "phrases": [
            "Authentication mechanisms: identity verification, username and password, "
            "multi-factor authentication (MFA), single sign-on (SSO), biometric login.",
            "How do users and service accounts prove their identity? What password "
            "policies, MFA requirements, and credential management practices are in place?",
            "Authentication controls: login procedures, credential storage, password "
            "complexity requirements, account lockout, default credential management.",
            "User authentication including federation, OAuth, SAML, OpenID Connect, "
            "certificate-based authentication, and API key management.",
        ],
    },
    {
        "id": "logging_monitoring",
        "name": "Logging & Monitoring",
        "phrases": [
            "Logging and monitoring: audit logs, event logging, log retention, "
            "security information and event management (SIEM), alerting, log review.",
            "What events does the application log? How long are logs retained? "
            "How are logs protected and reviewed? What monitoring alerts exist?",
            "Security monitoring: intrusion detection, anomaly detection, log aggregation, "
            "audit trail, incident alerting, operational monitoring.",
            "Log management policy: log storage location, access to logs, log integrity, "
            "real-time monitoring, dashboards, and security event review procedures.",
        ],
    },
    {
        "id": "certification_compliance",
        "name": "Certification & Compliance",
        "phrases": [
            "Regulatory compliance and security certifications: ISO 27001, SOC 2, "
            "PCI DSS, GDPR, HIPAA, NIST, compliance audits, third-party assessments.",
            "What standards or frameworks does the organisation comply with? "
            "How are compliance audits conducted and compliance status maintained?",
            "Certification and compliance requirements: regulatory obligations, "
            "compliance reporting, audit findings, remediation tracking.",
            "Security framework adherence: risk assessments, compliance posture, "
            "policy alignment with industry standards, regulatory reporting.",
        ],
    },
    {
        "id": "application_patching",
        "name": "Application Patching",
        "phrases": [
            "Patch management and vulnerability remediation: software updates, "
            "security patches, patch deployment timelines, vulnerability scanning.",
            "How are software patches and security updates applied? What is the "
            "patching schedule for critical, high, and medium severity vulnerabilities?",
            "Patch and update policy: patch prioritisation, patch testing, "
            "emergency patching procedures, legacy system patching, end-of-life software.",
            "Vulnerability management including CVE tracking, patch SLAs, "
            "dependency updates, OS patching, and application version control.",
        ],
    },
    {
        "id": "system_hardening",
        "name": "System Hardening",
        "phrases": [
            "System hardening and secure configuration: CIS benchmarks, security baselines, "
            "disabling unnecessary services, firewall configuration, encryption in transit.",
            "How are systems and servers hardened against attack? What configuration "
            "baselines are applied? How are unnecessary ports and services disabled?",
            "Infrastructure hardening: server configuration management, TLS/SSL settings, "
            "network segmentation, admin interface controls, secure defaults.",
            "Hardening controls: operating system hardening, application hardening, "
            "network hardening, cloud security posture management (CSPM), CIS controls.",
        ],
    },
    {
        "id": "session_management",
        "name": "Session Management",
        "phrases": [
            "Session management: session tokens, session timeout, session expiry, "
            "secure cookies, token invalidation on logout, concurrent session controls.",
            "How are user sessions created, maintained, and terminated? What are the "
            "timeout policies? How are session tokens protected from hijacking?",
            "Web session security: HTTP session handling, cookie security flags, "
            "JWT management, session fixation prevention, idle timeout policy.",
            "Session lifecycle management including token storage, session revocation, "
            "re-authentication requirements, and session activity monitoring.",
        ],
    },
]


def build_anchors(control_areas: list[dict] | None = None) -> list[ControlAnchor]:
    """
    Build anchor embeddings for each control area.

    Args:
        control_areas: optional list of dicts (only 'id' is used for lookup).
                       If None, uses the built-in CONTROL_AREA_ANCHORS.

    Returns:
        list of ControlAnchor objects with precomputed vectors for all phrases.
    """
    anchor_defs = []
    
    if control_areas is not None:
        built_in_map = {a["id"]: a for a in CONTROL_AREA_ANCHORS}
        for ca in control_areas:
            # Handle both Pydantic models and plain dicts just in case
            is_dict = isinstance(ca, dict)
            ca_id = ca.get("id") if is_dict else getattr(ca, "id", None)
            ca_name = ca.get("name", "Custom Area") if is_dict else getattr(ca, "name", "Custom Area")
            ca_placeholder = ca.get("placeholder", ca_name) if is_dict else getattr(ca, "placeholder", ca_name)
            
            if not ca_id:
                ca_id = ca_name.lower().replace(" ", "_")
                
            if ca_id in built_in_map:
                anchor_defs.append(built_in_map[ca_id])
            else:
                # Dynamically build anchor using the provided description/placeholder
                anchor_defs.append({
                    "id": ca_id,
                    "name": ca_name,
                    "phrases": [ca_placeholder],
                })
    else:
        anchor_defs = CONTROL_AREA_ANCHORS

    anchors = []
    all_phrases = []
    phrase_counts = []

    for adef in anchor_defs:
        anchors.append(ControlAnchor(
            id=adef["id"],
            name=adef["name"],
            phrases=adef["phrases"],
        ))
        all_phrases.extend(adef["phrases"])
        phrase_counts.append(len(adef["phrases"]))

    # Embed all phrases in one batched call
    all_vectors = embed_texts(all_phrases)

    # Distribute vectors back to anchors
    idx = 0
    for anchor, count in zip(anchors, phrase_counts):
        anchor.vectors = all_vectors[idx: idx + count]
        idx += count

    logger.info(
        "Built %d control-area anchors (%d total phrases).",
        len(anchors), len(all_phrases),
    )
    return anchors


def classify_chunks(
    chunks: list[dict],
    anchors: list[ControlAnchor],
    *,
    margin: float | None = None,
    min_score: float | None = None,
) -> dict[str, list[ClassifiedChunk]]:
    """
    Classify a list of embedded chunks into control areas.

    Uses max-pooled similarity across all phrases of each control area,
    so the best-matching phrase wins (no dilution from off-topic phrases).

    Args:
        chunks: list of dicts with keys 'text', 'source_file', 'heading', 'vector'.
        anchors: precomputed ControlAnchor objects (with .vectors).
        margin: if top-2 area scores differ by less than this, assign to both.
                Defaults to settings.classification_margin (0.05).
        min_score: minimum similarity to classify a chunk at all.
                   Defaults to settings.classification_min_score (0.25).

    Returns:
        dict mapping control_area_id → list of ClassifiedChunk.
    """
    if margin is None:
        margin = settings.classification_margin
    if min_score is None:
        min_score = settings.classification_min_score

    # Pre-normalise all phrase vectors for each anchor
    # anchor_phrase_matrices[i] shape: (num_phrases_i, dim), row-normalised
    anchor_phrase_matrices = []
    for anchor in anchors:
        mat = np.array(anchor.vectors, dtype=np.float32)   # (P, dim)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        mat = mat / np.where(norms == 0, 1.0, norms)
        anchor_phrase_matrices.append(mat)

    result: dict[str, list[ClassifiedChunk]] = {a.id: [] for a in anchors}
    unclassified_count = 0

    for chunk in chunks:
        chunk_vec = np.array(chunk["vector"], dtype=np.float32)
        chunk_norm = np.linalg.norm(chunk_vec)
        if chunk_norm == 0:
            unclassified_count += 1
            continue

        chunk_normed = chunk_vec / chunk_norm   # (dim,)

        # For each anchor, compute max similarity across all its phrases
        # max-pool: use the phrase that is most similar to the chunk
        area_scores = np.array([
            float(np.max(mat @ chunk_normed))   # max over phrases
            for mat in anchor_phrase_matrices
        ])  # shape: (num_anchors,)

        sorted_indices = np.argsort(area_scores)[::-1]
        top_score = float(area_scores[sorted_indices[0]])
        second_score = float(area_scores[sorted_indices[1]]) if len(sorted_indices) > 1 else 0.0

        if top_score < min_score:
            unclassified_count += 1
            logger.debug(
                "Chunk unclassified (top_score=%.3f < min_score=%.2f): %.80s",
                top_score, min_score, chunk.get("text", "")[:80],
            )
            # Map low-score preamble/noise explicitly
            classified = ClassifiedChunk(
                text=chunk["text"],
                source_file=chunk.get("source_file", "unknown"),
                heading=chunk.get("heading", ""),
                control_area_ids=["noise"],
                similarity=top_score,
            )
            if "noise" not in result:
                result["noise"] = []
            result["noise"].append(classified)
            continue

        # Primary assignment
        assigned_ids = [anchors[sorted_indices[0]].id]

        # Dual assignment only if margin is tight AND second area also clears the floor
        if (top_score - second_score) < margin and second_score >= min_score:
            assigned_ids.append(anchors[sorted_indices[1]].id)
            logger.debug(
                "Dual-assigned chunk to %s + %s (scores: %.3f, %.3f)",
                anchors[sorted_indices[0]].name, anchors[sorted_indices[1]].name,
                top_score, second_score,
            )

        classified = ClassifiedChunk(
            text=chunk["text"],
            source_file=chunk.get("source_file", "unknown"),
            heading=chunk.get("heading", ""),
            control_area_ids=assigned_ids,
            similarity=top_score,
        )

        for area_id in assigned_ids:
            result[area_id].append(classified)

    total = len(chunks)
    classified_count = total - unclassified_count
    logger.info(
        "Classification: %d/%d chunks assigned, %d unclassified (min_score=%.2f, margin=%.2f)",
        classified_count, total, unclassified_count, min_score, margin,
    )
    for anchor in anchors:
        count = len(result[anchor.id])
        logger.info("  %-30s %d chunks", anchor.name, count)

    return result


def debug_classify_chunk(
    text: str,
    anchors: list[ControlAnchor],
    top_n: int = 7,
) -> list[dict]:
    """
    Classify a single piece of text and return per-area score breakdown.
    Useful for debugging misclassifications.

    Returns list of {area_id, area_name, max_score, best_phrase} sorted desc by score.
    """
    from services.embedder import embed_query
    chunk_vec = np.array(embed_query(text), dtype=np.float32)
    chunk_norm = np.linalg.norm(chunk_vec)
    if chunk_norm == 0:
        return []
    chunk_normed = chunk_vec / chunk_norm

    results = []
    for anchor in anchors:
        mat = np.array(anchor.vectors, dtype=np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        mat = mat / np.where(norms == 0, 1.0, norms)
        phrase_scores = mat @ chunk_normed
        best_idx = int(np.argmax(phrase_scores))
        results.append({
            "area_id":     anchor.id,
            "area_name":   anchor.name,
            "max_score":   float(phrase_scores[best_idx]),
            "best_phrase": anchor.phrases[best_idx][:80] + "…",
            "all_scores":  [round(float(s), 4) for s in phrase_scores],
        })

    results.sort(key=lambda x: x["max_score"], reverse=True)
    return results[:top_n]


# ── Centroid-based classification (for BRD chunks) ────────────────────────────

def compute_policy_centroids(
    policy_chunks_by_area: dict[str, list[dict]],
) -> dict[str, list[float]]:
    """
    Compute a normalized centroid vector for each control area from
    human-verified policy chunks.

    Args:
        policy_chunks_by_area: {area_id: [{"vector": [...], ...}, ...]}

    Returns:
        {area_id: centroid_vector} — only for areas with at least one chunk.
    """
    centroids = {}
    for area_id, chunks in policy_chunks_by_area.items():
        if area_id == "noise":
            continue
        vectors = [c["vector"] for c in chunks if c.get("vector")]
        if not vectors:
            continue

        # Normalise each vector, then average, then re-normalise
        mat = np.array(vectors, dtype=np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        mat = mat / np.where(norms == 0, 1.0, norms)
        centroid = np.mean(mat, axis=0)
        centroid_norm = np.linalg.norm(centroid)
        if centroid_norm > 0:
            centroid = centroid / centroid_norm
        centroids[area_id] = centroid.tolist()

    logger.info("Computed centroids for %d control areas.", len(centroids))
    return centroids


def classify_chunks_by_centroids(
    chunks: list[dict],
    centroids: dict[str, list[float]],
    area_names: dict[str, str] | None = None,
    *,
    min_score: float | None = None,
    margin: float | None = None,
) -> dict[str, list[ClassifiedChunk]]:
    """
    Classify chunks using policy-derived centroids (one vector per area).
    Used for BRD chunk classification.

    Args:
        chunks: list of dicts with 'text', 'source_file', 'heading', 'vector'.
        centroids: {area_id: centroid_vector} from compute_policy_centroids().
        area_names: optional {area_id: "Human Name"} for logging.
        min_score: minimum similarity threshold.
        margin: dual-assignment margin.

    Returns:
        dict mapping area_id → list[ClassifiedChunk].
    """
    if min_score is None:
        min_score = settings.classification_min_score
    if margin is None:
        margin = settings.classification_margin
    if area_names is None:
        area_names = {aid: aid for aid in centroids}

    area_ids = list(centroids.keys())
    if not area_ids:
        logger.warning("No centroids provided — cannot classify.")
        return {}

    # Build centroid matrix
    centroid_matrix = np.array([centroids[aid] for aid in area_ids], dtype=np.float32)
    centroid_norms = np.linalg.norm(centroid_matrix, axis=1, keepdims=True)
    centroid_matrix = centroid_matrix / np.where(centroid_norms == 0, 1.0, centroid_norms)

    result: dict[str, list[ClassifiedChunk]] = {aid: [] for aid in area_ids}
    unclassified = 0

    for chunk in chunks:
        chunk_vec = np.array(chunk["vector"], dtype=np.float32)
        chunk_norm = np.linalg.norm(chunk_vec)
        if chunk_norm == 0:
            unclassified += 1
            continue

        chunk_normed = chunk_vec / chunk_norm
        sims = centroid_matrix @ chunk_normed

        sorted_idx = np.argsort(sims)[::-1]
        top = float(sims[sorted_idx[0]])
        second = float(sims[sorted_idx[1]]) if len(sorted_idx) > 1 else 0.0

        if top < min_score:
            unclassified += 1
            continue

        assigned = [area_ids[sorted_idx[0]]]
        if (top - second) < margin and second >= min_score:
            assigned.append(area_ids[sorted_idx[1]])

        cc = ClassifiedChunk(
            text=chunk["text"],
            source_file=chunk.get("source_file", "unknown"),
            heading=chunk.get("heading", ""),
            control_area_ids=assigned,
            similarity=top,
        )
        for aid in assigned:
            result[aid].append(cc)

    logger.info(
        "Centroid classification: %d/%d assigned, %d unclassified.",
        len(chunks) - unclassified, len(chunks), unclassified,
    )
    return result
