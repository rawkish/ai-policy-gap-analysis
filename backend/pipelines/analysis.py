"""
Analysis pipeline: field list → retrieval → injection check → LLM → results.
Now accepts collection_name to query the correct Weaviate collection.
"""
from __future__ import annotations
import logging
from models.schemas import FieldInput, ComplianceResult
from pipelines.retrieval import sanitize_input, retrieve_policy
from services.llm_client import analyse_compliance

logger = logging.getLogger(__name__)


async def run_analysis(
    fields: list[FieldInput],
    collection_name: str,
) -> list[ComplianceResult]:
    """
    Process each control-area field against the given collection:
    1. Sanitize input
    2. Retrieve policy chunks (with injection guard)
    3. Call LLM
    4. Return structured ComplianceResult
    """
    results: list[ComplianceResult] = []

    for field in fields:
        logger.info("Analysing field '%s' against collection '%s'", field.control_area_name, collection_name)

        
        clean_desc, input_injected = sanitize_input(field.description)

        if input_injected:
            results.append(ComplianceResult(
                control_area_id=field.control_area_id,
                control_area_name=field.control_area_name,
                status="Gap Identified",
                summary="Analysis blocked due to detected prompt injection in input.",
                gap_detail="The input description contained adversarial content and was blocked for security reasons.",
                injection_detected=True,
            ))
            continue

        query = f"{field.control_area_name}: {clean_desc}"

        try:
            chunks, retrieval_injected = retrieve_policy(
                query,
                collection_name=collection_name,
            )
        except Exception as exc:
            logger.exception("Retrieval failed: %s", exc)
            results.append(ComplianceResult(
                control_area_id=field.control_area_id,
                control_area_name=field.control_area_name,
                status="Gap Identified",
                summary="Policy retrieval failed — could not assess this control area.",
                error=str(exc),
            ))
            continue

        if retrieval_injected:
            results.append(ComplianceResult(
                control_area_id=field.control_area_id,
                control_area_name=field.control_area_name,
                status="Gap Identified",
                summary="Analysis blocked — prompt injection detected in retrieved policy data.",
                gap_detail="The retrieval pipeline flagged a security anomaly. This field was not sent to the AI for analysis.",
                injection_detected=True,
            ))
            continue

        if not chunks:
            results.append(ComplianceResult(
                control_area_id=field.control_area_id,
                control_area_name=field.control_area_name,
                status="Gap Identified",
                summary="No relevant policy content found. Please upload policy documents before running analysis.",
                gap_detail="No policy chunks were retrieved for this control area. Ingest relevant policy PDFs first.",
                error="No policy chunks retrieved.",
            ))
            continue

        
        try:
            llm_result = analyse_compliance(
                control_area=field.control_area_name,
                description=clean_desc,
                policy_chunks=chunks,
            )
            results.append(ComplianceResult(
                control_area_id=field.control_area_id,
                control_area_name=field.control_area_name,
                **llm_result,
            ))
        except Exception as exc:
            logger.exception("LLM analysis failed: %s", exc)
            results.append(ComplianceResult(
                control_area_id=field.control_area_id,
                control_area_name=field.control_area_name,
                status="Gap Identified",
                summary="AI analysis could not be completed due to a technical error.",
                error=str(exc),
            ))

    return results
