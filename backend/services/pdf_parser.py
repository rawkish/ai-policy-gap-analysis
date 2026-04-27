"""
Structural PDF chunker — uses pdfplumber natively for tables, lists, and paragraphs.
"""
from __future__ import annotations
import re
import pdfplumber
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class Chunk:
    text: str
    heading: str
    heading_level: int
    chunk_index: int
    source_file: str
    page_number: int = 0            
    section_type: str = "body"       


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


_SENTENCE_BOUNDARY = re.compile(
    r'(?<=[.!?;])'   # lookbehind: sentence-ending punctuation
    r'(?:\s+)'        
    r'(?=[A-Z0-9"\'(])'  # lookahead: next sentence starts with uppercase, digit, or quote
)

def _sentence_aware_split(
    text: str,
    max_chars: int = 1500,
    overlap_sentences: int = 1,
) -> list[str]:
    """
    Split long text at sentence boundaries with configurable overlap.
    Falls back to character-based splitting only for extremely long run-on text
    (e.g. OCR output with no punctuation).
    """
    if len(text) <= max_chars:
        return [text]

    sentences = _SENTENCE_BOUNDARY.split(text)
    # Filter out empty fragments
    sentences = [s.strip() for s in sentences if s.strip()]

    if len(sentences) <= 1:
        parts = []
        start = 0
        stride = max_chars // 3
        while start < len(text):
            parts.append(text[start:start + max_chars].strip())
            start += stride
        return [p for p in parts if p]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for sent in sentences:
        sent_len = len(sent)
        if current_len + sent_len + 1 > max_chars and current:
            chunks.append(" ".join(current))
            # Keep last N sentences as overlap for context continuity
            overlap = current[-overlap_sentences:] if overlap_sentences else []
            current = list(overlap)
            current_len = sum(len(s) for s in current) + len(current)
        current.append(sent)
        current_len += sent_len + 1  # +1 for the joining space

    if current:
        chunks.append(" ".join(current))

    return [c for c in chunks if c.strip()]



def _compute_thresholds(sizes: list[float]) -> dict:
    if not sizes:
        return {"h1": 18.0, "h2": 15.0, "h3": 13.0}

    sizes_sorted = sorted(s for s in sizes if s and s > 0)
    if not sizes_sorted:
        return {"h1": 18.0, "h2": 15.0, "h3": 13.0}
        
    body_size = sizes_sorted[len(sizes_sorted) // 2]

    return {
        "h1": body_size + 4.0,
        "h2": body_size + 2.0,
        "h3": body_size + 0.8,
        "body": body_size,
    }


# Font weight keywords beyond just "Bold"
_WEIGHT_KEYWORDS = {"bold", "semibold", "demibold", "black", "heavy", "extrabold", "ultrabold"}

def _classify_level(size: float, is_bold: bool, text: str, thresholds: dict,
                    fontname: str = "") -> int:
    """
    Multi-signal heading classifier.
    Combines font size, weight, ALL CAPS, numbered-section patterns,
    and line length into a composite score, then maps to heading level.
    """
    score = 0

    # Signal 1: Font size relative to body
    if size >= thresholds["h1"]:
        score += 4
    elif size >= thresholds["h2"]:
        score += 3
    elif size >= thresholds["h3"]:
        score += 2

    # Signal 2: Font weight (expanded keyword detection)
    fn_lower = fontname.lower() if fontname else ""
    if is_bold or any(kw in fn_lower for kw in _WEIGHT_KEYWORDS):
        score += 1

    # Signal 3: ALL CAPS (strong heading indicator for short lines)
    stripped = text.strip()
    words = stripped.split()
    if (stripped == stripped.upper()
            and len(words) <= 8
            and len(stripped) > 3
            and stripped.replace(" ", "").isalpha()):
        score += 2

    # Signal 4: Numbered section pattern (e.g. "4. Name" or "4.1.2 Name")
    if re.match(r'^\d+(\.\d+)*\.?\s+[A-Z]', stripped):
        dots = stripped.split()[0].count('.')
        score += 2 if dots <= 1 else 1

    # Signal 5: Short line (headings are rarely full paragraphs)
    if len(stripped) < 80:
        score += 1

    # Map composite score → heading level
    if score >= 6:
        return 1
    if score >= 4:
        return 2
    if score >= 3:
        return 3
    return 0


def _extract_page_elements(page) -> tuple[list[dict], list[float]]:
    """
    Extract tables, then text outside tables, returning elements sorted spatially (top-to-bottom).
    """
    elements = []
    sizes = []

    # 1. Native table extraction
    tables = page.find_tables()
    table_bboxes = []
    for t in tables:
        bbox = t.bbox  # (x0, top, x1, bottom)
        table_bboxes.append(bbox)
        # Extract row data
        data = t.extract()
        if data:
            elements.append({
                "type": "table",
                "top": bbox[1],
                "bottom": bbox[3],
                "data": data,
                "size": 12.0  # fallback size to avoid crashes
            })

    # 2. Filter words outside tables to prevent duplication
    def is_outside(obj):
        x0, top, x1, bottom = obj.get("x0"), obj.get("top"), obj.get("x1"), obj.get("bottom")
        if x0 is None or top is None or x1 is None or bottom is None:
            return True
        for bx0, btop, bx1, bbottom in table_bboxes:
            # Overlap check
            if x0 < bx1 and x1 > bx0 and top < bbottom and bottom > btop:
                return False
        return True

    safe_page = page.filter(is_outside)
    try:
        words = safe_page.extract_words(
            x_tolerance=3,
            y_tolerance=3,
            keep_blank_chars=False,
            use_text_flow=False,
            extra_attrs=["size", "fontname"]
        )
    except Exception:
        words = []

    if not words:
        elements.sort(key=lambda e: e["top"])
        return elements, sizes

    # Sort top-to-bottom
    words.sort(key=lambda w: (w.get("top", 0), w.get("x0", 0)))
    
    current_y = None
    current_words = []

    def flush_line():
        if not current_words:
            return
        text = _clean(" ".join(w["text"] for w in current_words))
        wsizes = [w.get("size") for w in current_words if w.get("size")]
        avg_size = sum(wsizes) / len(wsizes) if wsizes else 12.0
        fontnames = [str(w.get("fontname", "")) for w in current_words if w.get("fontname")]
        dominant_font = max(set(fontnames), key=fontnames.count) if fontnames else ""
        is_bold = any("Bold" in str(w.get("fontname", "")) for w in current_words)
        
        top = min(w["top"] for w in current_words)
        bottom = max(w["bottom"] for w in current_words)

        if text:
            elements.append({
                "type": "line",
                "text": text,
                "top": top,
                "bottom": bottom,
                "size": avg_size,
                "is_bold": is_bold,
                "fontname": dominant_font,   # NEW: passed to heading classifier
            })
            sizes.append(avg_size)

    for word in words:
        top = word.get("top", 0)
        # Group horizontally proximate words within ~3 points vertically
        if current_y is None or abs(top - current_y) > 3:
            flush_line()
            current_words = [word]
            current_y = top
        else:
            current_words.append(word)

    flush_line()
    
    # Sort all elements (tables + lines) spatially
    elements.sort(key=lambda e: e["top"])
    return elements, sizes



def _merge_cross_page_paragraphs(all_elements: list[list[dict]]) -> list[list[dict]]:
    """
    Merge the trailing text of page N with the leading text of page N+1
    when the last line on page N does not end with sentence-terminal punctuation.
    This fixes paragraph fragmentation at page boundaries.
    """
    for i in range(len(all_elements) - 1):
        if not all_elements[i] or not all_elements[i + 1]:
            continue

        last_elem = all_elements[i][-1]
        first_elem = all_elements[i + 1][0]

        # Only merge line→line (not tables or headings)
        if last_elem["type"] != "line" or first_elem["type"] != "line":
            continue

        last_text = last_elem["text"].rstrip()
        first_text = first_elem["text"].lstrip()

        # Heuristic: if the last line doesn't end with terminal punctuation
        # AND the next line starts with a lowercase letter (continuation),
        # merge them.
        ends_mid_sentence = not re.search(r'[.!?:;]\s*$', last_text)
        starts_continuation = bool(first_text) and (
            first_text[0].islower()
            or first_text[0] in ',(;—–-'
        )

        if ends_mid_sentence and starts_continuation:
            last_elem["text"] = last_text + " " + first_text
            all_elements[i + 1].pop(0)
            logger.debug(
                "Cross-page merge: page %d tail + page %d head → '%s…'",
                i + 1, i + 2, last_elem["text"][:80],
            )

    return all_elements



def parse_pdf(filepath: str, source_file: str, max_chars: int = 1500) -> list[Chunk]:
    all_elements: list[list[dict]] = []
    all_sizes: list[float] = []
    page_count = 0

    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                page_count += 1
                elements, sizes = _extract_page_elements(page)
                all_elements.append(elements)
                all_sizes.extend(sizes)
    except Exception as exc:
        raise RuntimeError(f"Failed to open PDF '{source_file}': {exc}") from exc

    if not all_elements:
        return []

    all_elements = _merge_cross_page_paragraphs(all_elements)

    thresholds = _compute_thresholds(all_sizes)

    chunks: list[Chunk] = []
    chunk_index = 0
    heading_stack: list[tuple[int, str]] = []

    current_block_type: str | None = None
    current_lines: list[str] = []
    current_page: int = 1  # track which page we're on

    def flush_block():
        nonlocal chunk_index, current_block_type, current_lines

        if not current_lines:
            return

        if current_block_type == "list":
            body = "\n".join(current_lines)
        else:
            body = " ".join(current_lines)

        body = body.strip()
        if not body:
            current_lines.clear()
            current_block_type = None
            return

        heading_path = " > ".join(h[1] for h in heading_stack) if heading_stack else "Document"
        h_level = heading_stack[-1][0] if heading_stack else 0

        # Determine section_type
        section_type = current_block_type if current_block_type else "body"

        context_prefix = f"[{source_file} | Page {current_page} | {heading_path}]"

        for segment in _sentence_aware_split(body, max_chars=max_chars):
            enriched_text = f"{context_prefix}\n{heading_path}\n\n{segment}"
            chunks.append(Chunk(
                text=enriched_text,
                heading=heading_path,
                heading_level=h_level,
                chunk_index=chunk_index,
                source_file=source_file,
                page_number=current_page,
                section_type=section_type,
            ))
            chunk_index += 1

        current_lines.clear()
        current_block_type = None

    for page_idx, page_elems in enumerate(all_elements):
        current_page = page_idx + 1  # 1-indexed page number

        for elem in page_elems:
            if elem["type"] == "table":
                data = elem["data"]
                if not data:
                    continue

                headers = [_clean(h) for h in data[0]] if data[0] else []
                headers = [h if h else f"Col{i+1}" for i, h in enumerate(headers)]

                rows_to_process = data[1:] if len(data) > 1 else data

                for row in rows_to_process:
                    if not any(_clean(c) for c in row if c):
                        continue

                    row_parts = []
                    for h, v in zip(headers, row):
                        val = _clean(v)
                        if val:
                            row_parts.append(f"{h}: {val}")

                    if row_parts:
                        # Flush any pending text block first
                        flush_block()
                        # Each table row becomes its own chunk for a focused embedding
                        current_block_type = "table"
                        current_lines.append("[Table] " + " | ".join(row_parts))
                        flush_block()
                        
            elif elem["type"] == "line":
                text = elem["text"].strip()
                level = _classify_level(
                    elem["size"],
                    elem.get("is_bold", False),
                    text,
                    thresholds,
                    fontname=elem.get("fontname", ""),
                )

                if level > 0:
                    flush_block()
                    while heading_stack and heading_stack[-1][0] >= level:
                        heading_stack.pop()
                    heading_stack.append((level, text))
                else:
                    current_lines.append(text)
            
    flush_block()
    
    logger.info("PDF %s parsed into %d structured chunks.", source_file, len(chunks))
    return chunks
