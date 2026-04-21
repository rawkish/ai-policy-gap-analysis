"""
Structural PDF chunker — uses pdfplumber natively for tables, lists, and paragraphs.
"""
from __future__ import annotations
import re
import pdfplumber
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class Chunk:
    text: str
    heading: str
    heading_level: int
    chunk_index: int
    source_file: str


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _sliding_split(text: str, max_chars: int = 1500, stride: int = 500) -> list[str]:
    """Split long text into overlapping windows as a fallback."""
    if len(text) <= max_chars:
        return [text]
    parts = []
    start = 0
    while start < len(text):
        parts.append(text[start:start + max_chars].strip())
        start += stride
    return [p for p in parts if p]


def _compute_thresholds(sizes: list[float]) -> dict:
    if not sizes:
        return {"h1": 18.0, "h2": 15.0, "h3": 13.0}

    sizes_sorted = sorted(s for s in sizes if s and s > 0)
    if not sizes_sorted:
        return {"h1": 18.0, "h2": 15.0, "h3": 13.0}
        
    # Most text in a document is body text, so median is a safe baseline.
    body_size = sizes_sorted[len(sizes_sorted) // 2]

    return {
        "h1": body_size + 4.0,
        "h2": body_size + 2.0,
        "h3": body_size + 0.8
    }


def _classify_level(size: float, is_bold: bool, text: str, thresholds: dict) -> int:
    if size >= thresholds["h1"]: return 1
    if size >= thresholds["h2"]: return 2
    if size >= thresholds["h3"]: return 3
    
    # Strict fallback for sections named like '4. Name' or '4.1 Name' exactly
    if is_bold and re.match(r'^\d+(\.\d+)*\s+[A-Z]', text):
        dots = text.split()[0].count('.')
        return 1 if dots == 1 else 2
        
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
                "is_bold": is_bold
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


def parse_pdf(filepath: str, source_file: str, max_chars: int = 1500) -> list[Chunk]:
    all_elements = []
    all_sizes = []

    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                elements, sizes = _extract_page_elements(page)
                all_elements.append(elements)
                all_sizes.extend(sizes)
    except Exception as exc:
        raise RuntimeError(f"Failed to open PDF '{source_file}': {exc}") from exc

    if not all_elements:
        return []

    thresholds = _compute_thresholds(all_sizes)

    chunks: list[Chunk] = []
    chunk_index = 0
    heading_stack: list[tuple[int, str]] = []

    current_block_type = None
    current_lines = []

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

        # LLM token limit protection
        for segment in _sliding_split(body, max_chars=max_chars):
            chunks.append(Chunk(
                text=f"{heading_path}\n\n{segment}" if current_block_type else segment,
                heading=heading_path,
                heading_level=h_level,
                chunk_index=chunk_index,
                source_file=source_file,
            ))
            chunk_index += 1

        current_lines.clear()
        current_block_type = None

    for page_elems in all_elements:
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
                        current_lines.append("[Table] " + " | ".join(row_parts))
                        
            elif elem["type"] == "line":
                text = elem["text"].strip()
                level = _classify_level(elem["size"], elem.get("is_bold", False), text, thresholds)

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
