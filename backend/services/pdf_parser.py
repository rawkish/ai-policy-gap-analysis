"""
Heading-aware PDF chunker — uses pdfplumber only (pure Python, no compilation).

Strategy:
  1. Use pdfplumber to extract text with character-level font sizes.
  2. Classify lines as H1/H2/H3/body based on relative font size thresholds
     computed from the document's own font distribution.
  3. Build a document tree: each heading "owns" the body paragraphs that
     follow until the next heading of equal or higher level.
  4. Emit chunks as (heading_breadcrumb, body_text) pairs.
  5. If body_text exceeds max_chars, split with sliding window.
  6. Fallback: if no structure found, chunk the raw text directly.
"""
from __future__ import annotations
import re
import pdfplumber
from dataclasses import dataclass
from typing import Optional


@dataclass
class Chunk:
    text: str
    heading: str
    heading_level: int        
    chunk_index: int
    source_file: str




def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _sliding_split(text: str, max_chars: int = 2000, stride: int = 500) -> list[str]:
    """Split long text into overlapping windows."""
    if len(text) <= max_chars:
        return [text]
    parts = []
    start = 0
    while start < len(text):
        parts.append(text[start:start + max_chars].strip())
        start += stride
    return [p for p in parts if p]


def _compute_thresholds(sizes: list[float]) -> dict:
    """
    Derive H1/H2/H3 cutoffs from the font size distribution in the document.
    Uses the 90th/75th/60th percentiles of sizes > 10pt.
    """
    eligible = sorted(s for s in sizes if s and s > 10)
    if not eligible:
        return {"h1": 20.0, "h2": 16.0, "h3": 13.0}

    n = len(eligible)
    h1 = eligible[int(n * 0.90)]
    h2 = eligible[int(n * 0.75)]
    h3 = eligible[int(n * 0.60)]

    
    h1 = max(h1, 14.0)
    h2 = min(h2, h1 - 1.0)
    h3 = min(h3, h2 - 1.0)
    return {"h1": h1, "h2": h2, "h3": h3}


def _classify_level(size: float, thresholds: dict) -> int:
    if size >= thresholds["h1"]:
        return 1
    if size >= thresholds["h2"]:
        return 2
    if size >= thresholds["h3"]:
        return 3
    return 0




def parse_pdf(filepath: str, source_file: str, max_chars: int = 2000) -> list[Chunk]:
    """
    Parse a PDF and return a list of structured Chunk objects.
    Falls back to plain text chunking if no structure is detected.
    """
    raw_lines: list[dict] = []   
    all_sizes: list[float] = []

    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                
                page_lines = _extract_lines_with_sizes(page)
                for ln in page_lines:
                    if ln["text"]:
                        raw_lines.append(ln)
                        if ln["size"]:
                            all_sizes.append(ln["size"])
    except Exception as exc:
        raise RuntimeError(f"Failed to open PDF '{source_file}': {exc}") from exc

    if not raw_lines:
        return []

    thresholds = _compute_thresholds(all_sizes)

    
    merged: list[dict] = []   

    for ln in raw_lines:
        level = _classify_level(ln["size"] or 0.0, thresholds)
        if merged and merged[-1]["level"] == 0 and level == 0:
            merged[-1]["text"] += " " + ln["text"]
        else:
            merged.append({"level": level, "text": ln["text"]})

    
    chunks: list[Chunk] = []
    chunk_index = 0
    heading_stack: list[tuple[int, str]] = []
    current_body: list[str] = []

    def flush_body() -> None:
        nonlocal chunk_index
        body = _clean(" ".join(current_body))
        if not body:
            current_body.clear()
            return

        heading_path = " > ".join(h[1] for h in heading_stack) if heading_stack else "Document"
        h_level = heading_stack[-1][0] if heading_stack else 0

        for segment in _sliding_split(body, max_chars=max_chars):
            chunks.append(Chunk(
                text=f"{heading_path}\n\n{segment}",
                heading=heading_path,
                heading_level=h_level,
                chunk_index=chunk_index,
                source_file=source_file,
            ))
            chunk_index += 1
        current_body.clear()

    for block in merged:
        level = block["level"]
        text = block["text"]

        if level == 0:
            if len(text) > 30:   
                current_body.append(text)
        else:
            flush_body()
            while heading_stack and heading_stack[-1][0] >= level:
                heading_stack.pop()
            heading_stack.append((level, text))

    flush_body()

    
    if not chunks:
        full_text = _clean(" ".join(ln["text"] for ln in raw_lines))
        for i, segment in enumerate(_sliding_split(full_text, max_chars=max_chars)):
            chunks.append(Chunk(
                text=segment,
                heading="Document",
                heading_level=0,
                chunk_index=i,
                source_file=source_file,
            ))

    return chunks




def _extract_lines_with_sizes(page) -> list[dict]:
    """
    Extract lines from a pdfplumber page, computing the average font size
    from character-level data.
    Returns list of {"text": str, "size": float | None}.
    """
    lines: list[dict] = []

    try:
        words = page.extract_words(
            x_tolerance=3,
            y_tolerance=3,
            keep_blank_chars=False,
            use_text_flow=False,
            extra_attrs=["size"],
        )
    except Exception:
        
        text = page.extract_text() or ""
        for line in text.splitlines():
            line = _clean(line)
            if line:
                lines.append({"text": line, "size": 12.0})   
        return lines

    if not words:
        return lines

    
    current_y: Optional[float] = None
    current_words: list[dict] = []

    def flush_line():
        if not current_words:
            return
        text = _clean(" ".join(w["text"] for w in current_words))
        sizes = [w.get("size") for w in current_words if w.get("size")]
        avg_size = sum(sizes) / len(sizes) if sizes else None
        if text:
            lines.append({"text": text, "size": avg_size})

    for word in words:
        top = word.get("top", 0)
        if current_y is None or abs(top - current_y) > 3:
            flush_line()
            current_words = [word]
            current_y = top
        else:
            current_words.append(word)

    flush_line()
    return lines
