"""
Singleton sentence-transformer embedder.
Uses all-MiniLM-L6-v2 (384-dim, ~80 MB) for fast local embeddings.
"""
from __future__ import annotations
from functools import lru_cache
from sentence_transformers import SentenceTransformer
from config import settings


@lru_cache(maxsize=1)
def get_embedder() -> SentenceTransformer:
    """Load model once and cache for the process lifetime."""
    return SentenceTransformer(
        settings.embedding_model,
        cache_folder = "./models_cache"
    )


def embed_texts(texts: list[str]) -> list[list[float]]:
    """
    Embed a list of strings and return a list of float vectors.
    Batched for efficiency.
    """
    model = get_embedder()
    vectors = model.encode(texts, batch_size=32, show_progress_bar=False)
    return [v.tolist() for v in vectors]


def embed_query(text: str) -> list[float]:
    """Embed a single query string."""
    return embed_texts([text])[0]
