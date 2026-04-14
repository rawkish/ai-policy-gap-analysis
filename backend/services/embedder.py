"""
Singleton sentence-transformer embedder.
Uses all-MiniLM-L6-v2 (384-dim, ~80 MB) for fast local embeddings.

On first run the model is downloaded from HuggingFace Hub and cached
in ./models_cache.  On subsequent runs (or when offline) the cached
copy is loaded directly without any network requests.
"""
from __future__ import annotations
import os
import logging
from functools import lru_cache
from sentence_transformers import SentenceTransformer
from config import settings

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "models_cache")


@lru_cache(maxsize=1)
def get_embedder() -> SentenceTransformer:
    """
    Load the embedding model, preferring the local cache.

    Strategy:
      1. Try loading with local_files_only=True — works when the model
         has been downloaded before (the common case).  No DNS, no HTTP.
      2. If that fails (first-ever run), download from HuggingFace Hub.
    """
    model_name = settings.embedding_model

    
    try:
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"

        model = SentenceTransformer(
            model_name,
            cache_folder=CACHE_DIR,
            local_files_only=True,
        )
        logger.info("Embedding model loaded from local cache: %s", model_name)
        return model
    except Exception:
        logger.info(
            "Model not found in local cache (%s). Downloading from HuggingFace Hub…",
            CACHE_DIR,
        )

    
    os.environ.pop("HF_HUB_OFFLINE", None)
    os.environ.pop("TRANSFORMERS_OFFLINE", None)

    model = SentenceTransformer(
        model_name,
        cache_folder=CACHE_DIR,
    )
    logger.info("Embedding model downloaded and cached: %s", model_name)

    
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"

    return model


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
