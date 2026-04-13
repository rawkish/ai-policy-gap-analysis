"""
Application configuration — reads from environment / .env file.
"""
from pydantic_settings import BaseSettings
from pydantic import Field
import os


class Settings(BaseSettings):
    
    weaviate_url: str = Field(default="http://localhost:8080", env="WEAVIATE_URL")
    
    default_collection: str = Field(default="SecurityPolicy", env="DEFAULT_COLLECTION")

    
    ollama_url: str = Field(default="http://localhost:11434", env="OLLAMA_URL")
    ollama_model: str = Field(default="llama3.2:3b", env="OLLAMA_MODEL")

    
    embedding_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2",
        env="EMBEDDING_MODEL"
    )
    embedding_dim: int = Field(default=384, env="EMBEDDING_DIM")

    
    max_chunk_tokens: int = Field(default=512, env="MAX_CHUNK_TOKENS")
    chunk_stride_tokens: int = Field(default=128, env="CHUNK_STRIDE_TOKENS")

    
    top_k: int = Field(default=5, env="TOP_K")

    
    canary_marker: str = Field(
        default="INJECTION_CANARY_8f3a9b2c-1d4e-47f6-a8b9-0c1d2e3f4a5b",
        env="CANARY_MARKER"
    )

    
    assets_dir: str = Field(
        default=os.path.join(os.path.dirname(__file__), "assets"),
        env="ASSETS_DIR"
    )
    sample_pdf_name: str = Field(
        default="security-control-policy.pdf",
        env="SAMPLE_PDF_NAME"
    )

    
    allowed_origins: list[str] = ["*"]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
