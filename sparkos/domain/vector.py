from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, Field


class VectorDocument(BaseModel):
    id: str
    text: str
    embedding: List[float]
    metadata: Dict[str, object] = Field(default_factory=dict)


class VectorWriteResult(BaseModel):
    collection: str
    document_count: int
    embedding_model: str
    dimension: int
    index_path: str
