from __future__ import annotations

from typing import Iterable, Protocol

from sparkos.domain.vector import VectorDocument, VectorWriteResult


class EmbeddingPort(Protocol):
    @property
    def model_name(self) -> str:
        """Embedding model name."""

    @property
    def dimension(self) -> int:
        """Embedding dimension."""

    def embed(self, text: str) -> list[float]:
        """Embed text."""


class VectorStorePort(Protocol):
    def upsert(
        self,
        collection: str,
        documents: Iterable[VectorDocument],
    ) -> VectorWriteResult:
        """Write vector documents."""


class VectorKbRuntime:
    def __init__(
        self,
        embedding: EmbeddingPort,
        vector_store: VectorStorePort,
        collection: str = "agi_gilgamesh_kb",
    ):
        self._embedding = embedding
        self._vector_store = vector_store
        self._collection = collection

    def build(self, chunks: Iterable[dict[str, object]]) -> VectorWriteResult:
        documents = []
        for chunk in chunks:
            text = str(chunk.get("text", ""))
            if not text.strip():
                continue
            metadata = dict(chunk.get("metadata", {}) or {})
            metadata["source_file"] = chunk.get("source_file", "")
            metadata["chunk_id"] = chunk.get("chunk_id", "")
            documents.append(
                VectorDocument(
                    id=str(chunk.get("id")),
                    text=text,
                    embedding=self._embedding.embed(text),
                    metadata=metadata,
                )
            )
        return self._vector_store.upsert(self._collection, documents)
