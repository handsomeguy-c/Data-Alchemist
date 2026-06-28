from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from sparkos.domain.vector import VectorDocument, VectorWriteResult


class LocalJsonlVectorStore:
    def __init__(self, root: Path, embedding_model: str = "unknown"):
        self._root = root
        self._embedding_model = embedding_model
        self._root.mkdir(parents=True, exist_ok=True)

    def upsert(
        self,
        collection: str,
        documents: Iterable[VectorDocument],
    ) -> VectorWriteResult:
        path = self._root / f"{collection}.jsonl"
        count = 0
        dimension = 0
        with path.open("w", encoding="utf-8") as file:
            for document in documents:
                file.write(document.model_dump_json() + "\n")
                count += 1
                dimension = len(document.embedding)
        return VectorWriteResult(
            collection=collection,
            document_count=count,
            embedding_model=self._embedding_model,
            dimension=dimension,
            index_path=str(path),
        )
