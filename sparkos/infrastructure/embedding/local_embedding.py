from __future__ import annotations

import hashlib


class LocalHashEmbedding:
    model_name = "local-hash-embedding"

    def __init__(self, dimension: int = 16):
        self.dimension = dimension

    def embed(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        values = []
        for index in range(self.dimension):
            byte = digest[index % len(digest)]
            values.append(round((byte / 255.0) * 2 - 1, 6))
        return values
