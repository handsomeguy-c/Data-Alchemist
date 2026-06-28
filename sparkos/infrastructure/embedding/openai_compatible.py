from __future__ import annotations

import json
import urllib.error
import urllib.request

from sparkos.infrastructure.settings import EmbeddingSettings


class OpenAICompatibleEmbedding:
    def __init__(self, settings: EmbeddingSettings):
        self._settings = settings
        self.model_name = settings.model
        self.dimension = settings.dimension

    def embed(self, text: str) -> list[float]:
        request = urllib.request.Request(
            self._settings.base_url or "",
            data=json.dumps({"model": self.model_name, "input": text}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._api_key()}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Embedding request failed: {exc.code} {detail}") from exc

        vector = payload["data"][0]["embedding"]
        self.dimension = len(vector)
        return [float(value) for value in vector]

    def _api_key(self) -> str:
        if self._settings.api_key:
            return self._settings.api_key
        raise RuntimeError("Missing embedding API key. Fill embedding-model.api-key.")
