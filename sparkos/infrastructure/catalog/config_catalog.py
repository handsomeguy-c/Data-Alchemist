from __future__ import annotations

from typing import List

from sparkos.domain.catalog import DatasetProfile


class ConfigCatalog:
    def __init__(self, datasets: List[DatasetProfile]):
        self._datasets = datasets

    def search(self, query: str, limit: int = 5) -> List[DatasetProfile]:
        if not self._datasets:
            return []

        tokens = set(query.lower().replace("，", " ").replace(",", " ").split())
        scored = []
        for dataset in self._datasets:
            text = dataset.searchable_text()
            score = sum(1 for token in tokens if token in text)
            if score:
                scored.append((score, dataset))

        if not scored:
            return self._datasets[:limit]

        scored.sort(key=lambda item: item[0], reverse=True)
        return [dataset for _, dataset in scored[:limit]]
