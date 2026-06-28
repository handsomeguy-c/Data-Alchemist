from __future__ import annotations

from typing import List

from sparkos.domain.catalog import DatasetProfile
from sparkos.domain.metadata import TableMetadata


class ConfigMetadataProvider:
    def __init__(self, datasets: List[DatasetProfile]):
        self._datasets = datasets

    def list_tables(self) -> List[TableMetadata]:
        return [
            TableMetadata(
                name=dataset.name,
                description=dataset.description,
                path=dataset.path,
                format=dataset.format,
                columns=dataset.columns,
                partition_columns=self._partition_columns(dataset),
            )
            for dataset in self._datasets
        ]

    def _partition_columns(self, dataset: DatasetProfile) -> list[str]:
        return [
            column.name
            for column in dataset.columns
            if column.name in {"dt", "date", "biz_date"}
            or column.semantic_type == "timestamp"
        ]
