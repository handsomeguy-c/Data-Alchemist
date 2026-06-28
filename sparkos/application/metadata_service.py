from __future__ import annotations

from typing import Iterable, List, Protocol

from sparkos.domain.catalog import DatasetProfile
from sparkos.domain.metadata import TableMetadata


class MetadataPort(Protocol):
    def list_tables(self) -> List[TableMetadata]:
        """Return known table metadata."""


class MetadataService:
    def __init__(self, ports: Iterable[MetadataPort]):
        self._ports = list(ports)

    def search(self, query: str, limit: int = 5) -> List[TableMetadata]:
        tables = self.list_tables()
        tokens = set(query.lower().replace("，", " ").replace(",", " ").split())
        scored = []
        for table in tables:
            text = self._searchable_text(table)
            score = sum(1 for token in tokens if token in text)
            if score:
                scored.append((score, table))
        if not scored:
            return tables[:limit]
        scored.sort(key=lambda item: item[0], reverse=True)
        return [table for _, table in scored[:limit]]

    def enrich_dataset(self, dataset: DatasetProfile) -> TableMetadata:
        for table in self.list_tables():
            if table.name == dataset.name:
                return table
        return TableMetadata(
            name=dataset.name,
            description=dataset.description,
            path=dataset.path,
            format=dataset.format,
            columns=dataset.columns,
            partition_columns=[
                column.name
                for column in dataset.columns
                if column.name in {"dt", "date", "biz_date"}
                or column.semantic_type == "timestamp"
            ],
        )

    def list_tables(self) -> List[TableMetadata]:
        tables = []
        for port in self._ports:
            tables.extend(port.list_tables())
        return tables

    def _searchable_text(self, table: TableMetadata) -> str:
        columns = " ".join(column.name for column in table.columns)
        lineage = " ".join(table.upstream_tables + table.downstream_tables)
        return f"{table.name} {table.description} {columns} {lineage}".lower()
