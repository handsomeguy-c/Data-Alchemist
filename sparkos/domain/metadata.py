from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field

from sparkos.domain.catalog import ColumnProfile


class TableMetadata(BaseModel):
    name: str
    description: str = ""
    path: str = ""
    format: str = "unknown"
    columns: List[ColumnProfile] = Field(default_factory=list)
    partition_columns: List[str] = Field(default_factory=list)
    upstream_tables: List[str] = Field(default_factory=list)
    downstream_tables: List[str] = Field(default_factory=list)
    estimated_size_bytes: int = 0

    def has_column(self, name: str) -> bool:
        return any(column.name == name for column in self.columns)
