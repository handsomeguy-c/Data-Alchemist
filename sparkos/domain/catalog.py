from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class ColumnProfile(BaseModel):
    name: str
    semantic_type: str = "unknown"


class DatasetProfile(BaseModel):
    name: str
    description: str
    path: str
    format: str
    columns: List[ColumnProfile] = Field(default_factory=list)

    def searchable_text(self) -> str:
        column_text = " ".join(column.name for column in self.columns)
        return f"{self.name} {self.description} {column_text}".lower()
