from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, Field


class GraphEdge(BaseModel):
    src: str
    dst: str
    weight: float = 1.0


class GraphResult(BaseModel):
    algorithm: str
    vertices: List[str] = Field(default_factory=list)
    edges: List[GraphEdge] = Field(default_factory=list)
    metrics: Dict[str, object] = Field(default_factory=dict)
    rows: List[Dict[str, object]] = Field(default_factory=list)
    artifact_path: str = ""
