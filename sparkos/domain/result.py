from __future__ import annotations

from enum import Enum
from typing import Dict, List

from pydantic import BaseModel, Field


class ExecutionState(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ResultArtifact(BaseModel):
    title: str
    summary: str
    rows_preview: List[Dict[str, object]] = Field(default_factory=list)
    metrics: Dict[str, object] = Field(default_factory=dict)
    evidence: List[str] = Field(default_factory=list)
    next_actions: List[str] = Field(default_factory=list)


class ExecutionResult(BaseModel):
    state: ExecutionState
    plan_id: str
    backend: str
    artifacts: List[ResultArtifact] = Field(default_factory=list)
    logs: List[str] = Field(default_factory=list)
