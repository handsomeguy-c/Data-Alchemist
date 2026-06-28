from __future__ import annotations

from enum import Enum
from typing import List
from uuid import uuid4

from pydantic import BaseModel, Field

from sparkos.domain.capability import ExecutionBackend
from sparkos.domain.catalog import DatasetProfile
from sparkos.domain.problem import ProblemSpec


class PlanStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    NEEDS_CONTEXT = "needs_context"
    REJECTED = "rejected"


class AnalysisStep(BaseModel):
    title: str
    rationale: str
    capability: str
    backend: ExecutionBackend


class AnalysisPlan(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    problem: ProblemSpec
    status: PlanStatus
    datasets: List[DatasetProfile] = Field(default_factory=list)
    steps: List[AnalysisStep] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    user_visible_summary: str

    @property
    def can_execute(self) -> bool:
        return self.status == PlanStatus.READY and bool(self.steps)
