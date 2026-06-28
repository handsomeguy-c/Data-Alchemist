from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from sparkos.domain.catalog import DatasetProfile


class AgentRunStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    NEEDS_CONTEXT = "needs_context"


class SkillSpec(BaseModel):
    name: str
    description: str
    body: str = ""
    source_path: Optional[Path] = None


class AgentPlanStep(BaseModel):
    id: str
    skill_name: str
    tool_name: str
    objective: str
    depends_on: List[str] = Field(default_factory=list)


class AgentPlan(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    user_request: str
    intent: str
    datasets: List[DatasetProfile] = Field(default_factory=list)
    steps: List[AgentPlanStep] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class AgentStepResult(BaseModel):
    step_id: str
    skill_name: str
    tool_name: str
    status: str
    summary: str
    artifact_path: Optional[str] = None
    payload: Dict[str, object] = Field(default_factory=dict)


class AgentRunResult(BaseModel):
    status: AgentRunStatus
    plan: AgentPlan
    results: List[AgentStepResult] = Field(default_factory=list)
    artifacts: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    @property
    def is_successful(self) -> bool:
        return self.status == AgentRunStatus.COMPLETED
