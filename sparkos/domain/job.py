from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    CREATED = "created"
    QUEUED = "queued"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class JobType(str, Enum):
    DISTRIBUTED_QUERY = "distributed_query"
    PIPELINE = "pipeline"
    GRAPH = "graph"
    VECTOR = "vector"


class JobAttempt(BaseModel):
    attempt: int
    status: JobStatus
    external_id: Optional[str] = None
    message: str = ""
    metrics: Dict[str, object] = Field(default_factory=dict)


class JobRecord(BaseModel):
    run_id: str
    job_id: str
    job_type: JobType
    status: JobStatus
    payload: Dict[str, object] = Field(default_factory=dict)
    attempts: List[JobAttempt] = Field(default_factory=list)
    artifacts: List[str] = Field(default_factory=list)

    @property
    def latest_attempt(self) -> Optional[JobAttempt]:
        if not self.attempts:
            return None
        return self.attempts[-1]
