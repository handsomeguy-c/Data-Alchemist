from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, Field


class StageMetric(BaseModel):
    stage_id: str
    duration_ms: int = 0
    task_count: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    input_rows: int = 0
    spilled_bytes: int = 0
    skew_ratio: float = 1.0


class DagObservation(BaseModel):
    app_id: str = ""
    job_id: str = ""
    stages: List[StageMetric] = Field(default_factory=list)
    logs: List[str] = Field(default_factory=list)
    metadata: Dict[str, object] = Field(default_factory=dict)

    @property
    def total_shuffle_bytes(self) -> int:
        return sum(
            stage.shuffle_read_bytes + stage.shuffle_write_bytes
            for stage in self.stages
        )
