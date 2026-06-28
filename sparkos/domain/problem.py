from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class ProblemType(str, Enum):
    DATA_PROFILING = "data_profiling"
    DATA_QUALITY = "data_quality"
    DATA_TRANSFORMATION = "data_transformation"
    RELATIONSHIP_ANALYSIS = "relationship_analysis"
    GRAPH_COMMUNITY = "graph_community"
    GRAPH_PATH = "graph_path"
    ANOMALY_DETECTION = "anomaly_detection"
    FEATURE_GENERATION = "feature_generation"
    UNKNOWN = "unknown"


class ProblemSpec(BaseModel):
    user_request: str
    problem_type: ProblemType
    objective: str
    entities: List[str] = Field(default_factory=list)
    time_range: Optional[str] = None
    constraints: List[str] = Field(default_factory=list)
    missing_context: List[str] = Field(default_factory=list)

    @property
    def is_ready(self) -> bool:
        return not self.missing_context
