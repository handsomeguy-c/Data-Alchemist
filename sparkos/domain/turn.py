from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field


class TurnMode(str, Enum):
    CHAT = "chat"
    TASK = "task"


class TaskType(str, Enum):
    AI_TRAINING_DATA = "ai_training_data"
    VECTOR_KNOWLEDGE_BASE = "vector_knowledge_base"
    UNKNOWN = "unknown"


class FileReference(BaseModel):
    raw: str
    path: Path
    exists: bool


class TaskRequest(BaseModel):
    query: str
    task_type: TaskType
    files: List[FileReference] = Field(default_factory=list)


class TurnResponse(BaseModel):
    mode: TurnMode
    message: str
    task_type: Optional[TaskType] = None
    files: List[FileReference] = Field(default_factory=list)
    artifacts: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
