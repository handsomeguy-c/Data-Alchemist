from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple

from sparkos.domain.turn import FileReference, TaskRequest, TaskType, TurnMode


class TurnRouter:
    _FILE_PATTERN = re.compile(r"@(?P<path>\"[^\"]+\"|'[^']+'|[^\s]+)")

    def __init__(self, workspace_root: Path):
        self._workspace_root = workspace_root

    def route(self, user_input: str) -> Tuple[TurnMode, Optional[TaskRequest]]:
        files = self._extract_files(user_input)
        if not files:
            return TurnMode.CHAT, None

        query = self._strip_file_refs(user_input).strip()
        return (
            TurnMode.TASK,
            TaskRequest(
                query=query,
                task_type=self._detect_task_type(query),
                files=files,
            ),
        )

    def _extract_files(self, user_input: str) -> List[FileReference]:
        refs = []
        for match in self._FILE_PATTERN.finditer(user_input):
            raw = match.group("path").strip("\"'")
            path = Path(raw).expanduser()
            if not path.is_absolute():
                path = self._workspace_root / path
            refs.append(FileReference(raw=raw, path=path, exists=path.exists()))
        return refs

    def _strip_file_refs(self, user_input: str) -> str:
        return self._FILE_PATTERN.sub("", user_input)

    def _detect_task_type(self, query: str) -> TaskType:
        normalized = query.lower()
        training_keywords = ["训练", "training", "sft", "fine-tune", "finetune", "样本"]
        vector_keywords = ["向量", "知识库", "embedding", "rag", "检索"]
        if any(keyword in normalized for keyword in training_keywords):
            return TaskType.AI_TRAINING_DATA
        if any(keyword in normalized for keyword in vector_keywords):
            return TaskType.VECTOR_KNOWLEDGE_BASE
        return TaskType.UNKNOWN
