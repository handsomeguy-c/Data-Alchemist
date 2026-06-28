from __future__ import annotations

import json
from pathlib import Path

from sparkos.domain.diagnosis import DagObservation, StageMetric


class EventLogParser:
    def parse(self, path: Path) -> DagObservation:
        stages: dict[str, StageMetric] = {}
        app_id = ""
        logs = []
        with path.open("r", encoding="utf-8", errors="replace") as file:
            for line in file:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                name = event.get("Event", "")
                if name == "SparkListenerApplicationStart":
                    app_id = str(event.get("App ID", ""))
                elif name == "SparkListenerStageCompleted":
                    info = event.get("Stage Info", {})
                    stage_id = str(info.get("Stage ID", "unknown"))
                    metric = stages.setdefault(stage_id, StageMetric(stage_id=stage_id))
                    metric.task_count = int(info.get("Number of Tasks", 0) or 0)
                    metric.duration_ms = self._duration(info)
                elif name == "SparkListenerTaskEnd":
                    self._merge_task_metrics(stages, event)
                elif "Error" in event:
                    logs.append(str(event.get("Error")))
        return DagObservation(
            app_id=app_id,
            stages=list(stages.values()),
            logs=logs,
            metadata={"source": str(path)},
        )

    def _duration(self, info: dict) -> int:
        submission = int(info.get("Submission Time", 0) or 0)
        completion = int(info.get("Completion Time", 0) or 0)
        if submission and completion:
            return max(0, completion - submission)
        return 0

    def _merge_task_metrics(
        self,
        stages: dict[str, StageMetric],
        event: dict,
    ) -> None:
        stage_id = str(event.get("Stage ID", "unknown"))
        metric = stages.setdefault(stage_id, StageMetric(stage_id=stage_id))
        task_metrics = event.get("Task Metrics", {})
        shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
        shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
        input_metrics = task_metrics.get("Input Metrics", {})
        metric.shuffle_read_bytes += int(
            shuffle_read.get("Remote Bytes Read", 0) or 0
        )
        metric.shuffle_write_bytes += int(
            shuffle_write.get("Shuffle Bytes Written", 0) or 0
        )
        metric.input_rows += int(input_metrics.get("Records Read", 0) or 0)
        metric.spilled_bytes += int(task_metrics.get("Memory Bytes Spilled", 0) or 0)
        metric.spilled_bytes += int(task_metrics.get("Disk Bytes Spilled", 0) or 0)
