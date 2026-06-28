from __future__ import annotations

from pathlib import Path
from typing import Optional

from sparkos.domain.diagnosis import DagObservation, StageMetric
from sparkos.domain.job import JobRecord


class DagObserver:
    def observe(
        self,
        job: JobRecord,
        event_log_path: Optional[Path] = None,
    ) -> DagObservation:
        if event_log_path and event_log_path.exists():
            return self._from_event_log(job, event_log_path)
        latest = job.latest_attempt
        metrics = latest.metrics if latest else {}
        stage = StageMetric(
            stage_id="local-0",
            duration_ms=int(metrics.get("duration_ms", 0) or 0),
            task_count=1,
            shuffle_read_bytes=int(metrics.get("shuffle_read_bytes", 0) or 0),
            shuffle_write_bytes=int(metrics.get("shuffle_write_bytes", 0) or 0),
        )
        return DagObservation(
            app_id=latest.external_id if latest else "",
            job_id=job.job_id,
            stages=[stage],
            logs=[latest.message if latest else "job has no attempts"],
            metadata={"status": job.status.value, "source": "job_record"},
        )

    def _from_event_log(self, job: JobRecord, path: Path) -> DagObservation:
        from sparkos.infrastructure.spark.event_log_parser import EventLogParser

        observation = EventLogParser().parse(path)
        return observation.model_copy(update={"job_id": job.job_id})
