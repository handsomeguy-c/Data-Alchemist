from __future__ import annotations

import hashlib

from sparkos.domain.job import JobAttempt, JobRecord, JobStatus


class LocalJobSubmitter:
    def submit(self, record: JobRecord) -> JobAttempt:
        digest = hashlib.sha256(record.model_dump_json().encode("utf-8")).hexdigest()
        return JobAttempt(
            attempt=len(record.attempts) + 1,
            status=JobStatus.SUCCEEDED,
            external_id=f"local-{digest[:10]}",
            message="本地提交器已记录作业生命周期。",
            metrics={
                "queue_time_ms": 1,
                "duration_ms": 5,
                "execution_mode": "local",
            },
        )
