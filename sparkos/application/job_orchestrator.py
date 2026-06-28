from __future__ import annotations

from typing import Protocol
from uuid import uuid4

from sparkos.domain.job import JobAttempt, JobRecord, JobStatus, JobType


class JobStorePort(Protocol):
    def save(self, record: JobRecord) -> None:
        """Persist a job record."""

    def get(self, job_id: str) -> JobRecord:
        """Load a job record by job id."""


class JobSubmitterPort(Protocol):
    def submit(self, record: JobRecord) -> JobAttempt:
        """Submit or simulate a job attempt."""


class JobOrchestrator:
    def __init__(
        self,
        store: JobStorePort,
        submitter: JobSubmitterPort,
        max_retries: int = 1,
    ):
        self._store = store
        self._submitter = submitter
        self._max_retries = max_retries

    def submit(
        self,
        run_id: str,
        job_type: JobType,
        payload: dict[str, object],
    ) -> JobRecord:
        record = JobRecord(
            run_id=run_id,
            job_id=f"job-{uuid4().hex[:12]}",
            job_type=job_type,
            status=JobStatus.CREATED,
            payload=payload,
        )
        record = self._transition(record, JobStatus.QUEUED)
        attempt = self._submitter.submit(record)
        record.attempts.append(attempt)
        record.status = attempt.status

        while record.status == JobStatus.FAILED and self._can_retry(record):
            record = self._transition(record, JobStatus.RETRYING)
            attempt = self._submitter.submit(record)
            record.attempts.append(attempt)
            record.status = attempt.status

        self._store.save(record)
        return record

    def get(self, job_id: str) -> JobRecord:
        return self._store.get(job_id)

    def _transition(self, record: JobRecord, status: JobStatus) -> JobRecord:
        record.status = status
        self._store.save(record)
        return record

    def _can_retry(self, record: JobRecord) -> bool:
        if len(record.attempts) > self._max_retries:
            return False
        latest = record.latest_attempt
        if latest is None:
            return False
        retryable = ["oom", "shuffle", "fetch failed", "timeout", "transient"]
        return any(token in latest.message.lower() for token in retryable)
