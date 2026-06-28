from __future__ import annotations

import json
import urllib.error
import urllib.request

from sparkos.domain.job import JobAttempt, JobRecord, JobStatus


class LivySubmitter:
    def __init__(self, base_url: str, timeout_seconds: int = 20):
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds

    def submit(self, record: JobRecord) -> JobAttempt:
        payload = {
            "name": record.payload.get("job_name", record.job_id),
            "conf": record.payload.get("spark_conf", {}),
            "args": record.payload.get("args", []),
            "file": record.payload.get("file", "local:///opt/sparkos/generated_job.py"),
        }
        request = urllib.request.Request(
            f"{self._base_url}/batches",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError) as exc:
            return JobAttempt(
                attempt=len(record.attempts) + 1,
                status=JobStatus.FAILED,
                message=f"Livy submit failed: {exc}",
            )

        return JobAttempt(
            attempt=len(record.attempts) + 1,
            status=JobStatus.SUBMITTED,
            external_id=str(raw.get("id", "")),
            message="Livy batch submitted.",
            metrics={"livy_state": raw.get("state", "unknown")},
        )
