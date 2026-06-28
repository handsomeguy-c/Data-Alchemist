from __future__ import annotations

import json
import urllib.error
import urllib.request

from sparkos.domain.diagnosis import DagObservation, StageMetric


class HistoryServerClient:
    def __init__(self, base_url: str, timeout_seconds: int = 20):
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds

    def observe_app(self, app_id: str) -> DagObservation:
        try:
            stages = self._get_json(f"/api/v1/applications/{app_id}/stages")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            return DagObservation(
                app_id=app_id,
                logs=[f"History Server unavailable: {exc}"],
                metadata={"source": "history_server"},
            )

        metrics = [
            StageMetric(
                stage_id=str(stage.get("stageId", "")),
                duration_ms=int(stage.get("duration", 0) or 0),
                task_count=int(stage.get("numTasks", 0) or 0),
                shuffle_read_bytes=int(stage.get("shuffleReadBytes", 0) or 0),
                shuffle_write_bytes=int(stage.get("shuffleWriteBytes", 0) or 0),
                input_rows=int(stage.get("inputRecords", 0) or 0),
            )
            for stage in stages
        ]
        return DagObservation(
            app_id=app_id,
            stages=metrics,
            metadata={"source": "history_server"},
        )

    def _get_json(self, path: str):
        with urllib.request.urlopen(
            f"{self._base_url}{path}",
            timeout=self._timeout_seconds,
        ) as response:
            return json.loads(response.read().decode("utf-8"))
