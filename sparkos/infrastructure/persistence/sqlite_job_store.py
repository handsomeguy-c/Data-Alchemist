from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from sparkos.domain.job import JobRecord


class SQLiteJobStore:
    def __init__(self, path: Path):
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save(self, record: JobRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                insert into jobs(job_id, run_id, status, payload_json)
                values (?, ?, ?, ?)
                on conflict(job_id) do update set
                  run_id=excluded.run_id,
                  status=excluded.status,
                  payload_json=excluded.payload_json
                """,
                (
                    record.job_id,
                    record.run_id,
                    record.status.value,
                    record.model_dump_json(),
                ),
            )

    def get(self, job_id: str) -> JobRecord:
        with self._connect() as conn:
            row = conn.execute(
                "select payload_json from jobs where job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Job not found: {job_id}")
        return JobRecord.model_validate(json.loads(row[0]))

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                create table if not exists jobs(
                  job_id text primary key,
                  run_id text not null,
                  status text not null,
                  payload_json text not null,
                  updated_at datetime default current_timestamp
                )
                """
            )

    def _connect(self):
        return sqlite3.connect(self._path)
