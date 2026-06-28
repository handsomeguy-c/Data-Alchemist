from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, List, Protocol

from sparkos.domain.turn import TaskRequest
from sparkos.infrastructure.spark.runtime_checks import java_available


class FileTaskEngine(Protocol):
    @property
    def name(self) -> str:
        """Engine display name."""

    def is_available(self) -> bool:
        """Return whether this engine can execute now."""

    def training_records(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        """Generate training records."""

    def chunk_records(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        """Generate knowledge-base chunk records."""


class LocalFileTaskEngine:
    name = "local"

    def is_available(self) -> bool:
        return True

    def training_records(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        for file in request.files:
            text = self._read_text(file.path)
            for index, chunk in enumerate(self._chunks(text), start=1):
                yield {
                    "instruction": request.query or "根据输入内容生成高质量训练样本",
                    "input": chunk,
                    "output": "",
                    "source_file": str(file.path),
                    "chunk_id": index,
                    "content_hash": self._hash(chunk),
                    "engine": self.name,
                }

    def chunk_records(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        for file in request.files:
            text = self._read_text(file.path)
            for index, chunk in enumerate(self._chunks(text), start=1):
                yield {
                    "id": f"{file.path.name}-{index}-{self._hash(chunk)[:8]}",
                    "text": chunk,
                    "source_file": str(file.path),
                    "chunk_id": index,
                    "metadata": {
                        "query": request.query,
                        "filename": file.path.name,
                        "engine": self.name,
                    },
                }

    def _read_text(self, path: Path) -> str:
        return path.read_text(encoding="utf-8", errors="replace")

    def _chunks(self, text: str, max_chars: int = 1200) -> Iterable[str]:
        normalized = "\n".join(line.rstrip() for line in text.splitlines())
        paragraphs = [part.strip() for part in normalized.split("\n\n") if part.strip()]
        if not paragraphs and normalized.strip():
            paragraphs = [normalized.strip()]

        buffer = ""
        for paragraph in paragraphs:
            if len(buffer) + len(paragraph) + 2 <= max_chars:
                buffer = f"{buffer}\n\n{paragraph}".strip()
                continue
            if buffer:
                yield buffer
            if len(paragraph) <= max_chars:
                buffer = paragraph
            else:
                for start in range(0, len(paragraph), max_chars):
                    yield paragraph[start : start + max_chars]
                buffer = ""
        if buffer:
            yield buffer

    def _hash(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


class SparkFileTaskEngine:
    name = "spark"

    def __init__(
        self,
        app_name: str = "AGI-Gilgamesh-FileTasks",
        master_url: str | None = None,
        event_log_dir: str | None = None,
        driver_host: str | None = None,
        driver_port: int | None = None,
    ):
        self._app_name = app_name
        self._master_url = master_url
        self._event_log_dir = event_log_dir
        self._driver_host = driver_host
        self._driver_port = driver_port
        self._spark_sql = None
        try:
            import pyspark.sql  # type: ignore

            self._spark_sql = pyspark.sql
        except ModuleNotFoundError:
            self._spark_sql = None

    def is_available(self) -> bool:
        return self._spark_sql is not None and java_available()

    def training_records(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        for row in self._text_rows(request):
            text = row["text"]
            if not text:
                continue
            yield {
                "instruction": request.query or "根据输入内容生成高质量训练样本",
                "input": text,
                "output": "",
                "source_file": row["source_file"],
                "chunk_id": row["chunk_id"],
                "content_hash": row["content_hash"],
                "engine": self.name,
            }

    def chunk_records(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        for row in self._text_rows(request):
            text = row["text"]
            if not text:
                continue
            source_file = row["source_file"]
            chunk_id = row["chunk_id"]
            digest = row["content_hash"][:8]
            yield {
                "id": f"{Path(source_file).name}-{chunk_id}-{digest}",
                "text": text,
                "source_file": source_file,
                "chunk_id": chunk_id,
                "metadata": {
                    "query": request.query,
                    "filename": Path(source_file).name,
                    "engine": self.name,
                },
            }

    def _text_rows(self, request: TaskRequest) -> Iterable[dict[str, object]]:
        spark = self._session()
        paths = [str(file.path) for file in request.files]
        data_frame = spark.read.text(paths).withColumnRenamed("value", "text")
        rows = (
            data_frame.rdd.zipWithIndex()
            .map(lambda pair: self._row_to_record(pair[0]["text"], pair[1], paths))
            .filter(lambda record: bool(record["text"]))
            .collect()
        )
        yield from rows

    def _row_to_record(
        self,
        text: str,
        index: int,
        paths: List[str],
    ) -> dict[str, object]:
        source_file = paths[0] if len(paths) == 1 else "multiple_files"
        normalized = (text or "").strip()
        return {
            "text": normalized,
            "source_file": source_file,
            "chunk_id": index + 1,
            "content_hash": hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        }

    def _session(self):
        builder = self._spark_sql.SparkSession.builder.appName(self._app_name)
        if self._master_url:
            builder = builder.master(self._master_url)
        if self._event_log_dir:
            builder = builder.config("spark.eventLog.enabled", "true")
            builder = builder.config("spark.eventLog.dir", self._event_log_dir)
        if self._driver_host:
            builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
            builder = builder.config("spark.driver.host", self._driver_host)
        if self._driver_port:
            builder = builder.config("spark.driver.port", str(self._driver_port))
        return builder.getOrCreate()


def write_jsonl(path: Path, records: Iterable[dict[str, object]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count
