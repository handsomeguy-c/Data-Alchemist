from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from sparkos.application.file_task_engines import (
    FileTaskEngine,
    LocalFileTaskEngine,
    SparkFileTaskEngine,
    write_jsonl,
)
from sparkos.application.vector_kb_runtime import VectorKbRuntime
from sparkos.domain.turn import TaskRequest, TaskType, TurnMode, TurnResponse
from sparkos.infrastructure.embedding.local_embedding import LocalHashEmbedding
from sparkos.infrastructure.vector.local_store import LocalJsonlVectorStore


class FileTaskService:
    def __init__(
        self,
        artifact_root: Path,
        spark_engine: Optional[FileTaskEngine] = None,
        local_engine: Optional[FileTaskEngine] = None,
        vector_runtime: Optional[VectorKbRuntime] = None,
    ):
        self._artifact_root = artifact_root
        self._spark_engine = spark_engine or SparkFileTaskEngine()
        self._local_engine = local_engine or LocalFileTaskEngine()
        self._vector_runtime = vector_runtime or VectorKbRuntime(
            embedding=LocalHashEmbedding(),
            vector_store=LocalJsonlVectorStore(artifact_root / "vector-store"),
        )

    def run(self, request: TaskRequest) -> TurnResponse:
        missing = [file for file in request.files if not file.exists]
        if missing:
            return TurnResponse(
                mode=TurnMode.TASK,
                task_type=request.task_type,
                files=request.files,
                message="有文件不存在，无法进入任务处理。",
                warnings=[f"未找到文件: {file.raw}" for file in missing],
            )

        engine, warnings = self._select_engine()
        if request.task_type == TaskType.AI_TRAINING_DATA:
            return self._training_data_response(request, engine, warnings)
        if request.task_type == TaskType.VECTOR_KNOWLEDGE_BASE:
            return self._vector_kb_response(request, engine, warnings)
        return TurnResponse(
            mode=TurnMode.TASK,
            task_type=TaskType.UNKNOWN,
            files=request.files,
            message="检测到文件输入，但无法判断任务类型。请说明是训练数据处理还是向量知识库构建。",
            warnings=[
                "任务处理模式目前只支持：自动 AI 训练数据处理、自动向量知识库构建。"
            ],
        )

    def _training_data_response(
        self,
        request: TaskRequest,
        engine: FileTaskEngine,
        warnings: list[str],
    ) -> TurnResponse:
        artifact = self._artifact_path("training-data", "training_dataset.jsonl")
        count = write_jsonl(artifact, engine.training_records(request))
        manifest = self._write_manifest(
            "training-data",
            request,
            engine,
            {"records": count, "dataset": str(artifact)},
        )
        return TurnResponse(
            mode=TurnMode.TASK,
            task_type=request.task_type,
            files=request.files,
            artifacts=[str(artifact), str(manifest)],
            warnings=warnings,
            message=(
                "已进入任务处理模式：自动 AI 训练数据处理。\n"
                f"执行引擎: {engine.name}\n"
                "将对输入文件进行清洗、结构化、去重、质量检查和训练样本导出。\n"
                f"生成样本数: {count}\n"
                f"计划产物: {artifact}"
            ),
        )

    def _vector_kb_response(
        self,
        request: TaskRequest,
        engine: FileTaskEngine,
        warnings: list[str],
    ) -> TurnResponse:
        chunks_artifact = self._artifact_path("vector-kb", "chunks.jsonl")
        chunks = list(engine.chunk_records(request))
        count = write_jsonl(chunks_artifact, chunks)
        vector_result = self._vector_runtime.build(chunks)
        manifest = self._write_manifest(
            "vector-kb",
            request,
            engine,
            {
                "chunk_count": count,
                "chunks": str(chunks_artifact),
                "embedding_status": "completed",
                "embedding_model": vector_result.embedding_model,
                "embedding_dimension": vector_result.dimension,
                "vector_collection": vector_result.collection,
                "vector_index": vector_result.index_path,
            },
        )
        return TurnResponse(
            mode=TurnMode.TASK,
            task_type=request.task_type,
            files=request.files,
            artifacts=[str(manifest), str(chunks_artifact)],
            warnings=warnings,
            message=(
                "已进入任务处理模式：自动向量知识库构建。\n"
                f"执行引擎: {engine.name}\n"
                "将对输入文件进行解析、切分、元数据抽取、向量化准备和索引清单生成。\n"
                f"生成 chunk 数: {count}\n"
                f"计划产物: {manifest}"
            ),
        )

    def _select_engine(self) -> tuple[FileTaskEngine, list[str]]:
        if self._spark_engine.is_available():
            return self._spark_engine, []
        return self._local_engine, ["未检测到 PySpark，已 fallback 到本地文件处理引擎。"]

    def _write_manifest(
        self,
        namespace: str,
        request: TaskRequest,
        engine: FileTaskEngine,
        extra: dict[str, object],
    ) -> Path:
        artifact = self._artifact_path(namespace, "manifest.json")
        manifest = {
            "task": request.task_type.value,
            "query": request.query,
            "engine": engine.name,
            "files": [str(file.path) for file in request.files],
            **extra,
        }
        artifact.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return artifact

    def _artifact_path(self, namespace: str, filename: str) -> Path:
        path = self._artifact_root / namespace
        path.mkdir(parents=True, exist_ok=True)
        return path / filename
