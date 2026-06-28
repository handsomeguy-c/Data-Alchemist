from __future__ import annotations

import argparse
from pathlib import Path

from sparkos.application.agent_runtime import AgentRuntime
from sparkos.application.dag_observer import DagObserver
from sparkos.application.graph_runtime import GraphRuntime
from sparkos.application.job_orchestrator import JobOrchestrator
from sparkos.application.metadata_service import MetadataService
from sparkos.application.skill_registry import SkillRegistry
from sparkos.application.spark_tools import SparkToolExecutor
from sparkos.application.vector_kb_runtime import VectorKbRuntime
from sparkos.application.workbench import WorkbenchService
from sparkos.application.file_task_engines import SparkFileTaskEngine
from sparkos.infrastructure.catalog.config_catalog import ConfigCatalog
from sparkos.infrastructure.compute.router import ComputeRouter
from sparkos.infrastructure.embedding.local_embedding import LocalHashEmbedding
from sparkos.infrastructure.embedding.openai_compatible import OpenAICompatibleEmbedding
from sparkos.infrastructure.metadata.config_metadata import ConfigMetadataProvider
from sparkos.infrastructure.persistence.sqlite_job_store import SQLiteJobStore
from sparkos.infrastructure.spark.docker_runner import DockerSparkRunner
from sparkos.infrastructure.spark.livy_submitter import LivySubmitter
from sparkos.infrastructure.spark.local_submitter import LocalJobSubmitter
from sparkos.infrastructure.llm.local_models import LocalModelGateway
from sparkos.infrastructure.llm.model_router import ModelRouter
from sparkos.infrastructure.llm.openai_compatible import OpenAICompatibleGateway
from sparkos.infrastructure.settings import load_settings
from sparkos.infrastructure.vector.local_store import LocalJsonlVectorStore
from sparkos.interfaces.terminal.chat import run_terminal_chat


def build_service(config_path: Path) -> WorkbenchService:
    settings = load_settings(config_path)
    catalog = ConfigCatalog(settings.catalog.datasets)
    metadata_service = MetadataService(
        [ConfigMetadataProvider(settings.catalog.datasets)]
    )
    skill_registry = SkillRegistry.from_directory(settings.agent.skills_path)
    gateways = {"local": LocalModelGateway()}
    for name, provider in settings.models.providers.items():
        if provider.kind == "openai-compatible":
            gateways[name] = OpenAICompatibleGateway(provider)
    model_router = ModelRouter(settings.models, gateways)
    compute_router = ComputeRouter()
    agent_runtime = None
    artifact_root = settings.runtime.artifact_root
    embedding = _build_embedding(settings.embedding)
    vector_store = LocalJsonlVectorStore(
        settings.vector_store.path,
        embedding_model=embedding.model_name,
    )
    vector_runtime = VectorKbRuntime(
        embedding=embedding,
        vector_store=vector_store,
        collection=settings.vector_store.collection,
    )
    spark_file_engine = SparkFileTaskEngine(
        master_url=settings.runtime.spark_master_url,
        event_log_dir=settings.runtime.spark_event_log_dir,
        driver_host=settings.runtime.spark_driver_host,
        driver_port=settings.runtime.spark_driver_port,
    )
    if settings.agent.enabled:
        submitter = (
            LivySubmitter(settings.runtime.livy_url)
            if settings.runtime.livy_url
            else LocalJobSubmitter()
        )
        job_orchestrator = JobOrchestrator(
            store=SQLiteJobStore(settings.runtime.job_store_path),
            submitter=submitter,
        )
        tool_executor = SparkToolExecutor(
            artifact_root=artifact_root,
            job_orchestrator=job_orchestrator,
            dag_observer=DagObserver(),
            metadata_service=metadata_service,
            graph_runtime=GraphRuntime(artifact_root),
            spark_master_url=settings.runtime.spark_master_url,
            spark_event_log_dir=settings.runtime.spark_event_log_dir,
            spark_driver_host=settings.runtime.spark_driver_host,
            spark_driver_port=settings.runtime.spark_driver_port,
            require_spark=bool(settings.runtime.spark_master_url or settings.runtime.livy_url),
            docker_runner=DockerSparkRunner(
                repo_root=Path.cwd(),
                container_name=settings.runtime.docker_spark_container or "sparkos-spark-master",
                master_url=settings.runtime.docker_spark_master_url or "spark://spark-master:7077",
            ),
        )
        agent_runtime = AgentRuntime(
            catalog=catalog,
            skill_registry=skill_registry,
            artifact_root=artifact_root,
            tool_executor=tool_executor,
        )
    return WorkbenchService(
        catalog=catalog,
        model_router=model_router,
        compute_router=compute_router,
        workspace_root=Path.cwd(),
        agent_runtime=agent_runtime,
        artifact_root=artifact_root,
        vector_runtime=vector_runtime,
        spark_file_engine=spark_file_engine,
    )


def _build_embedding(settings):
    if settings.provider == "embedding-model" and settings.base_url and settings.api_key:
        return OpenAICompatibleEmbedding(settings)
    return LocalHashEmbedding(settings.dimension)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sparkos",
        description="Problem-first terminal workbench for big data and graph analysis.",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to the unified AGI-吉尔伽美什 config.yaml.",
    )
    parser.add_argument(
        "--config-dir",
        default=None,
        help="Deprecated. Directory containing config.yaml.",
    )
    parser.add_argument(
        "--plain",
        action="store_true",
        help="Run the dependency-light terminal interface instead of the Textual TUI.",
    )
    args = parser.parse_args()

    config_path = Path(args.config_dir) if args.config_dir else Path(args.config)
    service = build_service(config_path)

    if args.plain:
        run_terminal_chat(service)
        return

    try:
        from sparkos.interfaces.tui.app import SparkOsApp
    except ModuleNotFoundError as exc:
        if exc.name != "textual":
            raise
        print("Textual is not installed. Falling back to plain terminal mode.")
        print('Install with: python3 -m pip install -e ".[dev]"')
        run_terminal_chat(service)
        return

    SparkOsApp(service).run()


if __name__ == "__main__":
    main()
