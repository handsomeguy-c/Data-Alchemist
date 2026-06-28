from pathlib import Path

from sparkos.application.agent_runtime import AgentRuntime
from sparkos.application.file_tasks import FileTaskService
from sparkos.application.graph_runtime import GraphRuntime
from sparkos.application.job_orchestrator import JobOrchestrator
from sparkos.application.skill_registry import SkillRegistry
from sparkos.cli import build_service
from sparkos.domain.agent import AgentRunStatus
from sparkos.domain.graph import GraphEdge
from sparkos.domain.job import JobStatus, JobType
from sparkos.domain.plan import PlanStatus
from sparkos.domain.problem import ProblemType
from sparkos.domain.turn import TaskType, TurnMode
from sparkos.infrastructure.catalog.config_catalog import ConfigCatalog
from sparkos.infrastructure.settings import load_settings
from sparkos.infrastructure.persistence.sqlite_job_store import SQLiteJobStore
from sparkos.infrastructure.spark.event_log_parser import EventLogParser
from sparkos.infrastructure.spark.local_submitter import LocalJobSubmitter


def test_prepare_graph_anomaly_plan_is_problem_first(tmp_path):
    service = build_service(_local_config(tmp_path))

    plan = service.prepare("帮我找出近 7 天交易网络里疑似团伙刷单的用户群")

    assert plan.problem.problem_type == ProblemType.ANOMALY_DETECTION
    assert plan.status == PlanStatus.READY
    assert "spark" not in plan.user_visible_summary.lower()
    assert any(step.capability == "community_detection" for step in plan.steps)


def test_execute_uses_simulated_backend_without_spark_requirement(tmp_path):
    service = build_service(_local_config(tmp_path))
    plan = service.prepare("分析用户和设备之间的异常关系网络")

    turn = service.execute(plan)

    assert turn.result is not None
    assert turn.result.state.value == "completed"
    assert turn.result.artifacts


def test_unknown_problem_requests_more_context(tmp_path):
    service = build_service(_local_config(tmp_path))

    plan = service.prepare("帮我看看这个")

    assert plan.status == PlanStatus.NEEDS_CONTEXT
    assert plan.problem.missing_context


def test_normal_input_stays_in_chat_mode(tmp_path):
    service = build_service(_local_config(tmp_path))

    response = service.handle_input("你好，介绍一下你自己")

    assert response.mode == TurnMode.CHAT
    assert response.task_type is None
    assert "@文件" in response.message


def test_at_file_enters_training_data_task_mode(tmp_path):
    data_file = tmp_path / "raw.txt"
    data_file.write_text("问题：什么是图计算？\n答案：分析实体关系。", encoding="utf-8")
    service = build_service(_local_config(tmp_path))

    response = service.handle_input(f"请做训练数据处理 @{data_file}")

    assert response.mode == TurnMode.TASK
    assert response.task_type == TaskType.AI_TRAINING_DATA
    assert response.files[0].exists
    assert len(response.artifacts) == 2
    assert Path(response.artifacts[0]).exists()
    assert "instruction" in Path(response.artifacts[0]).read_text(encoding="utf-8")
    assert "engine" in Path(response.artifacts[1]).read_text(encoding="utf-8")


def test_at_file_enters_vector_knowledge_base_task_mode(tmp_path):
    data_file = tmp_path / "doc.md"
    data_file.write_text("# 文档\n\n这是知识库内容。", encoding="utf-8")
    service = build_service(_local_config(tmp_path))

    response = service.handle_input(f"构建向量知识库 @{data_file}")

    assert response.mode == TurnMode.TASK
    assert response.task_type == TaskType.VECTOR_KNOWLEDGE_BASE
    assert len(response.artifacts) == 2
    assert Path(response.artifacts[0]).exists()
    assert Path(response.artifacts[1]).exists()
    assert "chunk_count" in Path(response.artifacts[0]).read_text(encoding="utf-8")


def test_at_file_with_unknown_task_requests_clarification(tmp_path):
    data_file = tmp_path / "doc.txt"
    data_file.write_text("hello", encoding="utf-8")
    service = build_service(_local_config(tmp_path))

    response = service.handle_input(f"处理一下 @{data_file}")

    assert response.mode == TurnMode.TASK
    assert response.task_type == TaskType.UNKNOWN
    assert response.warnings


def test_stream_input_streams_chat_chunks(tmp_path):
    service = build_service(_local_config(tmp_path))

    chunks = list(service.stream_input("你好"))

    assert len(chunks) > 1
    assert "正常对话模式" in "".join(chunks)


def test_stream_input_streams_file_task_result(tmp_path):
    data_file = tmp_path / "doc.md"
    data_file.write_text("知识库内容", encoding="utf-8")
    service = build_service(_local_config(tmp_path))

    chunks = list(service.stream_input(f"构建向量知识库 @{data_file}"))

    output = "".join(chunks)
    assert len(chunks) > 1
    assert "自动向量知识库构建" in output
    assert "manifest.json" in output


def test_skill_registry_loads_external_skills():
    registry = SkillRegistry.from_directory(Path("skills"))

    names = [skill.name for skill in registry.list()]

    assert names == [
        "spark-sql",
        "etl-pipeline",
        "data-cleaning",
        "data-quality",
        "feature-engineering",
        "spark-job",
        "dag-diagnosis",
    ]


def test_agent_runtime_generates_spark_job_artifacts(tmp_path):
    runtime = AgentRuntime(
        catalog=ConfigCatalog([]),
        skill_registry=SkillRegistry([]),
        artifact_root=tmp_path / "artifacts",
    )

    result = runtime.run("统计最近 7 天每个用户的交易次数并执行分布式任务")

    assert result.status == AgentRunStatus.COMPLETED
    assert any(step.skill_name == "spark-sql" for step in result.results)
    assert any(step.skill_name == "spark-job" for step in result.results)
    assert any(Path(path).exists() for path in result.artifacts)
    assert any("06_execution_config.json" in path for path in result.artifacts)


def test_settings_environment_overrides_runtime_paths(tmp_path, monkeypatch):
    config = _local_config(tmp_path)
    monkeypatch.setenv("SPARKOS_ARTIFACT_ROOT", str(tmp_path / "custom-artifacts"))
    monkeypatch.setenv("SPARKOS_SKILLS_PATH", "skills")

    settings = load_settings(config)

    assert settings.runtime.artifact_root == tmp_path / "custom-artifacts"
    assert str(settings.agent.skills_path) == "skills"


def test_settings_maps_master_and_embedding_model_slots(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        """
master-model:
  model: gpt-master
  url: https://model.example/v1
  api-key: dummy-master-key
embedding-model:
  model: embed-large
  url: https://embedding.example/v1
  api-key: dummy-embedding-key
catalog:
  datasets: []
""",
        encoding="utf-8",
    )

    settings = load_settings(config)

    assert settings.models.default_provider == "master-model"
    assert settings.models.role("chat").model == "gpt-master"
    assert settings.models.providers["master-model"].base_url == "https://model.example/v1"
    assert settings.models.providers["master-model"].api_key == "dummy-master-key"
    assert settings.embedding.provider == "embedding-model"
    assert settings.embedding.model == "embed-large"
    assert settings.embedding.api_key == "dummy-embedding-key"


def test_service_uses_configured_artifact_root(tmp_path):
    config = _local_config(tmp_path, artifact_root=tmp_path / "custom-artifacts")
    service = build_service(config)

    output = "".join(service.stream_input("统计最近 7 天每个用户的交易次数"))

    assert str(tmp_path / "custom-artifacts") in output


def test_job_orchestrator_persists_history(tmp_path):
    store = SQLiteJobStore(tmp_path / "jobs.sqlite3")
    orchestrator = JobOrchestrator(store=store, submitter=LocalJobSubmitter())

    record = orchestrator.submit(
        run_id="run-1",
        job_type=JobType.DISTRIBUTED_QUERY,
        payload={"sql": "select 1"},
    )

    loaded = orchestrator.get(record.job_id)
    assert loaded.status == JobStatus.SUCCEEDED
    assert loaded.attempts[0].external_id.startswith("local-")


def test_event_log_parser_extracts_stage_metrics(tmp_path):
    event_log = tmp_path / "eventlog.jsonl"
    event_log.write_text(
        "\n".join(
            [
                '{"Event":"SparkListenerApplicationStart","App ID":"app-1"}',
                '{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":1,"Number of Tasks":2,"Submission Time":100,"Completion Time":250}}',
                '{"Event":"SparkListenerTaskEnd","Stage ID":1,"Task Metrics":{"Shuffle Read Metrics":{"Remote Bytes Read":128},"Shuffle Write Metrics":{"Shuffle Bytes Written":256},"Input Metrics":{"Records Read":10},"Memory Bytes Spilled":64}}',
            ]
        ),
        encoding="utf-8",
    )

    observation = EventLogParser().parse(event_log)

    assert observation.app_id == "app-1"
    assert observation.stages[0].duration_ms == 150
    assert observation.total_shuffle_bytes == 384


def test_metadata_service_enriches_catalog_dataset(tmp_path):
    service = build_service(_local_config(tmp_path))
    runtime = service._agent_runtime  # noqa: SLF001 - exercising assembled app.

    output = "".join(runtime.stream("统计 transactions 表最近 7 天交易次数"))

    assert "transactions" in output
    assert "分布式查询" in output


def test_vector_kb_writes_embeddings_and_index(tmp_path):
    data_file = tmp_path / "doc.md"
    data_file.write_text("# 文档\n\n这是知识库内容。", encoding="utf-8")
    service = build_service(_local_config(tmp_path))

    response = service.handle_input(f"构建向量知识库 @{data_file}")

    manifest = Path(response.artifacts[0]).read_text(encoding="utf-8")
    assert '"embedding_status": "completed"' in manifest
    assert "local-hash-embedding" in manifest
    assert Path(response.artifacts[0]).exists()


def test_graph_runtime_runs_connected_components(tmp_path):
    result = GraphRuntime(tmp_path / "artifacts").run(
        "run-graph",
        [
            GraphEdge(src="u1", dst="d1"),
            GraphEdge(src="u2", dst="d1"),
            GraphEdge(src="u3", dst="d2"),
        ],
    )

    assert result.metrics["vertex_count"] == 5
    assert any(row["size"] == 3 for row in result.rows)
    assert Path(result.artifact_path).exists()


def test_agent_runtime_runs_graph_task_with_artifact(tmp_path):
    runtime = AgentRuntime(
        catalog=ConfigCatalog([]),
        skill_registry=SkillRegistry([]),
        artifact_root=tmp_path / "artifacts",
    )

    result = runtime.run("分析用户设备关系网络，找出可疑团伙社区")

    assert any(step.skill_name == "graph-compute" for step in result.results)
    assert any("08_graph_result.json" in path for path in result.artifacts)


def test_agent_runtime_diagnoses_shuffle_and_skew(tmp_path):
    runtime = AgentRuntime(
        catalog=ConfigCatalog([]),
        skill_registry=SkillRegistry([]),
        artifact_root=tmp_path / "artifacts",
    )

    output = "".join(runtime.stream("这个 Join 任务 shuffle 很大而且有数据倾斜，帮我诊断 DAG"))

    assert "性能诊断" in output
    assert "Shuffle" in output or "shuffle" in output
    assert "artifact:" in output


def test_workbench_routes_data_engineering_to_agent_runtime(tmp_path):
    service = build_service(_local_config(tmp_path))

    output = "".join(service.stream_input("构建用户日活宽表 ETL 并检查数据质量"))

    assert "AGI Runtime 已接管" in output
    assert "ETL 编排" in output
    assert "质量检查" in output


def test_file_task_service_prefers_spark_engine(tmp_path):
    data_file = tmp_path / "doc.txt"
    data_file.write_text("hello", encoding="utf-8")
    request = _task_request("构建向量知识库", data_file, TaskType.VECTOR_KNOWLEDGE_BASE)
    service = FileTaskService(
        artifact_root=tmp_path / "artifacts",
        spark_engine=_FakeEngine("spark", available=True),
        local_engine=_FakeEngine("local", available=True),
    )

    response = service.run(request)

    manifest = Path(response.artifacts[0]).read_text(encoding="utf-8")
    assert '"engine": "spark"' in manifest
    assert not response.warnings


def test_file_task_service_falls_back_to_local_engine(tmp_path):
    data_file = tmp_path / "doc.txt"
    data_file.write_text("hello", encoding="utf-8")
    request = _task_request("构建向量知识库", data_file, TaskType.VECTOR_KNOWLEDGE_BASE)
    service = FileTaskService(
        artifact_root=tmp_path / "artifacts",
        spark_engine=_FakeEngine("spark", available=False),
        local_engine=_FakeEngine("local", available=True),
    )

    response = service.run(request)

    manifest = Path(response.artifacts[0]).read_text(encoding="utf-8")
    assert '"engine": "local"' in manifest
    assert response.warnings


def _local_config(tmp_path, artifact_root=None) -> Path:
    config_path = tmp_path / "config.yaml"
    artifact_root_value = artifact_root or tmp_path / "artifacts"
    config_path.write_text(
        f"""
models:
  default_provider: local
  default_model: local-rule-planner
  fallback_provider: local
  roles:
    planner:
      provider: local
      model: local-rule-planner
    critic:
      provider: local
      model: local-rule-critic
    explainer:
      provider: local
      model: local-rule-explainer
  providers:
    local:
      kind: deterministic
catalog:
  datasets:
    - name: transactions
      description: Payment and order events with user, device, ip, amount, and timestamp fields.
      path: data/transactions.parquet
      format: parquet
      columns:
        - name: user_id
          semantic_type: entity
        - name: device_id
          semantic_type: entity
        - name: ip
          semantic_type: entity
        - name: amount
          semantic_type: metric
        - name: created_at
          semantic_type: timestamp
agent:
  enabled: true
  skills_path: skills
runtime:
  artifact_root: {artifact_root_value}
  job_store_path: {artifact_root_value}/runtime/jobs.sqlite3
""",
        encoding="utf-8",
    )
    return config_path


def _task_request(query, data_file, task_type):
    from sparkos.domain.turn import FileReference, TaskRequest

    return TaskRequest(
        query=query,
        task_type=task_type,
        files=[
            FileReference(
                raw=str(data_file),
                path=data_file,
                exists=data_file.exists(),
            )
        ],
    )


class _FakeEngine:
    def __init__(self, name, available):
        self.name = name
        self._available = available

    def is_available(self):
        return self._available

    def training_records(self, request):
        return [
            {
                "instruction": request.query,
                "input": "fake",
                "output": "",
                "engine": self.name,
            }
        ]

    def chunk_records(self, request):
        return [
            {
                "id": "fake-1",
                "text": "fake",
                "source_file": str(request.files[0].path),
                "chunk_id": 1,
                "metadata": {"engine": self.name},
            }
        ]
