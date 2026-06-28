from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

from sparkos.application.dag_observer import DagObserver
from sparkos.application.dag_diagnosis import DagDiagnosisEngine
from sparkos.application.graph_edge_loader import GraphEdgeLoader
from sparkos.application.graph_runtime import GraphRuntime
from sparkos.application.job_orchestrator import JobOrchestrator
from sparkos.application.metadata_service import MetadataService
from sparkos.application.spark_execution import SparkExecution
from sparkos.application.spark_session_factory import SparkSessionFactory
from sparkos.application.spark_templates import SparkTaskTemplates
from sparkos.domain.agent import AgentPlan, AgentPlanStep, AgentStepResult
from sparkos.domain.catalog import DatasetProfile
from sparkos.domain.job import JobType
from sparkos.infrastructure.persistence.sqlite_job_store import SQLiteJobStore
from sparkos.infrastructure.spark.docker_runner import DockerSparkRunner
from sparkos.infrastructure.spark.local_submitter import LocalJobSubmitter
from sparkos.infrastructure.spark.runtime_checks import java_available


class SparkToolExecutor:
    def __init__(
        self,
        artifact_root: Path,
        job_orchestrator: Optional[JobOrchestrator] = None,
        dag_observer: Optional[DagObserver] = None,
        metadata_service: Optional[MetadataService] = None,
        graph_runtime: Optional[GraphRuntime] = None,
        spark_master_url: Optional[str] = None,
        spark_event_log_dir: Optional[str] = None,
        spark_driver_host: Optional[str] = None,
        spark_driver_port: Optional[int] = None,
        require_spark: bool = False,
        docker_runner: Optional[DockerSparkRunner] = None,
    ):
        self._artifact_root = artifact_root
        self._dag_engine = DagDiagnosisEngine()
        self._dag_observer = dag_observer or DagObserver()
        self._job_orchestrator = job_orchestrator or JobOrchestrator(
            store=SQLiteJobStore(artifact_root / "runtime" / "jobs.sqlite3"),
            submitter=LocalJobSubmitter(),
        )
        self._metadata_service = metadata_service
        self._graph_runtime = graph_runtime or GraphRuntime(artifact_root)
        self._graph_edge_loader = GraphEdgeLoader()
        self._spark_master_url = spark_master_url
        self._spark_event_log_dir = spark_event_log_dir
        self._spark_driver_host = spark_driver_host
        self._spark_driver_port = spark_driver_port
        self._require_spark = require_spark
        self._docker_runner = docker_runner
        self._run_results: dict[str, list[AgentStepResult]] = {}
        self._templates = SparkTaskTemplates()
        self._spark_execution = SparkExecution(
            None,
            self._templates,
            self._spark_event_log_dir,
            self._docker_runner,
        )
        self._pyspark_sql = None
        try:
            import pyspark.sql  # type: ignore

            self._pyspark_sql = pyspark.sql
            self._spark_sessions = SparkSessionFactory(
                spark_sql=self._pyspark_sql,
                master_url=self._spark_master_url,
                event_log_dir=self._spark_event_log_dir,
                driver_host=self._spark_driver_host,
                driver_port=self._spark_driver_port,
            )
            self._spark_execution = SparkExecution(
                self._spark_sessions,
                self._templates,
                self._spark_event_log_dir,
                self._docker_runner,
            )
        except ModuleNotFoundError:
            self._pyspark_sql = None
            self._spark_sessions = None

    @property
    def spark_available(self) -> bool:
        return bool(self._docker_runner and self._docker_runner.is_available()) or (
            self._pyspark_sql is not None and java_available()
        )

    def execute(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        handlers = {
            "spark_sql": self._spark_sql,
            "etl_pipeline": self._etl_pipeline,
            "data_cleaning": self._data_cleaning,
            "data_quality": self._data_quality,
            "feature_engineering": self._feature_engineering,
            "spark_job": self._spark_job,
            "dag_diagnosis": self._dag_diagnosis,
            "graph_compute": self._graph_compute,
        }
        handler = handlers.get(step.tool_name)
        if handler is None:
            raise ValueError(f"Unknown Spark tool: {step.tool_name}")
        result = handler(plan, step)
        self._run_results.setdefault(plan.id, []).append(result)
        return result

    def _spark_sql(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        dataset = self._primary_dataset(plan)
        sql = self._templates.build_query_sql(plan.user_request, dataset)
        payload = {
            "skill": "spark-sql",
            "sql": sql,
            "explanation": "根据自然语言需求生成分布式查询计划，并保留分区、Join 和聚合风险提示。",
            "assumptions": self._templates.dataset_assumptions(plan),
            "risk_warnings": self._templates.sql_risks(sql, dataset),
            "optimization_tips": [
                "优先使用分区字段过滤时间范围。",
                "大表 Join 前先过滤或预聚合，必要时广播小表。",
                "避免 select *，只读取业务所需字段。",
            ],
            "next_skill": "spark-job",
        }
        artifact = self._write_json(plan.id, "01_distributed_query.json", payload)
        return self._result(step, "已生成分布式查询任务描述。", artifact, payload)

    def _etl_pipeline(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        dataset = self._primary_dataset(plan)
        target = self._templates.target_table_name(plan, "dwd_agent_output_di")
        steps = [
            {
                "step_id": "read_source",
                "type": "read",
                "description": f"读取源数据 {dataset.name}，保留必要字段和业务分区。",
                "sql_or_code": self._templates.select_source_sql(dataset),
            },
            {
                "step_id": "transform_records",
                "type": "transform",
                "description": "执行字段标准化、派生字段和业务过滤。",
                "sql_or_code": self._templates.transform_sql(dataset),
            },
            {
                "step_id": "write_target",
                "type": "write",
                "description": f"写入目标表 {target}，按业务日期分区覆盖。",
                "sql_or_code": (
                    f"insert overwrite table {target} partition(dt='${{biz_date}}')\n"
                    "select * from transformed_records;"
                ),
            },
        ]
        payload = {
            "pipeline_name": self._templates.slug(plan.intent or "agent_etl"),
            "steps": steps,
            "dependencies": [
                {"from": "read_source", "to": "transform_records"},
                {"from": "transform_records", "to": "write_target"},
            ],
            "target_table": target,
            "run_config": self._templates.default_run_config(),
            "next_skill": "spark-job",
        }
        artifact = self._write_json(plan.id, "02_etl_pipeline.json", payload)
        return self._result(step, "已生成可编排 ETL DAG。", artifact, payload)

    def _data_cleaning(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        dataset = self._primary_dataset(plan)
        rules = []
        for column in dataset.columns:
            if column.semantic_type in {"id", "entity"}:
                rules.append(
                    {
                        "column": column.name,
                        "problem": "关键字段可能存在空值或重复。",
                        "strategy": "非空校验，重复记录按更新时间或业务时间保留最新。",
                        "reason": "关键实体字段异常会影响下游 Join、图构建和样本粒度。",
                    }
                )
            elif column.semantic_type == "metric":
                rules.append(
                    {
                        "column": column.name,
                        "problem": "数值字段可能存在极端值或非法值。",
                        "strategy": "生成异常标记，必要时按业务阈值截断。",
                        "reason": "保守处理可减少业务语义损伤。",
                    }
                )
            elif column.semantic_type == "timestamp":
                rules.append(
                    {
                        "column": column.name,
                        "problem": "时间字段可能格式不一致或为空。",
                        "strategy": "统一转换为 timestamp，无法解析时保留空值并打标。",
                        "reason": "避免错误填充当前时间造成时间穿越。",
                    }
                )
        if not rules:
            rules.append(
                {
                    "column": "*",
                    "problem": "缺少字段语义画像。",
                    "strategy": "先做 schema、空值率、重复率和枚举值探查。",
                    "reason": "未知字段不应自动执行破坏性清洗。",
                }
            )
        payload = {
            "cleaning_rules": rules,
            "sql_or_code": self._templates.cleaning_sql(dataset),
            "before_after_metrics": {
                "before": {"null_rate_scan": "required", "duplicate_scan": "required"},
                "after_expected": {"critical_nulls": 0, "duplicate_keys": 0},
            },
            "warnings": ["清洗动作默认生成规则和代码，执行前需要确认关键字段口径。"],
            "next_skill": "data-quality",
        }
        artifact = self._write_json(plan.id, "03_data_cleaning.json", payload)
        return self._result(step, "已生成数据清洗规则和分布式转换逻辑。", artifact, payload)

    def _data_quality(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        dataset = self._primary_dataset(plan)
        checks = []
        for column in dataset.columns:
            if column.semantic_type in {"id", "entity"}:
                checks.append(
                    {
                        "check_name": f"{column.name} 非空",
                        "sql": (
                            f"select count(*) as invalid_cnt from {dataset.name} "
                            f"where {column.name} is null"
                        ),
                        "severity": "blocker",
                        "pass_condition": "invalid_cnt = 0",
                    }
                )
                checks.append(
                    {
                        "check_name": f"{column.name} 重复率",
                        "sql": (
                            f"select count(*) - count(distinct {column.name}) "
                            f"as duplicate_cnt from {dataset.name}"
                        ),
                        "severity": "warning",
                        "pass_condition": "duplicate_cnt is within baseline",
                    }
                )
            if column.semantic_type == "metric":
                checks.append(
                    {
                        "check_name": f"{column.name} 数值范围",
                        "sql": (
                            f"select min({column.name}) as min_value, "
                            f"max({column.name}) as max_value from {dataset.name}"
                        ),
                        "severity": "warning",
                        "pass_condition": "range matches business baseline",
                    }
                )
        if not checks:
            checks.append(
                {
                    "check_name": "行数检查",
                    "sql": f"select count(*) as row_count from {dataset.name}",
                    "severity": "info",
                    "pass_condition": "row_count > 0",
                }
            )
        payload = {
            "quality_checks": checks,
            "quality_report": {
                "summary": "已生成质量检查项，等待分布式执行后回填检查结果。",
                "passed": [],
                "failed": [],
                "warnings": ["当前阶段为规则生成，尚未执行质量 SQL。"],
            },
            "recommended_actions": [
                "把 blocker 级检查接入发布阻断。",
                "为行数、空值率和指标分布配置 7 日基线。",
            ],
            "next_skill": "spark-job",
        }
        artifact = self._write_json(plan.id, "04_data_quality.json", payload)
        return self._result(step, "已生成数据质量检查规则。", artifact, payload)

    def _feature_engineering(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        dataset = self._primary_dataset(plan)
        entity = self._templates.first_column(dataset, {"entity", "id"}) or "entity_id"
        metric = self._templates.first_column(dataset, {"metric"})
        features = [
            {
                "name": "event_cnt_7d",
                "type": "numeric",
                "description": "样本实体近 7 天事件次数。",
                "source": dataset.name,
                "calculation": f"count(*) grouped by {entity} in 7-day window",
            },
            {
                "name": "active_days_30d",
                "type": "numeric",
                "description": "样本实体近 30 天活跃天数。",
                "source": dataset.name,
                "calculation": f"count(distinct dt) grouped by {entity}",
            },
        ]
        if metric:
            features.append(
                {
                    "name": f"{metric}_sum_30d",
                    "type": "numeric",
                    "description": f"{metric} 近 30 天累计值。",
                    "source": dataset.name,
                    "calculation": f"sum({metric}) in 30-day window",
                }
            )
        payload = {
            "feature_set_name": self._templates.slug(f"{entity}_feature_set"),
            "entity_key": entity,
            "label": {
                "name": "label",
                "definition": "需要用户确认预测目标、观察窗口和标签窗口。",
            },
            "features": features,
            "sql_or_code": self._templates.feature_sql(dataset, entity, metric),
            "leakage_warnings": [
                "标签时间之后的数据不能进入特征窗口。",
                "训练样本需要明确 observation_time 和 label_time。",
            ],
            "next_skill": "spark-job",
        }
        artifact = self._write_json(plan.id, "05_feature_engineering.json", payload)
        return self._result(step, "已生成特征字典和分布式加工逻辑。", artifact, payload)

    def _spark_job(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        dataset = self._primary_dataset(plan)
        sql = self._templates.build_query_sql(plan.user_request, dataset)
        docker_ready = self._docker_runner and self._docker_runner.is_available()
        if self._require_spark and not self.spark_available:
            return self._failed_spark_job(plan, step, dataset, sql)
        status = "planned"
        log_summary = "已进入本地作业编排队列。"
        metrics: dict[str, object] = {
            "execution_mode": "orchestrated",
            "spark_available": self.spark_available,
        }
        if docker_ready and self._can_execute_dataset(dataset):
            status, log_summary, metrics = self._try_execute_docker_sql(sql, dataset, plan.id)
        elif self.spark_available and self._can_execute_dataset(dataset):
            status, log_summary, metrics = self._try_execute_sql(sql, dataset)

        job = self._job_orchestrator.submit(
            run_id=plan.id,
            job_type=JobType.DISTRIBUTED_QUERY,
            payload={
                "job_name": f"agi-gilgamesh-{plan.id[:8]}",
                "sql": sql,
                "dataset": dataset.model_dump(),
                "spark_conf": self._templates.default_run_config()["spark_conf"],
                "runtime_status": status,
            },
        )
        payload = {
            "job_id": job.job_id,
            "status": status,
            "orchestrator_status": job.status.value,
            "attempts": [attempt.model_dump() for attempt in job.attempts],
            "submit_command": self._submit_command(plan, sql),
            "log_summary": log_summary,
            "failure_reason": None if status == "success" else log_summary,
            "retry_suggestion": "先根据 DAG 诊断结果调整 SQL、分区和资源参数，再重试。",
            "metrics": metrics,
            "history_store": str(self._artifact_root / "runtime" / "jobs.sqlite3"),
            "next_skill": "dag-diagnosis",
        }
        artifact = self._write_json(plan.id, "06_execution_config.json", payload)
        return self._result(step, "已生成分布式执行配置并完成可用性检查。", artifact, payload)

    def _try_execute_docker_sql(self, sql, dataset, run_id):
        return self._spark_execution.run_docker(sql, dataset, run_id)

    def _failed_spark_job(self, plan, step, dataset, sql) -> AgentStepResult:
        payload = {
            "status": "failed",
            "failure_reason": "真实 Spark 执行被要求开启，但 PySpark driver 不可用。请安装 Java 并确认 Spark runtime 可启动。",
            "sql": sql,
            "dataset": dataset.model_dump(),
            "metrics": {"execution_mode": "spark_required", "spark_available": False},
            "next_skill": "dag-diagnosis",
        }
        artifact = self._write_json(plan.id, "06_execution_config.json", payload)
        return self._result(step, "真实 Spark 执行不可用，已写入失败诊断产物。", artifact, payload)

    def _dag_diagnosis(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        observation = None
        job_result = self._latest_result(plan, "spark-job")
        if job_result and job_result.payload.get("job_id"):
            job = self._job_orchestrator.get(str(job_result.payload["job_id"]))
            observation = self._dag_observer.observe(job)
        payload = self._dag_engine.diagnose(plan.user_request, observation)
        artifact = self._write_json(plan.id, "07_dag_diagnosis.json", payload)
        return self._result(step, "已生成 DAG 性能诊断和优化建议。", artifact, payload)

    def _graph_compute(self, plan: AgentPlan, step: AgentPlanStep) -> AgentStepResult:
        edges = self._graph_edge_loader.load_for_plan(plan)
        algorithm = "degree" if "关键" in plan.user_request else "connected_components"
        graph_result = self._graph_runtime.run(plan.id, edges, algorithm=algorithm)
        payload = graph_result.model_dump()
        return self._result(
            step,
            "已完成图计算任务并生成图分析结果。",
            Path(graph_result.artifact_path),
            payload,
        )

    def _result(
        self,
        step: AgentPlanStep,
        summary: str,
        artifact: Path,
        payload: dict[str, object],
    ) -> AgentStepResult:
        return AgentStepResult(
            step_id=step.id,
            skill_name=step.skill_name,
            tool_name=step.tool_name,
            status="completed",
            summary=summary,
            artifact_path=str(artifact),
            payload=payload,
        )

    def _try_execute_sql(
        self,
        sql: str,
        dataset: DatasetProfile,
    ) -> tuple[str, str, dict[str, object]]:
        return self._spark_execution.run_pyspark(sql, dataset)

    def _can_execute_dataset(self, dataset: DatasetProfile) -> bool:
        if dataset.format.lower() == "hive":
            return bool(dataset.path or dataset.name)
        return bool(dataset.path) and Path(dataset.path).exists()

    def _latest_result(
        self,
        plan: AgentPlan,
        skill_name: str,
    ) -> Optional[AgentStepResult]:
        results = self._run_results.get(plan.id, [])
        for result in reversed(results):
            if result.skill_name == skill_name:
                return result
        return None

    def _primary_dataset(self, plan: AgentPlan) -> DatasetProfile:
        dataset = self._templates.primary_dataset(plan)
        if self._metadata_service is None:
            return dataset
        metadata = self._metadata_service.enrich_dataset(dataset)
        return DatasetProfile(
            name=metadata.name,
            description=metadata.description,
            path=metadata.path,
            format=metadata.format,
            columns=metadata.columns,
        )

    def _submit_command(self, plan: AgentPlan, sql: str) -> str:
        sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:10]
        return (
            "spark-submit --class org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver "
            f"--conf spark.app.name=agi-gilgamesh-{plan.id[:8]} "
            f"--conf agi.sql.hash={sql_hash} "
            "<generated-spark-sql-job>"
        )

    def _write_json(self, run_id: str, filename: str, payload: dict[str, object]) -> Path:
        run_dir = self._artifact_root / "agent-runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / filename
        content = json.dumps(payload, ensure_ascii=False, indent=2)
        path.write_text(content, encoding="utf-8")
        return path
