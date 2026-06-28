from __future__ import annotations

from sparkos.application.spark_session_factory import SparkSessionFactory
from sparkos.application.spark_templates import SparkTaskTemplates
from sparkos.domain.catalog import DatasetProfile
from sparkos.infrastructure.spark.docker_runner import DockerSparkRunner


class SparkExecution:
    def __init__(
        self,
        sessions: SparkSessionFactory | None,
        templates: SparkTaskTemplates,
        event_log_dir: str | None,
        docker_runner: DockerSparkRunner | None = None,
    ):
        self._sessions = sessions
        self._templates = templates
        self._event_log_dir = event_log_dir
        self._docker_runner = docker_runner

    def run_docker(self, sql, dataset, run_id):
        executable_sql = self._templates.replace_template_vars(sql)
        return self._docker_runner.run_sql(
            run_id,
            executable_sql,
            dataset,
            self._docker_event_log_dir(),
        )

    def run_pyspark(self, sql, dataset: DatasetProfile):
        try:
            spark = self._sessions.create("AGI-Gilgamesh-AgentRuntime")
            frame = self._read_frame(spark, dataset)
            if frame is None:
                return (
                    "dry_run",
                    f"数据格式 {dataset.format} 暂未自动执行，已保留提交配置。",
                    {"execution_mode": "dry_run", "spark_available": True},
                )
            frame.createOrReplaceTempView(dataset.name)
            preview = spark.sql(self._templates.replace_template_vars(sql)).limit(5).collect()
            return (
                "success",
                "分布式查询执行成功，已采集预览行。",
                {"execution_mode": "spark", "spark_available": True, "preview_rows": len(preview)},
            )
        except Exception as exc:  # pragma: no cover - depends on Spark runtime.
            return (
                "failed",
                f"{type(exc).__name__}: {exc}",
                {"execution_mode": "spark", "spark_available": True},
            )

    def _read_frame(self, spark, dataset):
        reader = spark.read
        fmt = dataset.format.lower()
        if fmt == "parquet":
            return reader.parquet(dataset.path)
        if fmt == "json":
            return reader.json(dataset.path)
        if fmt == "csv":
            return reader.option("header", True).csv(dataset.path)
        if fmt == "hive":
            return spark.table(dataset.path or dataset.name)
        return None

    def _docker_event_log_dir(self) -> str:
        return self._event_log_dir or ""
