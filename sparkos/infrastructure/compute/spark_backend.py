from __future__ import annotations

from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.result import ExecutionResult, ExecutionState, ResultArtifact


class SparkBackend:
    def __init__(self):
        self._pyspark = None
        try:
            import pyspark.sql  # type: ignore

            self._pyspark = pyspark.sql
        except ModuleNotFoundError:
            self._pyspark = None

    def is_available(self) -> bool:
        return self._pyspark is not None

    def execute(self, plan: AnalysisPlan) -> ExecutionResult:
        artifact = ResultArtifact(
            title="Spark 执行占位结果",
            summary="已检测到 PySpark。当前版本保留后端接口，具体能力执行器将在 capability 层逐步接入。",
            metrics={
                "steps": [step.capability for step in plan.steps],
                "datasets": [dataset.name for dataset in plan.datasets],
            },
            next_actions=[
                "实现 capability 到 DataFrame/GraphFrame 作业的映射。",
                "接入作业事件流和产物存储。",
            ],
        )
        return ExecutionResult(
            state=ExecutionState.COMPLETED,
            plan_id=plan.id,
            backend="spark",
            artifacts=[artifact],
            logs=["PySpark is available.", "Spark backend interface executed."],
        )
