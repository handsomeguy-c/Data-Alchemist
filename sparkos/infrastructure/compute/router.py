from __future__ import annotations

from sparkos.domain.capability import ExecutionBackend
from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.result import ExecutionResult
from sparkos.infrastructure.compute.simulated import SimulatedBackend
from sparkos.infrastructure.compute.spark_backend import SparkBackend


class ComputeRouter:
    def __init__(self):
        self._simulated = SimulatedBackend()
        self._spark = SparkBackend()

    def execute(self, plan: AnalysisPlan) -> ExecutionResult:
        backend = self._select_backend(plan)
        if backend == ExecutionBackend.SPARK and self._spark.is_available():
            return self._spark.execute(plan)
        return self._simulated.execute(plan, requested_backend=backend.value)

    def _select_backend(self, plan: AnalysisPlan) -> ExecutionBackend:
        if not plan.steps:
            return ExecutionBackend.LOCAL
        if any(step.backend == ExecutionBackend.SPARK for step in plan.steps):
            return ExecutionBackend.SPARK
        return plan.steps[0].backend
