from __future__ import annotations

from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.result import ExecutionResult, ExecutionState, ResultArtifact


class SimulatedBackend:
    def execute(
        self,
        plan: AnalysisPlan,
        requested_backend: str = "local",
    ) -> ExecutionResult:
        dataset_names = [dataset.name for dataset in plan.datasets]
        capabilities = [step.capability for step in plan.steps]

        artifact = ResultArtifact(
            title="分析结果预览",
            summary="已生成预览级结果，用于验证分析链路；接入生产数据后会输出实际结果。",
            rows_preview=[
                {
                    "group_id": "G-001",
                    "risk_score": 0.91,
                    "evidence": "多个用户共享设备和 IP，且时间窗口高度重叠。",
                },
                {
                    "group_id": "G-002",
                    "risk_score": 0.78,
                    "evidence": "交易金额分布异常，关联账户集中出现。",
                },
            ],
            metrics={
                "datasets": dataset_names,
                "capabilities": capabilities,
                "compute_mode": self._display_compute_mode(requested_backend),
                "execution_mode": "simulated",
            },
            evidence=[
                "已完成数据目录匹配。",
                "已生成用户可审阅的分析步骤。",
                "生产计算环境可用时会自动切换到真实执行。",
            ],
            next_actions=[
                "接入真实数据目录。",
                "配置 Spark 运行环境。",
                "为高风险群体增加人工复核规则。",
            ],
        )

        return ExecutionResult(
            state=ExecutionState.COMPLETED,
            plan_id=plan.id,
            backend="simulated",
            artifacts=[artifact],
            logs=[
                "Plan accepted.",
                f"Requested backend: {requested_backend}.",
                "Generated deterministic preview artifact.",
            ],
        )

    def _display_compute_mode(self, requested_backend: str) -> str:
        if requested_backend == "spark":
            return "distributed"
        return requested_backend
