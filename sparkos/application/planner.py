from __future__ import annotations

from typing import Iterable, List

from sparkos.domain.capability import DEFAULT_CAPABILITIES, Capability
from sparkos.domain.catalog import DatasetProfile
from sparkos.domain.plan import AnalysisPlan, AnalysisStep, PlanStatus
from sparkos.domain.problem import ProblemSpec, ProblemType


class AnalysisPlanner:
    def __init__(self, capabilities: Iterable[Capability] = DEFAULT_CAPABILITIES):
        self._capabilities = list(capabilities)

    def create_plan(
        self,
        problem: ProblemSpec,
        datasets: List[DatasetProfile],
    ) -> AnalysisPlan:
        if not problem.is_ready:
            return AnalysisPlan(
                problem=problem,
                status=PlanStatus.NEEDS_CONTEXT,
                datasets=datasets,
                user_visible_summary="需要补充业务上下文后才能开始分析。",
                risks=["用户问题中缺少必要的范围、实体或目标。"],
            )

        capabilities = self._match_capabilities(problem.problem_type)
        steps = [
            AnalysisStep(
                title=capability.label,
                rationale=capability.description,
                capability=capability.name,
                backend=capability.preferred_backend,
            )
            for capability in capabilities
        ]

        if not steps and problem.problem_type == ProblemType.UNKNOWN:
            return AnalysisPlan(
                problem=problem,
                status=PlanStatus.NEEDS_CONTEXT,
                datasets=datasets,
                user_visible_summary="我还不能稳定判断这个问题属于哪类数据任务。",
                risks=["任务类型未知，需要进一步澄清目标。"],
            )

        assumptions = []
        if datasets:
            assumptions.append("系统会优先使用数据目录中语义最相关的数据集。")
        else:
            assumptions.append("当前未发现匹配数据集，执行前需要接入或声明数据源。")

        return AnalysisPlan(
            problem=problem,
            status=PlanStatus.READY,
            datasets=datasets,
            steps=steps,
            assumptions=assumptions,
            risks=self._collect_risks(problem, datasets),
            user_visible_summary=self._summarize(problem, steps),
        )

    def _match_capabilities(self, problem_type: ProblemType) -> List[Capability]:
        return [
            capability
            for capability in self._capabilities
            if problem_type in capability.problem_types
        ]

    def _collect_risks(
        self,
        problem: ProblemSpec,
        datasets: List[DatasetProfile],
    ) -> List[str]:
        risks = []
        if problem.problem_type in {
            ProblemType.RELATIONSHIP_ANALYSIS,
            ProblemType.GRAPH_COMMUNITY,
            ProblemType.GRAPH_PATH,
            ProblemType.ANOMALY_DETECTION,
        }:
            has_entity = any(
                column.semantic_type == "entity"
                for dataset in datasets
                for column in dataset.columns
            )
            if not has_entity:
                risks.append("未在匹配数据集中发现明确的实体字段，图关系可能需要人工确认。")
        if not datasets:
            risks.append("没有匹配到可用数据集，计划只能停留在准备阶段。")
        return risks

    def _summarize(
        self,
        problem: ProblemSpec,
        steps: List[AnalysisStep],
    ) -> str:
        step_titles = "、".join(step.title for step in steps) or "上下文澄清"
        return f"已识别为「{problem.problem_type.value}」任务，将通过{step_titles}来解决。"
