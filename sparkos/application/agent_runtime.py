from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

from sparkos.application.ports import CatalogPort
from sparkos.application.skill_registry import SkillRegistry
from sparkos.application.spark_tools import SparkToolExecutor
from sparkos.domain.agent import (
    AgentPlan,
    AgentPlanStep,
    AgentRunResult,
    AgentRunStatus,
    AgentStepResult,
)


class AgentRuntime:
    def __init__(
        self,
        catalog: CatalogPort,
        skill_registry: SkillRegistry,
        artifact_root: Path,
        tool_executor: Optional[SparkToolExecutor] = None,
    ):
        self._catalog = catalog
        self._skill_registry = skill_registry
        self._tool_executor = tool_executor or SparkToolExecutor(artifact_root)

    def can_handle(self, user_input: str) -> bool:
        text = user_input.lower()
        return any(keyword in text for keyword in _AGENT_INTENT_KEYWORDS)

    def run(self, user_input: str) -> AgentRunResult:
        plan = self.plan(user_input)
        if not plan.steps:
            return AgentRunResult(
                status=AgentRunStatus.NEEDS_CONTEXT,
                plan=plan,
                warnings=["未匹配到可执行 skill，请补充 SQL、ETL、清洗、特征、质量、Job 或 DAG 诊断目标。"],
            )

        results = []
        warnings = list(plan.warnings)
        for step in plan.steps:
            try:
                results.append(self._tool_executor.execute(plan, step))
            except Exception as exc:
                warnings.append(f"{step.skill_name} 执行失败: {type(exc).__name__}: {exc}")
                return AgentRunResult(
                    status=AgentRunStatus.FAILED,
                    plan=plan,
                    results=results,
                    artifacts=self._artifact_paths(results),
                    warnings=warnings,
                )
        return AgentRunResult(
            status=AgentRunStatus.COMPLETED,
            plan=plan,
            results=results,
            artifacts=self._artifact_paths(results),
            warnings=warnings,
        )

    def stream(self, user_input: str) -> Iterable[str]:
        result = self.run(user_input)
        yield from self.format_stream(result)

    def format_stream(self, result: AgentRunResult) -> Iterable[str]:
        plan = result.plan
        yield "AGI Runtime 已接管这个数据工程任务。\n"
        yield f"意图: {plan.intent}\n"
        yield f"Run ID: {plan.id}\n"
        execution_mode = "分布式执行可用" if self._tool_executor.spark_available else "计划模式"
        yield f"执行模式: {execution_mode}\n"
        if plan.datasets:
            names = ", ".join(dataset.name for dataset in plan.datasets)
            yield f"匹配数据集: {names}\n"
        if plan.assumptions:
            yield "假设:\n"
            for assumption in plan.assumptions:
                yield f"- {assumption}\n"
        yield "\n执行步骤:\n"
        for step in plan.steps:
            yield f"- [{step.id}] {self._skill_label(step.skill_name)}: {step.objective}\n"
        if result.results:
            yield "\n结果:\n"
        for step_result in result.results:
            yield f"- {self._skill_label(step_result.skill_name)}: {step_result.summary}\n"
            if step_result.artifact_path:
                yield f"  artifact: {step_result.artifact_path}\n"
        if result.warnings:
            yield "\n注意:\n"
            for warning in result.warnings:
                yield f"- {warning}\n"
        yield f"\n状态: {result.status.value}\n"

    def plan(self, user_input: str) -> AgentPlan:
        selected_skills = self._select_skills(user_input)
        datasets = self._catalog.search(user_input)
        steps = self._build_steps(user_input, selected_skills)
        warnings = self._skill_warnings(selected_skills)
        assumptions = [
            "分布式计算引擎作为项目基建运行，用户侧不需要编写底层计算代码。",
            "执行前会先生成可审计查询、DAG、执行配置和诊断产物。",
        ]
        if not datasets:
            assumptions.append("catalog 未匹配到数据集时，会用 source_table 占位生成任务骨架。")
        return AgentPlan(
            user_request=user_input,
            intent=self._intent_label(user_input, selected_skills),
            datasets=datasets,
            steps=steps,
            assumptions=assumptions,
            warnings=warnings,
        )

    def _select_skills(self, user_input: str) -> List[str]:
        text = user_input.lower()
        selected = []
        if self._has_any(text, ["sql", "查询", "统计", "指标", "分析", "报表"]):
            selected.append("spark-sql")
        if self._has_any(text, ["etl", "流水线", "同步", "加工", "写入", "宽表", "合并"]):
            selected.append("etl-pipeline")
        if self._has_any(text, ["清洗", "缺失", "重复", "脏数据", "异常值", "标准化"]):
            selected.append("data-cleaning")
        if self._has_any(text, ["质量", "校验", "schema", "空值率", "唯一性", "分布"]):
            selected.append("data-quality")
        if self._has_any(text, ["特征", "训练样本", "样本表", "标签", "分桶", "编码"]):
            selected.append("feature-engineering")
        if self._has_any(text, ["job", "提交", "执行", "运行", "重试", "状态"]):
            selected.append("spark-job")
        if self._has_any(text, ["dag", "shuffle", "倾斜", "join", "oom", "长尾", "优化", "慢"]):
            selected.append("dag-diagnosis")
        if self._has_any(text, ["图", "关系网络", "社区", "路径", "团伙"]):
            selected.extend(
                ["spark-sql", "etl-pipeline", "graph-compute", "spark-job", "dag-diagnosis"]
            )
        if self._has_any(text, ["spark"]) and not selected:
            selected.extend(["spark-sql", "spark-job", "dag-diagnosis"])

        selected = self._dedupe(selected)
        if selected and "spark-job" not in selected:
            selected.append("spark-job")
        if selected and "dag-diagnosis" not in selected:
            selected.append("dag-diagnosis")
        return selected

    def _build_steps(self, user_input: str, selected_skills: List[str]) -> List[AgentPlanStep]:
        steps = []
        previous_id = None
        for index, skill_name in enumerate(selected_skills, start=1):
            step_id = f"s{index}"
            steps.append(
                AgentPlanStep(
                    id=step_id,
                    skill_name=skill_name,
                    tool_name=skill_name.replace("-", "_"),
                    objective=self._objective_for(skill_name, user_input),
                    depends_on=[previous_id] if previous_id else [],
                )
            )
            previous_id = step_id
        return steps

    def _skill_warnings(self, selected_skills: List[str]) -> List[str]:
        warnings = []
        if not self._skill_registry.list():
            warnings.append("未加载外部 skill 目录，已使用内置 Spark AgentOS skill 行为。")
            return warnings
        for skill_name in selected_skills:
            if skill_name in _BUILTIN_EXTENSION_SKILLS:
                continue
            if not self._skill_registry.has(skill_name):
                warnings.append(f"外部 skill 缺失: {skill_name}，已使用内置 fallback。")
        return warnings

    def _objective_for(self, skill_name: str, user_input: str) -> str:
        objectives = {
            "spark-sql": "把自然语言需求转换为分布式查询计划，并进行风险检查。",
            "etl-pipeline": "拆解读取、转换、Join、聚合和写入步骤，形成可观测 DAG。",
            "data-cleaning": "生成缺失值、重复值、异常值和字段标准化清洗规则。",
            "data-quality": "生成 schema、空值率、唯一性、范围和分区完整性检查。",
            "feature-engineering": "生成特征字典、窗口特征和训练样本表加工逻辑。",
            "spark-job": "生成分布式执行配置，检测运行时可用性并尝试执行。",
            "dag-diagnosis": "分析 Shuffle、数据倾斜、Join 策略、OOM 和长尾风险。",
            "graph-compute": "执行关系网络算法，生成社区、路径或关键节点结果。",
        }
        return objectives.get(skill_name, user_input)

    def _skill_label(self, skill_name: str) -> str:
        labels = {
            "spark-sql": "分布式查询",
            "etl-pipeline": "ETL 编排",
            "data-cleaning": "数据清洗",
            "data-quality": "质量检查",
            "feature-engineering": "特征工程",
            "spark-job": "分布式执行",
            "dag-diagnosis": "性能诊断",
            "graph-compute": "图计算",
        }
        return labels.get(skill_name, skill_name)

    def _intent_label(self, user_input: str, selected_skills: List[str]) -> str:
        if not selected_skills:
            return "unknown"
        if "feature-engineering" in selected_skills:
            return "feature_engineering"
        if "etl-pipeline" in selected_skills:
            return "etl_pipeline"
        if "data-cleaning" in selected_skills:
            return "data_cleaning"
        if "data-quality" in selected_skills:
            return "data_quality"
        if "dag-diagnosis" in selected_skills and "spark-sql" not in selected_skills:
            return "dag_diagnosis"
        return "spark_job"

    def _artifact_paths(self, results: List[AgentStepResult]) -> List[str]:
        return [
            result.artifact_path
            for result in results
            if result.artifact_path is not None
        ]

    def _has_any(self, text: str, keywords: list[str]) -> bool:
        return any(keyword in text for keyword in keywords)

    def _dedupe(self, values: List[str]) -> List[str]:
        seen = set()
        result = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result


_AGENT_INTENT_KEYWORDS = [
    "spark",
    "sql",
    "统计",
    "指标",
    "报表",
    "查询",
    "etl",
    "dag",
    "shuffle",
    "join",
    "oom",
    "数据",
    "表",
    "字段",
    "清洗",
    "质量",
    "特征",
    "样本",
    "向量",
    "图计算",
    "关系网络",
    "社区发现",
    "job",
    "宽表",
    "分区",
]

_BUILTIN_EXTENSION_SKILLS = {"graph-compute"}
