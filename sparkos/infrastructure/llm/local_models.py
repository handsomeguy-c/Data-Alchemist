from __future__ import annotations

import re
from typing import Iterable, Optional

from sparkos.domain.plan import AnalysisPlan, PlanStatus
from sparkos.domain.problem import ProblemSpec, ProblemType
from sparkos.domain.result import ExecutionResult


class LocalModelGateway:
    """Deterministic role implementation used for offline development."""

    def plan_problem(
        self,
        user_request: str,
        catalog_context: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> ProblemSpec:
        text = user_request.lower()
        problem_type = self._classify(text)
        entities = self._extract_entities(user_request)
        missing_context = []

        if problem_type == ProblemType.UNKNOWN:
            missing_context.append("请说明要处理的数据对象、目标结果或判断标准。")
        if problem_type in {
            ProblemType.GRAPH_COMMUNITY,
            ProblemType.GRAPH_PATH,
            ProblemType.ANOMALY_DETECTION,
        } and not entities:
            missing_context.append("请说明图中的核心实体，例如用户、设备、IP、账户或订单。")
        if not catalog_context:
            missing_context.append("请先在数据目录中登记可用数据集。")

        return ProblemSpec(
            user_request=user_request,
            problem_type=problem_type,
            objective=self._objective(user_request),
            entities=entities,
            time_range=self._extract_time_range(user_request),
            missing_context=missing_context,
        )

    def review_plan(
        self,
        plan: AnalysisPlan,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> AnalysisPlan:
        if not plan.datasets and plan.status == PlanStatus.READY:
            return plan.model_copy(
                update={
                    "status": PlanStatus.NEEDS_CONTEXT,
                    "risks": [*plan.risks, "缺少可执行数据源。"],
                }
            )
        return plan

    def explain_result(
        self,
        result: ExecutionResult,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> str:
        if not result.artifacts:
            return "任务执行完成，但没有生成可展示的结果。"
        artifact_titles = "、".join(artifact.title for artifact in result.artifacts)
        return f"任务已完成，生成了{artifact_titles}。可以继续追问结果细节或调整分析范围。"

    def chat(
        self,
        user_input: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> str:
        return (
            "我在正常对话模式。\n"
            f"你刚才说：{user_input}\n"
            "如果要处理数据文件，请在消息里使用 @文件，并说明是训练数据处理还是向量知识库构建。"
        )

    def stream_chat(
        self,
        user_input: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> Iterable[str]:
        yield from self._stream_text(self.chat(user_input, model, temperature, options))

    def _classify(self, text: str) -> ProblemType:
        keyword_groups = [
            (ProblemType.ANOMALY_DETECTION, ["异常", "风险", "欺诈", "刷单", "可疑", "团伙"]),
            (ProblemType.GRAPH_COMMUNITY, ["社区", "群体", "团伙", "连通", "关系网络"]),
            (ProblemType.GRAPH_PATH, ["路径", "链路", "追踪", "几跳", "关联到"]),
            (ProblemType.RELATIONSHIP_ANALYSIS, ["关系", "网络", "关联", "共同设备", "共同ip"]),
            (ProblemType.DATA_QUALITY, ["质量", "缺失", "重复", "脏数据", "校验"]),
            (ProblemType.DATA_PROFILING, ["画像", "概览", "分布", "字段", "schema"]),
            (ProblemType.FEATURE_GENERATION, ["特征", "标签", "训练集", "样本"]),
            (ProblemType.DATA_TRANSFORMATION, ["清洗", "转换", "合并", "聚合", "join"]),
        ]
        for problem_type, keywords in keyword_groups:
            if any(keyword in text for keyword in keywords):
                return problem_type
        return ProblemType.UNKNOWN

    def _extract_entities(self, user_request: str) -> list[str]:
        entity_words = ["用户", "设备", "IP", "ip", "账户", "订单", "商户", "地址"]
        return [word for word in entity_words if word in user_request]

    def _extract_time_range(self, user_request: str) -> Optional[str]:
        patterns: Iterable[str] = [
            r"近\s*\d+\s*天",
            r"最近\s*\d+\s*天",
            r"近\s*\d+\s*周",
            r"最近\s*\d+\s*周",
            r"\d{4}-\d{2}-\d{2}\s*到\s*\d{4}-\d{2}-\d{2}",
        ]
        for pattern in patterns:
            match = re.search(pattern, user_request)
            if match:
                return match.group(0)
        return None

    def _objective(self, user_request: str) -> str:
        return user_request.strip(" 。\n\t")

    def _stream_text(self, text: str, chunk_size: int = 8) -> Iterable[str]:
        for index in range(0, len(text), chunk_size):
            yield text[index : index + chunk_size]
