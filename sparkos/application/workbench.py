from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from sparkos.application.agent_runtime import AgentRuntime
from sparkos.application.file_tasks import FileTaskService
from sparkos.application.file_task_engines import FileTaskEngine
from sparkos.application.planner import AnalysisPlanner
from sparkos.application.ports import CatalogPort, ComputeRouterPort, ModelRouterPort
from sparkos.application.vector_kb_runtime import VectorKbRuntime
from sparkos.application.turn_router import TurnRouter
from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.result import ExecutionResult
from sparkos.domain.turn import TurnMode, TurnResponse


@dataclass
class WorkbenchTurn:
    plan: AnalysisPlan
    result: Optional[ExecutionResult] = None
    message: str = ""


class WorkbenchService:
    def __init__(
        self,
        catalog: CatalogPort,
        model_router: ModelRouterPort,
        compute_router: ComputeRouterPort,
        workspace_root: Path,
        agent_runtime: Optional[AgentRuntime] = None,
        artifact_root: Optional[Path] = None,
        vector_runtime: Optional[VectorKbRuntime] = None,
        spark_file_engine: Optional[FileTaskEngine] = None,
    ):
        self._catalog = catalog
        self._model_router = model_router
        self._compute_router = compute_router
        self._planner = AnalysisPlanner()
        self._turn_router = TurnRouter(workspace_root)
        self._file_tasks = FileTaskService(
            artifact_root or workspace_root / "artifacts",
            spark_engine=spark_file_engine,
            vector_runtime=vector_runtime,
        )
        self._agent_runtime = agent_runtime

    @property
    def model_name(self) -> str:
        return self._model_router.model_name

    @property
    def used_tokens(self) -> int:
        return self._model_router.used_tokens

    @property
    def last_model_error(self) -> Optional[str]:
        return self._model_router.last_model_error

    def prepare(self, user_request: str) -> AnalysisPlan:
        datasets = self._catalog.search(user_request)
        catalog_context = self._catalog_context(datasets)
        problem = self._model_router.plan_problem(user_request, catalog_context)
        plan = self._planner.create_plan(problem, datasets)
        return self._model_router.review_plan(plan)

    def handle_input(self, user_input: str) -> TurnResponse:
        mode, task_request = self._turn_router.route(user_input)
        if mode == TurnMode.TASK and task_request is not None:
            self._model_router.clear_model_error()
            return self._file_tasks.run(task_request)

        if self._agent_runtime and self._agent_runtime.can_handle(user_input):
            self._model_router.clear_model_error()
            result = self._agent_runtime.run(user_input)
            return TurnResponse(
                mode=TurnMode.TASK,
                message="".join(self._agent_runtime.format_stream(result)),
                artifacts=result.artifacts,
                warnings=result.warnings,
            )

        return TurnResponse(
            mode=TurnMode.CHAT,
            message=self._chat(user_input),
        )

    def stream_input(self, user_input: str) -> Iterable[str]:
        mode, task_request = self._turn_router.route(user_input)
        if mode == TurnMode.TASK and task_request is not None:
            self._model_router.clear_model_error()
            response = self._file_tasks.run(task_request)
            yield from self._stream_text(self.format_turn_response(response))
            return

        if self._agent_runtime and self._agent_runtime.can_handle(user_input):
            self._model_router.clear_model_error()
            yield from self._agent_runtime.stream(user_input)
            return

        yield from self._model_router.stream_chat(user_input)

    def format_turn_response(self, response: TurnResponse) -> str:
        if response.mode == TurnMode.CHAT:
            return response.message

        lines = [
            response.message,
            "",
            f"任务类型: {response.task_type.value if response.task_type else 'unknown'}",
        ]
        if response.files:
            lines.append("")
            lines.append("输入文件:")
            lines.extend(
                f"  {file.raw}  {'OK' if file.exists else 'MISSING'}"
                for file in response.files
            )
        if response.artifacts:
            lines.append("")
            lines.append("计划产物:")
            lines.extend(f"  {artifact}" for artifact in response.artifacts)
        if response.warnings:
            lines.append("")
            lines.append("注意:")
            lines.extend(f"  {warning}" for warning in response.warnings)
        return "\n".join(lines)

    def execute(self, plan: AnalysisPlan) -> WorkbenchTurn:
        if not plan.can_execute:
            return WorkbenchTurn(
                plan=plan,
                message="这个计划还不能执行，需要先补充上下文。",
            )

        result = self._compute_router.execute(plan)
        explanation = self._model_router.explain_result(result)
        return WorkbenchTurn(plan=plan, result=result, message=explanation)

    def solve(self, user_request: str, auto_execute: bool = False) -> WorkbenchTurn:
        plan = self.prepare(user_request)
        if auto_execute:
            return self.execute(plan)
        return WorkbenchTurn(plan=plan, message=plan.user_visible_summary)

    def _chat(self, user_input: str) -> str:
        return self._model_router.chat(user_input)

    def _stream_text(self, text: str, chunk_size: int = 16) -> Iterable[str]:
        for index in range(0, len(text), chunk_size):
            yield text[index : index + chunk_size]

    def _catalog_context(self, datasets: List[object]) -> str:
        lines = []
        for dataset in datasets:
            columns = ", ".join(column.name for column in dataset.columns)
            lines.append(f"- {dataset.name}: {dataset.description}; columns: {columns}")
        return "\n".join(lines)
