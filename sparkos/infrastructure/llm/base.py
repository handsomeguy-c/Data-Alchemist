from __future__ import annotations

from typing import Iterable, Optional, Protocol

from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.problem import ProblemSpec
from sparkos.domain.result import ExecutionResult


class ModelGateway(Protocol):
    def plan_problem(
        self,
        user_request: str,
        catalog_context: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> ProblemSpec:
        """Create a problem spec."""

    def review_plan(
        self,
        plan: AnalysisPlan,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> AnalysisPlan:
        """Review and optionally adjust a plan."""

    def explain_result(
        self,
        result: ExecutionResult,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> str:
        """Explain execution results."""

    def chat(
        self,
        user_input: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> str:
        """Return a normal chat response."""

    def stream_chat(
        self,
        user_input: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> Iterable[str]:
        """Return a normal chat response as text chunks."""
