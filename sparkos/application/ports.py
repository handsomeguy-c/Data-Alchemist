from __future__ import annotations

from typing import Iterable, Optional, Protocol

from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.problem import ProblemSpec
from sparkos.domain.result import ExecutionResult


class CatalogPort(Protocol):
    def search(self, query: str, limit: int = 5):
        """Return dataset profiles relevant to a business problem."""


class ModelRouterPort(Protocol):
    @property
    def model_name(self) -> str:
        """Return the configured primary model name for display."""

    @property
    def used_tokens(self) -> int:
        """Return total model tokens used in this process."""

    @property
    def last_model_error(self) -> Optional[str]:
        """Return the last primary model error when fallback was used."""

    def clear_model_error(self) -> None:
        """Clear model error state after non-model work."""

    def plan_problem(self, user_request: str, catalog_context: str) -> ProblemSpec:
        """Create a structured problem spec from user language."""

    def review_plan(self, plan: AnalysisPlan) -> AnalysisPlan:
        """Review a plan before it can execute."""

    def explain_result(self, result: ExecutionResult) -> str:
        """Create a concise user-facing result explanation."""

    def chat(self, user_input: str) -> str:
        """Return a normal conversational response."""

    def stream_chat(self, user_input: str) -> Iterable[str]:
        """Return a normal conversational response as text chunks."""


class ComputeRouterPort(Protocol):
    def execute(self, plan: AnalysisPlan) -> ExecutionResult:
        """Run a reviewed plan on the selected backend."""
