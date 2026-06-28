from __future__ import annotations

from typing import Dict, Iterable, Optional

from sparkos.domain.plan import AnalysisPlan
from sparkos.domain.problem import ProblemSpec
from sparkos.domain.result import ExecutionResult
from sparkos.infrastructure.llm.base import ModelGateway
from sparkos.infrastructure.settings import ModelRoleConfig, ModelsSettings


class ModelRouter:
    def __init__(
        self,
        settings: ModelsSettings,
        gateways: Dict[str, ModelGateway],
    ):
        self._settings = settings
        self._gateways = gateways
        self._last_error: Optional[Exception] = None

    @property
    def model_name(self) -> str:
        return self._settings.role("planner").model

    @property
    def used_tokens(self) -> int:
        return sum(
            int(getattr(gateway, "used_tokens", 0))
            for gateway in self._gateways.values()
        )

    @property
    def last_model_error(self) -> Optional[str]:
        if self._last_error is None:
            return None
        return f"{type(self._last_error).__name__}: {self._last_error}"

    def clear_model_error(self) -> None:
        self._last_error = None

    def plan_problem(self, user_request: str, catalog_context: str) -> ProblemSpec:
        role = self._settings.role("planner")
        return self._call_with_fallback(
            role,
            lambda gateway, active_role: gateway.plan_problem(
                user_request=user_request,
                catalog_context=catalog_context,
                model=active_role.model,
                temperature=active_role.temperature,
                options=self._options(active_role),
            ),
        )

    def review_plan(self, plan: AnalysisPlan) -> AnalysisPlan:
        role = self._settings.role("critic")
        return self._call_with_fallback(
            role,
            lambda gateway, active_role: gateway.review_plan(
                plan=plan,
                model=active_role.model,
                temperature=active_role.temperature,
                options=self._options(active_role),
            ),
        )

    def explain_result(self, result: ExecutionResult) -> str:
        role = self._settings.role("explainer")
        return self._call_with_fallback(
            role,
            lambda gateway, active_role: gateway.explain_result(
                result=result,
                model=active_role.model,
                temperature=active_role.temperature,
                options=self._options(active_role),
            ),
        )

    def chat(self, user_input: str) -> str:
        role = self._settings.role("chat")
        return self._call_with_fallback(
            role,
            lambda gateway, active_role: gateway.chat(
                user_input=user_input,
                model=active_role.model,
                temperature=active_role.temperature,
                options=self._options(active_role),
            ),
        )

    def stream_chat(self, user_input: str) -> Iterable[str]:
        role = self._settings.role("chat")
        return self._stream_with_fallback(
            role,
            lambda gateway, active_role: gateway.stream_chat(
                user_input=user_input,
                model=active_role.model,
                temperature=active_role.temperature,
                options=self._options(active_role),
            ),
        )

    def _gateway(self, provider: str) -> ModelGateway:
        try:
            return self._gateways[provider]
        except KeyError as exc:
            known = ", ".join(sorted(self._gateways))
            raise ValueError(f"Unknown model provider '{provider}'. Known: {known}") from exc

    def _call_with_fallback(self, role: ModelRoleConfig, operation):
        try:
            result = operation(self._gateway(role.provider), role)
            self._last_error = None
            return result
        except Exception as exc:
            self._last_error = exc
            fallback_provider = self._settings.fallback_provider
            if not fallback_provider or fallback_provider == role.provider:
                raise
            fallback_role = ModelRoleConfig(
                provider=fallback_provider,
                model=self._settings.default_model,
                temperature=role.temperature,
            )
            return operation(self._gateway(fallback_provider), fallback_role)

    def _stream_with_fallback(self, role: ModelRoleConfig, operation) -> Iterable[str]:
        try:
            for chunk in operation(self._gateway(role.provider), role):
                yield chunk
            self._last_error = None
        except Exception as exc:
            self._last_error = exc
            fallback_provider = self._settings.fallback_provider
            if not fallback_provider or fallback_provider == role.provider:
                raise
            fallback_role = ModelRoleConfig(
                provider=fallback_provider,
                model=self._settings.default_model,
                temperature=role.temperature,
            )
            for chunk in operation(self._gateway(fallback_provider), fallback_role):
                yield chunk

    def _options(self, role: ModelRoleConfig) -> dict:
        return {
            "thinking_enabled": role.thinking_enabled,
            "reasoning_effort": role.reasoning_effort,
            "stream": role.stream,
        }
