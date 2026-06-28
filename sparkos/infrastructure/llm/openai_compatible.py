from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Iterable, Optional

from sparkos.domain.plan import AnalysisPlan, PlanStatus
from sparkos.domain.problem import ProblemSpec, ProblemType
from sparkos.domain.result import ExecutionResult
from sparkos.infrastructure.settings import ProviderSettings


class OpenAICompatibleGateway:
    def __init__(self, provider: ProviderSettings):
        self._provider = provider
        self.used_tokens = 0

    def plan_problem(
        self,
        user_request: str,
        catalog_context: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> ProblemSpec:
        system = (
            "你是 AGI-吉尔伽美什 的问题理解代理。系统面向大数据处理和图计算任务，"
            "但用户不应该感知 Spark。只返回 JSON，不要使用 Markdown。"
        )
        user = {
            "user_request": user_request,
            "catalog_context": catalog_context,
            "allowed_problem_types": [item.value for item in ProblemType],
            "schema": {
                "user_request": "string",
                "problem_type": "one allowed_problem_types value",
                "objective": "string",
                "entities": ["string"],
                "time_range": "string or null",
                "constraints": ["string"],
                "missing_context": ["string"],
            },
        }
        content = self._chat(
            model=model,
            temperature=temperature,
            options=options,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
        )
        return ProblemSpec.model_validate(self._json_object(content))

    def review_plan(
        self,
        plan: AnalysisPlan,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> AnalysisPlan:
        system = (
            "你是 AGI-吉尔伽美什 的计划审阅代理。检查计划是否能解决用户问题、是否缺少上下文、"
            "是否隐藏了计算引擎细节。只返回 JSON，不要使用 Markdown。"
        )
        payload = {
            "plan": plan.model_dump(mode="json"),
            "schema": {
                "status": "ready | needs_context | rejected",
                "user_visible_summary": "string",
                "assumptions": ["string"],
                "risks": ["string"],
            },
        }
        content = self._chat(
            model=model,
            temperature=temperature,
            options=options,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
        )
        review = self._json_object(content)
        update = {}
        if review.get("status"):
            update["status"] = PlanStatus(review["status"])
        for field in ["user_visible_summary", "assumptions", "risks"]:
            if field in review:
                update[field] = review[field]
        return plan.model_copy(update=update)

    def explain_result(
        self,
        result: ExecutionResult,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> str:
        system = (
            "你是 AGI-吉尔伽美什 的结果解释代理。用简洁中文总结结果、证据和下一步，"
            "不要提 Spark、DataFrame、shuffle 等底层实现。"
        )
        content = self._chat(
            model=model,
            temperature=temperature,
            options=options,
            messages=[
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(result.model_dump(mode="json"), ensure_ascii=False),
                },
            ],
        )
        return content.strip()

    def chat(
        self,
        user_input: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> str:
        system = (
            "你是 AGI-吉尔伽美什。默认进行正常对话。"
            "只有用户显式 @ 文件时，系统才进入数据工程任务处理模式。"
        )
        return self._chat(
            model=model,
            temperature=temperature,
            options=options,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_input},
            ],
        ).strip()

    def stream_chat(
        self,
        user_input: str,
        model: str,
        temperature: float,
        options: Optional[dict] = None,
    ) -> Iterable[str]:
        system = (
            "你是 AGI-吉尔伽美什。默认进行正常对话。"
            "只有用户显式 @ 文件时，系统才进入数据工程任务处理模式。"
        )
        yield from self._stream_chat(
            model=model,
            temperature=temperature,
            options=options,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_input},
            ],
        )

    def _chat(
        self,
        model: str,
        temperature: float,
        options: Optional[dict],
        messages: list[dict[str, str]],
    ) -> str:
        api_key = self._api_key()
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": bool((options or {}).get("stream", False)),
        }
        if (options or {}).get("thinking_enabled"):
            payload["thinking"] = {"type": "enabled"}
        if (options or {}).get("reasoning_effort"):
            payload["reasoning_effort"] = (options or {})["reasoning_effort"]

        request = urllib.request.Request(
            self._url(),
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._provider.timeout_seconds,
            ) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"LLM request failed: {exc.code} {detail}") from exc

        data = json.loads(raw)
        usage = data.get("usage") or {}
        self.used_tokens += int(usage.get("total_tokens") or 0)
        return data["choices"][0]["message"]["content"]

    def _stream_chat(
        self,
        model: str,
        temperature: float,
        options: Optional[dict],
        messages: list[dict[str, str]],
    ) -> Iterable[str]:
        api_key = self._api_key()
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }
        if (options or {}).get("thinking_enabled"):
            payload["thinking"] = {"type": "enabled"}
        if (options or {}).get("reasoning_effort"):
            payload["reasoning_effort"] = (options or {})["reasoning_effort"]

        request = urllib.request.Request(
            self._url(),
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "Accept": "text/event-stream",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._provider.timeout_seconds,
            ) as response:
                for raw_line in response:
                    line = raw_line.decode("utf-8", errors="replace").strip()
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[len("data:") :].strip()
                    if data == "[DONE]":
                        break
                    chunk = self._parse_sse_chunk(data)
                    if chunk:
                        yield chunk
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"LLM stream failed: {exc.code} {detail}") from exc

    def _parse_sse_chunk(self, data: str) -> str:
        payload = json.loads(data)
        usage = payload.get("usage") or {}
        self.used_tokens += int(usage.get("total_tokens") or 0)
        choices = payload.get("choices") or []
        if not choices:
            return ""
        delta = choices[0].get("delta") or {}
        return delta.get("content") or ""

    def _api_key(self) -> str:
        if self._provider.api_key:
            return self._provider.api_key
        if self._provider.api_key_env:
            if self._provider.api_key_env.startswith("sk-"):
                return self._provider.api_key_env
            value = os.environ.get(self._provider.api_key_env)
            if value:
                return value
        raise RuntimeError(
            "Missing LLM API key. Fill master-model.api-key in config/config.yaml."
        )

    def _url(self) -> str:
        if not self._provider.base_url:
            raise RuntimeError("OpenAI-compatible provider requires base_url.")
        return self._provider.base_url.rstrip("/") + self._provider.chat_path

    def _json_object(self, content: str) -> dict[str, Any]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.S)
            if not match:
                raise
            data = json.loads(match.group(0))
        if not isinstance(data, dict):
            raise ValueError("Expected a JSON object from model response.")
        return data
