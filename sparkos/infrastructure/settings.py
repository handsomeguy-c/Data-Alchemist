from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field

from sparkos.domain.catalog import DatasetProfile


class ModelRoleConfig(BaseModel):
    provider: str
    model: str
    temperature: float = 0.2
    thinking_enabled: bool = False
    reasoning_effort: Optional[str] = None
    stream: bool = False


class ProviderSettings(BaseModel):
    kind: str
    base_url: Optional[str] = None
    chat_path: str = "/chat/completions"
    api_key_env: Optional[str] = None
    api_key: Optional[str] = None
    timeout_seconds: int = 60


class ModelsSettings(BaseModel):
    default_provider: str = "local"
    default_model: str = "local-rule-planner"
    fallback_provider: Optional[str] = "local"
    roles: Dict[str, ModelRoleConfig] = Field(default_factory=dict)
    providers: Dict[str, ProviderSettings] = Field(default_factory=dict)

    def role(self, name: str) -> ModelRoleConfig:
        return self.roles.get(
            name,
            ModelRoleConfig(
                provider=self.default_provider,
                model=self.default_model,
            ),
        )


class CatalogSettings(BaseModel):
    datasets: List[DatasetProfile] = Field(default_factory=list)


class AgentSettings(BaseModel):
    enabled: bool = True
    skills_path: Optional[Path] = None


class RuntimeSettings(BaseModel):
    job_store_path: Path = Path("artifacts/runtime/jobs.sqlite3")
    artifact_root: Path = Path("artifacts")
    livy_url: Optional[str] = None
    history_server_url: Optional[str] = None
    spark_master_url: Optional[str] = None
    spark_event_log_dir: Optional[str] = None
    spark_driver_host: Optional[str] = None
    spark_driver_port: Optional[int] = None
    docker_spark_container: Optional[str] = None
    docker_spark_master_url: Optional[str] = None


class EmbeddingSettings(BaseModel):
    provider: str = "local"
    model: str = "local-hash-embedding"
    api_key_env: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    dimension: int = 16


class VectorStoreSettings(BaseModel):
    provider: str = "local-jsonl"
    collection: str = "agi_gilgamesh_kb"
    path: Path = Path("artifacts/vector-store")
    url: Optional[str] = None
    api_key_env: Optional[str] = None
    api_key: Optional[str] = None


class Settings(BaseModel):
    models: ModelsSettings
    catalog: CatalogSettings = Field(default_factory=CatalogSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)
    embedding: EmbeddingSettings = Field(default_factory=EmbeddingSettings)
    vector_store: VectorStoreSettings = Field(default_factory=VectorStoreSettings)


def load_settings(config_path: Path) -> Settings:
    resolved_path = _resolve_config_path(config_path)
    if not resolved_path.exists():
        return Settings(models=ModelsSettings())

    with resolved_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    raw = _expand_config_vars(raw, resolved_path.parent)
    raw = _normalize_user_friendly_model_config(raw)
    settings = Settings.model_validate(raw)
    return _apply_environment_overrides(settings)


def _resolve_config_path(config_path: Path) -> Path:
    if config_path.is_dir():
        return config_path / "config.yaml"
    return config_path


def _expand_config_vars(value, config_dir: Path):
    variables = {
        "SPARKOS_REPO_ROOT": str(config_dir.parent.resolve()),
        "SPARKOS_CONFIG_DIR": str(config_dir.resolve()),
    }
    if isinstance(value, dict):
        return {key: _expand_config_vars(item, config_dir) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_config_vars(item, config_dir) for item in value]
    if isinstance(value, str):
        expanded = value
        for name, replacement in variables.items():
            expanded = expanded.replace(f"${{{name}}}", replacement)
        return os.path.expandvars(expanded)
    return value


def _normalize_user_friendly_model_config(raw: dict) -> dict:
    normalized = dict(raw)
    if "models" not in normalized and "master-model" not in normalized:
        normalized["models"] = ModelsSettings().model_dump()
    master = normalized.get("master-model")
    if isinstance(master, dict):
        model_name = _non_empty(master.get("model"))
        base_url = _non_empty(master.get("url"))
        api_key = _non_empty(master.get("api-key"))
        if not (model_name and base_url and api_key):
            normalized["models"] = ModelsSettings().model_dump()
            return _normalize_embedding_config(normalized)
        provider_name = "master-model"
        base_url, chat_path = _split_chat_url(base_url)
        normalized["models"] = {
            "default_provider": provider_name,
            "default_model": model_name,
            "fallback_provider": "local",
            "providers": {
                "local": {"kind": "deterministic"},
                provider_name: {
                    "kind": "openai-compatible",
                    "base_url": base_url,
                    "chat_path": master.get("chat_path") or chat_path,
                    "api_key": api_key,
                    "timeout_seconds": master.get("timeout_seconds", 60),
                },
            },
            "roles": {
                "planner": _role(provider_name, model_name, 0.2, False),
                "critic": _role(provider_name, model_name, 0.0, False),
                "explainer": _role(provider_name, model_name, 0.2, False),
                "chat": _role(provider_name, model_name, 0.4, True),
            },
        }
    return _normalize_embedding_config(normalized)


def _normalize_embedding_config(normalized: dict) -> dict:
    embedding = normalized.get("embedding-model")
    if isinstance(embedding, dict):
        model_name = _non_empty(embedding.get("model"))
        base_url = _non_empty(embedding.get("url"))
        api_key = _non_empty(embedding.get("api-key"))
        if not (model_name and base_url and api_key):
            normalized["embedding"] = EmbeddingSettings().model_dump()
            return normalized
        normalized["embedding"] = {
            "provider": "embedding-model",
            "model": model_name,
            "base_url": base_url,
            "api_key": api_key,
            "dimension": embedding.get("dimension", 16),
        }
    return normalized


def _non_empty(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _split_chat_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    suffix = "/chat/completions"
    if not path.endswith(suffix):
        return url.rstrip("/"), suffix
    base_path = path[: -len(suffix)].rstrip("/")
    base_url = f"{parsed.scheme}://{parsed.netloc}{base_path}"
    return base_url.rstrip("/"), suffix


def _role(provider: str, model: str, temperature: float, stream: bool) -> dict:
    return {
        "provider": provider,
        "model": model,
        "temperature": temperature,
        "thinking_enabled": False,
        "reasoning_effort": None,
        "stream": stream,
    }


def _apply_environment_overrides(settings: Settings) -> Settings:
    runtime_update = {}
    if os.environ.get("SPARKOS_ARTIFACT_ROOT"):
        runtime_update["artifact_root"] = Path(os.environ["SPARKOS_ARTIFACT_ROOT"])
    if os.environ.get("SPARKOS_JOB_STORE_PATH"):
        runtime_update["job_store_path"] = Path(os.environ["SPARKOS_JOB_STORE_PATH"])
    if os.environ.get("SPARKOS_LIVY_URL"):
        runtime_update["livy_url"] = os.environ["SPARKOS_LIVY_URL"]
    if os.environ.get("SPARKOS_HISTORY_SERVER_URL"):
        runtime_update["history_server_url"] = os.environ["SPARKOS_HISTORY_SERVER_URL"]
    if os.environ.get("SPARKOS_SPARK_MASTER_URL"):
        runtime_update["spark_master_url"] = os.environ["SPARKOS_SPARK_MASTER_URL"]
    if os.environ.get("SPARKOS_SPARK_EVENT_LOG_DIR"):
        runtime_update["spark_event_log_dir"] = os.environ["SPARKOS_SPARK_EVENT_LOG_DIR"]
    if os.environ.get("SPARKOS_SPARK_DRIVER_HOST"):
        runtime_update["spark_driver_host"] = os.environ["SPARKOS_SPARK_DRIVER_HOST"]
    if os.environ.get("SPARKOS_SPARK_DRIVER_PORT"):
        runtime_update["spark_driver_port"] = int(os.environ["SPARKOS_SPARK_DRIVER_PORT"])
    if os.environ.get("SPARKOS_DOCKER_SPARK_CONTAINER"):
        runtime_update["docker_spark_container"] = os.environ["SPARKOS_DOCKER_SPARK_CONTAINER"]
    if os.environ.get("SPARKOS_DOCKER_SPARK_MASTER_URL"):
        runtime_update["docker_spark_master_url"] = os.environ["SPARKOS_DOCKER_SPARK_MASTER_URL"]

    agent_update = {}
    if os.environ.get("SPARKOS_SKILLS_PATH"):
        agent_update["skills_path"] = Path(os.environ["SPARKOS_SKILLS_PATH"])

    updates = {}
    if runtime_update:
        updates["runtime"] = settings.runtime.model_copy(update=runtime_update)
    if agent_update:
        updates["agent"] = settings.agent.model_copy(update=agent_update)
    if not updates:
        return settings
    return settings.model_copy(update=updates)
