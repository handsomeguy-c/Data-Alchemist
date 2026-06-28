#!/usr/bin/env python3
from __future__ import annotations

import shlex
import sys
import os
from pathlib import Path

import yaml


def main() -> int:
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("config/config.yaml")
    if config_path.is_dir():
        config_path = config_path / "config.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    raw = _expand_config_vars(raw, config_path.parent)

    exports: dict[str, str] = {
        "SPARKOS_CONFIG": str(config_path),
    }
    _collect_simple_model("master-model", raw.get("master-model", {}), exports)
    _collect_simple_model("embedding-model", raw.get("embedding-model", {}), exports)
    _collect_model_keys(raw.get("models", {}), exports)
    _collect_named_secret(raw.get("embedding", {}), exports)
    _collect_named_secret(raw.get("vector_store", {}), exports)
    _collect_runtime(raw.get("runtime", {}), exports)
    _collect_agent(raw.get("agent", {}), exports)

    for key in sorted(exports):
        value = exports[key]
        if value is None or value == "":
            continue
        print(f"export {key}={shlex.quote(str(value))}")
    return 0


def _collect_model_keys(models: dict, exports: dict[str, str]) -> None:
    for provider in (models.get("providers") or {}).values():
        _collect_named_secret(provider, exports)


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


def _collect_simple_model(name: str, section: dict, exports: dict[str, str]) -> None:
    if not isinstance(section, dict):
        return
    prefix = name.upper().replace("-", "_")
    mapping = {
        "model": f"{prefix}_NAME",
        "url": f"{prefix}_URL",
        "api-key": f"{prefix}_API_KEY",
    }
    for field, env_name in mapping.items():
        value = section.get(field)
        if value:
            exports[env_name] = str(value)


def _collect_named_secret(section: dict, exports: dict[str, str]) -> None:
    env_name = section.get("api_key_env")
    api_key = section.get("api_key")
    if env_name and api_key:
        exports[str(env_name)] = str(api_key)
    if section.get("base_url"):
        key = _section_env_name(section, "BASE_URL")
        exports[key] = str(section["base_url"])
    if section.get("url"):
        key = _section_env_name(section, "URL")
        exports[key] = str(section["url"])


def _collect_runtime(runtime: dict, exports: dict[str, str]) -> None:
    mapping = {
        "artifact_root": "SPARKOS_ARTIFACT_ROOT",
        "job_store_path": "SPARKOS_JOB_STORE_PATH",
        "livy_url": "SPARKOS_LIVY_URL",
        "history_server_url": "SPARKOS_HISTORY_SERVER_URL",
        "spark_master_url": "SPARKOS_SPARK_MASTER_URL",
        "spark_event_log_dir": "SPARKOS_SPARK_EVENT_LOG_DIR",
        "spark_driver_host": "SPARKOS_SPARK_DRIVER_HOST",
        "spark_driver_port": "SPARKOS_SPARK_DRIVER_PORT",
        "docker_spark_container": "SPARKOS_DOCKER_SPARK_CONTAINER",
        "docker_spark_master_url": "SPARKOS_DOCKER_SPARK_MASTER_URL",
    }
    for field, env_name in mapping.items():
        value = runtime.get(field)
        if value:
            exports[env_name] = str(value)


def _collect_agent(agent: dict, exports: dict[str, str]) -> None:
    if agent.get("skills_path"):
        exports["SPARKOS_SKILLS_PATH"] = str(agent["skills_path"])


def _section_env_name(section: dict, suffix: str) -> str:
    provider = str(section.get("provider") or section.get("kind") or "SPARKOS")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in provider.upper())
    return f"{normalized}_{suffix}"


if __name__ == "__main__":
    raise SystemExit(main())
