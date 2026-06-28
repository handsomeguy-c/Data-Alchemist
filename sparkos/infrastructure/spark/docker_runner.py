from __future__ import annotations

import json
import subprocess
from pathlib import Path

from sparkos.domain.catalog import DatasetProfile


class DockerSparkRunner:
    def __init__(
        self,
        repo_root: Path,
        container_name: str = "sparkos-spark-master",
        master_url: str = "spark://spark-master:7077",
    ):
        self._repo_root = repo_root.resolve()
        self._container_name = container_name
        self._master_url = master_url
        self._container_root = self._detect_container_root()

    def is_available(self) -> bool:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0 and self._container_name in result.stdout.splitlines()

    def run_sql(
        self,
        run_id: str,
        sql: str,
        dataset: DatasetProfile,
        event_log_dir: str,
    ) -> tuple[str, str, dict[str, object]]:
        run_dir = self._repo_root / "artifacts" / "agent-runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        script_path = run_dir / "docker_spark_job.py"
        result_path = run_dir / "docker_spark_result.json"
        script_path.write_text(
            self._script(sql, dataset, result_path, event_log_dir),
            encoding="utf-8",
        )
        container_script = self._container_path(script_path)
        command = [
            "docker",
            "exec",
            self._container_name,
            "/opt/spark/bin/spark-submit",
            "--master",
            self._master_url,
            container_script,
        ]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=180)
        metrics = {
            "execution_mode": "docker_spark",
            "spark_available": True,
            "return_code": completed.returncode,
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "result_path": str(result_path),
        }
        if completed.returncode != 0:
            return "failed", completed.stderr[-1000:] or completed.stdout[-1000:], metrics
        if result_path.exists():
            result = json.loads(result_path.read_text(encoding="utf-8"))
            metrics.update(result)
        return "success", "Docker Spark 作业执行成功，已写入结果产物。", metrics

    def _script(
        self,
        sql: str,
        dataset: DatasetProfile,
        result_path: Path,
        event_log_dir: str,
    ) -> str:
        payload = {
            "sql": sql,
            "dataset": self._container_dataset(dataset),
            "result_path": self._container_path(result_path),
            "event_log_dir": self._container_uri(event_log_dir),
        }
        return f"""from __future__ import annotations
import json
from pathlib import Path
from pyspark.sql import SparkSession

payload = {json.dumps(payload, ensure_ascii=False)}
dataset = payload["dataset"]
builder = SparkSession.builder.appName("AGI-Gilgamesh-DockerSpark")
if payload["event_log_dir"]:
    builder = builder.config("spark.eventLog.enabled", "true")
    builder = builder.config("spark.eventLog.dir", payload["event_log_dir"])
spark = builder.getOrCreate()
reader = spark.read.option("header", True).option("inferSchema", True)
fmt = dataset["format"].lower()
path = dataset["path"]
if fmt == "csv":
    frame = reader.csv(path)
elif fmt == "json":
    frame = spark.read.json(path)
elif fmt == "parquet":
    frame = spark.read.parquet(path)
elif fmt == "hive":
    frame = spark.table(path or dataset["name"])
else:
    raise ValueError(f"Unsupported format: {{fmt}}")
frame.createOrReplaceTempView(dataset["name"])
rows = [row.asDict(recursive=True) for row in spark.sql(payload["sql"]).limit(20).collect()]
result = {{
    "preview_rows": len(rows),
    "preview": rows,
    "input_rows": frame.count(),
    "spark_app_id": spark.sparkContext.applicationId,
}}
Path(payload["result_path"]).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
spark.stop()
"""

    def _container_path(self, path: Path) -> str:
        absolute = path.resolve()
        return str(absolute).replace(str(self._repo_root), self._container_root, 1)

    def _container_uri(self, value: str) -> str:
        return value.replace(str(self._repo_root), self._container_root)

    def _container_dataset(self, dataset: DatasetProfile) -> dict[str, object]:
        payload = dataset.model_dump()
        path = str(payload.get("path") or "")
        if path.startswith(str(self._repo_root)):
            payload["path"] = path.replace(str(self._repo_root), self._container_root, 1)
        return payload

    def _detect_container_root(self) -> str:
        command = [
            "docker",
            "inspect",
            self._container_name,
            "--format",
            "{{range .Mounts}}{{println .Source .Destination}}{{end}}",
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        except Exception:
            return "/opt/sparkos"
        for line in result.stdout.splitlines():
            parts = line.split(maxsplit=1)
            if len(parts) == 2 and Path(parts[0]).resolve() == self._repo_root:
                return parts[1]
        return "/opt/sparkos"
