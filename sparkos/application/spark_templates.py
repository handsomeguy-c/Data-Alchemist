from __future__ import annotations

import re
from typing import Iterable, Optional

from sparkos.domain.agent import AgentPlan
from sparkos.domain.catalog import ColumnProfile, DatasetProfile


class SparkTaskTemplates:
    def primary_dataset(self, plan: AgentPlan) -> DatasetProfile:
        if plan.datasets:
            return plan.datasets[0]
        return DatasetProfile(
            name="source_table",
            description="用户尚未在 catalog 中声明的源数据表。",
            path="",
            format="unknown",
            columns=[
                ColumnProfile(name="entity_id", semantic_type="entity"),
                ColumnProfile(name="event_time", semantic_type="timestamp"),
                ColumnProfile(name="dt", semantic_type="timestamp"),
            ],
        )

    def build_query_sql(self, user_request: str, dataset: DatasetProfile) -> str:
        columns = [column.name for column in dataset.columns] or ["*"]
        entity = self.first_column(dataset, {"entity", "id"}) or columns[0]
        metric = self.first_column(dataset, {"metric"})
        timestamp = self.first_column(dataset, {"timestamp"}) or "dt"
        if "count" in user_request.lower() or "统计" in user_request:
            metric_expr = "count(*) as record_cnt"
            if metric:
                metric_expr = f"count(*) as record_cnt, sum({metric}) as {metric}_sum"
            return (
                f"select\n  {entity},\n  {metric_expr}\n"
                f"from {dataset.name}\n"
                f"where {timestamp} between '${{start_time}}' and '${{end_time}}'\n"
                f"group by {entity}\n"
                "order by record_cnt desc\n"
                "limit 100;"
            )
        selected = ",\n  ".join(columns[:8])
        return (
            f"select\n  {selected}\n"
            f"from {dataset.name}\n"
            f"where {timestamp} between '${{start_time}}' and '${{end_time}}'\n"
            "limit 100;"
        )

    def select_source_sql(self, dataset: DatasetProfile) -> str:
        columns = [column.name for column in dataset.columns] or ["*"]
        selected = ", ".join(columns[:12])
        return f"select {selected} from {dataset.name} where dt='${{biz_date}}';"

    def transform_sql(self, dataset: DatasetProfile) -> str:
        expressions = []
        for column in dataset.columns[:12]:
            if column.semantic_type == "timestamp":
                expressions.append(f"cast({column.name} as timestamp) as {column.name}")
            elif column.semantic_type in {"id", "entity", "dimension"}:
                expressions.append(f"trim(cast({column.name} as string)) as {column.name}")
            else:
                expressions.append(column.name)
        select_body = ",\n  ".join(expressions or ["*"])
        return (
            "create or replace temporary view transformed_records as\n"
            f"select\n  {select_body}\n"
            "from source_records;"
        )

    def cleaning_sql(self, dataset: DatasetProfile) -> str:
        expressions = []
        filters = []
        for column in dataset.columns[:12]:
            if column.semantic_type in {"id", "entity", "dimension"}:
                expressions.append(f"trim(cast({column.name} as string)) as {column.name}")
                if column.semantic_type in {"id", "entity"}:
                    filters.append(f"{column.name} is not null")
            elif column.semantic_type == "timestamp":
                expressions.append(f"cast({column.name} as timestamp) as {column.name}")
            else:
                expressions.append(column.name)
        select_body = ",\n  ".join(expressions or ["*"])
        where_clause = f"\nwhere {' and '.join(filters)}" if filters else ""
        return (
            "create or replace temporary view cleaned_records as\n"
            f"select distinct\n  {select_body}\n"
            f"from {dataset.name}{where_clause};"
        )

    def feature_sql(
        self,
        dataset: DatasetProfile,
        entity: str,
        metric: Optional[str],
    ) -> str:
        metric_expr = ""
        if metric:
            metric_expr = f",\n  sum({metric}) as {metric}_sum_30d"
        return (
            f"select\n  {entity},\n"
            "  count(*) as event_cnt_7d,\n"
            "  count(distinct dt) as active_days_30d"
            f"{metric_expr}\n"
            f"from {dataset.name}\n"
            "where dt between date_sub('${biz_date}', 30) and '${biz_date}'\n"
            f"group by {entity};"
        )

    def dataset_assumptions(self, plan: AgentPlan) -> list[str]:
        if not plan.datasets:
            return ["catalog 中没有匹配数据集，已使用 source_table 占位。"]
        return [f"优先使用数据集 {dataset.name}。" for dataset in plan.datasets[:3]]

    def sql_risks(self, sql: str, dataset: DatasetProfile) -> list[str]:
        risks = []
        if "where" not in sql.lower():
            risks.append("SQL 缺少过滤条件，可能全表扫描。")
        if not any(column.semantic_type == "timestamp" for column in dataset.columns):
            risks.append("未发现明确时间或分区字段，分区裁剪需要人工确认。")
        if "join" in sql.lower():
            risks.append("Join Key 和大小表关系需要执行前确认。")
        return risks or ["未发现阻断级风险，执行前仍需确认字段口径。"]

    def default_run_config(self) -> dict[str, object]:
        return {
            "spark_conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.shuffle.partitions": "400",
            },
            "partition": "dt='${biz_date}'",
        }

    def target_table_name(self, plan: AgentPlan, fallback: str) -> str:
        match = re.search(r"(?:写入|产出|生成)\s*([a-zA-Z_][a-zA-Z0-9_\.]*)", plan.user_request)
        if match:
            return match.group(1)
        return fallback

    def first_column(
        self,
        dataset: DatasetProfile,
        semantic_types: Iterable[str],
    ) -> Optional[str]:
        accepted = set(semantic_types)
        for column in dataset.columns:
            if column.semantic_type in accepted:
                return column.name
        return None

    def replace_template_vars(self, sql: str) -> str:
        return (
            sql.replace("${start_time}", "1900-01-01")
            .replace("${end_time}", "2999-12-31")
            .replace("${biz_date}", "2999-12-31")
        )

    def slug(self, value: str) -> str:
        text = re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower())
        text = re.sub(r"_+", "_", text).strip("_")
        return text or "agent_task"
