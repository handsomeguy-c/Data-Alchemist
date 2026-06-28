---
name: etl-pipeline
description: 当用户需要构建 ETL 流水线、读取数据源、字段映射、数据转换、分区写入、生成 Spark ETL Job 或编排多步骤数据处理流程时使用。
---

# ETL Pipeline Skill

ETL Pipeline Skill 负责把用户的数据加工需求转换为结构化 ETL 流程，生成可执行的 Spark SQL 或 PySpark 任务，并管理输入、转换、输出之间的依赖关系。

## 能力边界

本 Skill 负责：

1. 设计 ETL 流水线。
2. 生成数据读取逻辑。
3. 生成字段映射和转换逻辑。
4. 生成过滤、聚合、Join、落表逻辑。
5. 设计分区写入和覆盖策略。
6. 输出 DAG 化的 ETL 步骤。
7. 将可执行任务交给 SparkJobSkill。

不负责单独的数据质量分析、复杂特征工程设计或 Spark DAG 性能诊断。

## 输入

```json
{
  "user_intent": "用户的 ETL 需求",
  "source_tables": [
    {
      "name": "源表名",
      "columns": [],
      "partition_columns": [],
      "incremental_column": "增量字段"
    }
  ],
  "target_table": {
    "name": "目标表名",
    "columns": [],
    "partition_columns": []
  },
  "transform_rules": [
    {
      "source_column": "源字段",
      "target_column": "目标字段",
      "rule": "转换规则"
    }
  ],
  "write_mode": "overwrite | append | merge",
  "schedule": "daily | hourly | manual"
}
```

## 输出

```json
{
  "pipeline_name": "ETL 流水线名称",
  "steps": [
    {
      "step_id": "步骤 ID",
      "type": "read | transform | join | aggregate | write",
      "description": "步骤说明",
      "sql_or_code": "Spark SQL 或 PySpark 代码"
    }
  ],
  "dependencies": [
    {
      "from": "上游步骤",
      "to": "下游步骤"
    }
  ],
  "target_table": "目标表",
  "run_config": {
    "spark_conf": {},
    "partition": "分区配置"
  },
  "next_skill": "spark-job | data-quality | dag-diagnosis"
}
```

## 工作流程

1. 识别用户是全量 ETL、增量 ETL、宽表构建还是多表汇总。
2. 分析源表、目标表和字段映射关系。
3. 生成读取步骤，明确分区条件和增量条件。
4. 生成转换步骤，包括字段重命名、类型转换、派生字段和过滤条件。
5. 如果涉及多表，生成 Join 步骤并说明 Join Key。
6. 如果涉及指标汇总，生成聚合步骤并说明聚合粒度。
7. 生成写入步骤，明确写入模式、分区策略和幂等性。
8. 输出结构化 DAG，供 Runtime 编排。
9. 如需要执行，交给 SparkJobSkill。
10. 如需要校验产出，交给 DataQualitySkill。

## 设计原则

1. ETL 步骤必须可拆解、可观察、可重试。
2. 每个步骤只做一类事情，避免巨型 SQL。
3. 目标表字段必须能追溯到源字段或转换规则。
4. 增量任务必须明确增量字段和补数策略。
5. 分区写入必须避免误覆盖历史数据。
6. 输出必须包含执行顺序和依赖关系。

## 示例

用户需求：

```text
每天把用户行为明细加工成用户日活宽表。
```

输出步骤：

```json
{
  "pipeline_name": "user_daily_active_etl",
  "steps": [
    {
      "step_id": "read_action",
      "type": "read",
      "description": "读取用户行为明细表指定日期分区"
    },
    {
      "step_id": "aggregate_user",
      "type": "aggregate",
      "description": "按 user_id 聚合活跃次数、最近活跃时间"
    },
    {
      "step_id": "write_target",
      "type": "write",
      "description": "写入 dws_user_active_di 的 dt 分区"
    }
  ]
}
```
