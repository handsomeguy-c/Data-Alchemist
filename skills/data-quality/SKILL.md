---
name: data-quality
description: 当用户需要检查数据质量、验证表产出、生成质量规则、校验 schema、空值率、唯一性、枚举值、数据分布或生成质量报告时使用。
---

# Data Quality Skill

Data Quality Skill 负责对输入表、ETL 产出表或特征表进行质量校验，生成规则、SQL 检查项和质量报告。

## 能力边界

本 Skill 负责：

1. Schema 校验。
2. 空值率检查。
3. 唯一性检查。
4. 重复率检查。
5. 枚举值合法性检查。
6. 数值范围检查。
7. 分区完整性检查。
8. 数据量波动检查。
9. 特征分布检查。
10. 输出质量报告和阻断建议。

不负责生成 ETL 主流程、修复脏数据或提交 Spark Job。

## 输入

```json
{
  "user_intent": "质量检查目标",
  "table": {
    "name": "表名",
    "columns": [],
    "partition_columns": []
  },
  "quality_rules": [
    {
      "rule_type": "not_null | unique | range | enum | volume | distribution",
      "column": "字段名",
      "condition": "规则条件"
    }
  ],
  "baseline": {
    "row_count": 0,
    "null_rates": {},
    "distribution": {}
  }
}
```

## 输出

```json
{
  "quality_checks": [
    {
      "check_name": "检查项",
      "sql": "检查 SQL",
      "severity": "blocker | warning | info",
      "pass_condition": "通过条件"
    }
  ],
  "quality_report": {
    "summary": "整体质量结论",
    "passed": [],
    "failed": [],
    "warnings": []
  },
  "recommended_actions": ["修复建议"],
  "next_skill": "data-cleaning | etl-pipeline | spark-job | null"
}
```

## 工作流程

1. 判断检查对象是源表、产出表还是特征表。
2. 根据字段类型和业务含义生成质量规则。
3. 对主键字段生成唯一性和非空检查。
4. 对分区字段生成分区完整性检查。
5. 对枚举字段生成合法值检查。
6. 对数值字段生成范围和分布检查。
7. 对产出表生成数据量波动检查。
8. 对特征表生成缺失率、极值、分布漂移检查。
9. 输出可执行 SQL 检查项。
10. 根据检查结果生成质量报告和阻断建议。

## 质量规则原则

1. 主键字段缺失或重复通常是 blocker。
2. 分区缺失通常是 blocker。
3. 重要指标大幅波动通常是 warning 或 blocker。
4. 非核心字段空值率升高通常是 warning。
5. 特征分布漂移需要提示模型风险。
6. 质量报告必须区分错误、警告和提示。

## 示例

用户需求：

```text
检查用户日活宽表今天分区是否正常。
```

输出检查项：

```json
[
  {
    "check_name": "分区是否存在",
    "severity": "blocker",
    "sql": "select count(*) from dws_user_active_di where dt='${biz_date}'"
  },
  {
    "check_name": "user_id 非空",
    "severity": "blocker",
    "sql": "select count(*) from dws_user_active_di where dt='${biz_date}' and user_id is null"
  },
  {
    "check_name": "数据量波动",
    "severity": "warning",
    "sql": "compare current row count with 7-day average"
  }
]
```
