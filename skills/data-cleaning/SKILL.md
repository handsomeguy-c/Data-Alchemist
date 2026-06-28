---
name: data-cleaning
description: 当用户需要处理缺失值、重复值、异常值、类型不一致、脏数据、字段标准化、枚举值修正或生成数据清洗规则时使用。
---

# Data Cleaning Skill

Data Cleaning Skill 负责根据数据质量问题和业务目标生成清洗规则，并将规则转换为 Spark SQL 或 PySpark 代码。

## 能力边界

本 Skill 负责：

1. 识别缺失值问题。
2. 识别重复数据问题。
3. 识别异常值和非法值。
4. 处理字段类型转换。
5. 标准化日期、金额、枚举、文本字段。
6. 生成清洗前后对比指标。
7. 输出清洗规则和可执行代码。

不负责完整 ETL 编排、模型特征构造或 Spark Job 调度。

## 输入

```json
{
  "user_intent": "清洗目标",
  "table": {
    "name": "表名",
    "columns": [
      {
        "name": "字段名",
        "type": "字段类型",
        "description": "字段说明"
      }
    ],
    "sample_rows": []
  },
  "quality_profile": {
    "null_rates": {},
    "duplicate_rate": 0.0,
    "invalid_values": {},
    "outlier_summary": {}
  },
  "cleaning_preferences": {
    "missing_value_strategy": "drop | fill | infer | keep",
    "duplicate_strategy": "drop_exact | drop_by_key | keep_latest",
    "outlier_strategy": "clip | remove | flag | keep"
  }
}
```

## 输出

```json
{
  "cleaning_rules": [
    {
      "column": "字段名",
      "problem": "问题描述",
      "strategy": "处理策略",
      "reason": "选择原因"
    }
  ],
  "sql_or_code": "Spark SQL 或 PySpark 清洗代码",
  "before_after_metrics": {
    "before": {},
    "after_expected": {}
  },
  "warnings": ["可能影响业务语义的清洗动作"],
  "next_skill": "data-quality | feature-engineering | etl-pipeline"
}
```

## 工作流程

1. 判断清洗任务类型：缺失值、重复值、异常值、格式标准化或综合清洗。
2. 分析字段类型和业务含义。
3. 根据质量画像选择清洗策略。
4. 对高风险字段保守处理，例如用户 ID、订单 ID、金额、时间字段。
5. 生成清洗规则列表。
6. 生成 Spark SQL 或 PySpark 代码。
7. 输出清洗前后指标口径。
8. 将清洗结果交给 DataQualitySkill 进行校验。
9. 如果后续要做建模，将结果交给 FeatureEngineeringSkill。

## 清洗策略

### 缺失值

1. 主键、用户 ID、订单 ID 缺失：默认删除或标记为无效。
2. 数值字段缺失：可用 0、中位数、均值或业务默认值填充。
3. 类别字段缺失：可填充为 `unknown`。
4. 时间字段缺失：优先保留并标记，不默认填当前时间。

### 重复值

1. 完全重复：可直接去重。
2. 主键重复：优先保留更新时间最新的一条。
3. 业务重复：需要根据用户指定 Key 判断。

### 异常值

1. 金额为负：根据业务判断是否非法。
2. 年龄、时长、次数等字段超出合理范围：可截断、删除或标记。
3. 枚举值不在白名单：映射到标准值或标记为 `unknown`。

## 示例

用户需求：

```text
清洗用户表，去掉重复用户，年龄为空填 unknown，非法手机号置空。
```

输出规则：

```json
[
  {
    "column": "user_id",
    "problem": "重复用户",
    "strategy": "按 user_id 去重，保留 update_time 最新记录"
  },
  {
    "column": "age",
    "problem": "缺失值",
    "strategy": "填充 unknown"
  },
  {
    "column": "phone",
    "problem": "格式非法",
    "strategy": "不符合手机号正则的值置空"
  }
]
```
