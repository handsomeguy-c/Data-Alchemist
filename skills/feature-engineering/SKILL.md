---
name: feature-engineering
description: 当用户需要构造机器学习特征、统计特征、窗口特征、行为特征、分桶、编码、归一化、特征宽表或训练样本表时使用。
---

# Feature Engineering Skill

Feature Engineering Skill 负责把业务目标和原始数据转换为可训练、可复用、可解释的特征集合，并生成 Spark SQL 或 PySpark 特征加工逻辑。

## 能力边界

本 Skill 负责：

1. 设计特征集合。
2. 生成统计聚合特征。
3. 生成时间窗口特征。
4. 生成用户行为序列特征。
5. 生成类别编码、分桶、归一化逻辑。
6. 构建训练样本表。
7. 输出特征字典和特征加工代码。

不负责模型训练、模型评估、线上推理服务或 Spark Job 调度。

## 输入

```json
{
  "user_intent": "特征工程目标",
  "prediction_target": "预测目标",
  "entity_key": "样本粒度，如 user_id / item_id / order_id",
  "label_definition": "标签定义",
  "source_tables": [
    {
      "name": "源表名",
      "columns": [],
      "partition_columns": []
    }
  ],
  "time_windows": ["1d", "7d", "30d"],
  "feature_preferences": {
    "include_behavior_features": true,
    "include_statistical_features": true,
    "include_cross_features": false
  }
}
```

## 输出

```json
{
  "feature_set_name": "特征集合名称",
  "entity_key": "样本主键",
  "label": {
    "name": "标签名",
    "definition": "标签定义"
  },
  "features": [
    {
      "name": "特征名",
      "type": "numeric | categorical | boolean | sequence",
      "description": "特征含义",
      "source": "来源字段或表",
      "calculation": "计算逻辑"
    }
  ],
  "sql_or_code": "Spark SQL 或 PySpark 特征加工代码",
  "leakage_warnings": ["数据泄漏风险"],
  "next_skill": "spark-job | data-quality | dag-diagnosis"
}
```

## 工作流程

1. 明确预测目标、样本粒度和标签定义。
2. 检查是否存在时间穿越或标签泄漏风险。
3. 根据业务目标设计特征类别。
4. 生成基础统计特征，如计数、求和、均值、最大值、最小值。
5. 生成时间窗口特征，如近 1 天、7 天、30 天行为次数。
6. 生成类别特征处理逻辑，如 one-hot、frequency encoding、target encoding 提示。
7. 生成数值特征处理逻辑，如归一化、分桶、截断。
8. 输出特征字典。
9. 输出 Spark SQL 或 PySpark 代码。
10. 将结果交给 DataQualitySkill 校验特征完整性。

## 特征设计原则

1. 特征必须绑定样本主键。
2. 标签时间之后的数据不能用于特征计算。
3. 窗口特征必须明确观察窗口和截止时间。
4. 类别特征必须处理未知枚举值。
5. 数值特征必须处理缺失值和极端值。
6. 特征名称要包含业务含义、窗口和统计方式。
7. 特征字典必须能解释每个特征的来源和计算方式。

## 示例

用户需求：

```text
为用户流失预测构造特征。
```

输出特征：

```json
[
  {
    "name": "login_cnt_7d",
    "type": "numeric",
    "description": "用户近 7 天登录次数",
    "calculation": "count login events in 7-day window"
  },
  {
    "name": "pay_amount_sum_30d",
    "type": "numeric",
    "description": "用户近 30 天支付总金额",
    "calculation": "sum payment amount in 30-day window"
  },
  {
    "name": "active_days_30d",
    "type": "numeric",
    "description": "用户近 30 天活跃天数",
    "calculation": "count distinct active dt in 30-day window"
  }
]
```
