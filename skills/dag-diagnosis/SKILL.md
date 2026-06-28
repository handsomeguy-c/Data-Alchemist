---
name: dag-diagnosis
description: 当用户需要分析 Spark DAG、执行计划、Stage 耗时、Shuffle、数据倾斜、Join 策略、任务慢、OOM、长尾任务或生成 Spark 性能优化建议时使用。
---

# DAG Diagnosis Skill

DAG Diagnosis Skill 负责分析 Spark Job 的执行计划、DAG、Stage 指标和日志，定位性能瓶颈，并生成可执行的优化建议。

## 能力边界

本 Skill 负责：

1. 分析 Spark DAG。
2. 分析 Stage 耗时和 Task 分布。
3. 识别 Shuffle 过大。
4. 识别数据倾斜。
5. 分析 Join 策略。
6. 分析 Executor OOM。
7. 分析长尾任务。
8. 生成 SQL、参数和数据布局优化建议。

不负责提交 Spark Job、生成业务 SQL 或修改数据源。

## 输入

```json
{
  "job_id": "Spark Job ID",
  "sql_or_code": "SQL 或 PySpark 代码",
  "explain_plan": "Spark explain 执行计划",
  "dag_metrics": {
    "stages": [
      {
        "stage_id": "Stage ID",
        "duration": "耗时",
        "task_count": 0,
        "shuffle_read": "Shuffle Read",
        "shuffle_write": "Shuffle Write",
        "input_rows": 0,
        "skewed_tasks": []
      }
    ]
  },
  "logs": "关键日志摘要"
}
```

## 输出

```json
{
  "diagnosis_summary": "性能问题总结",
  "bottlenecks": [
    {
      "type": "shuffle | skew | join | oom | scan | small_files | partition",
      "evidence": "判断依据",
      "impact": "影响范围"
    }
  ],
  "optimization_suggestions": [
    {
      "priority": "high | medium | low",
      "action": "优化动作",
      "reason": "原因",
      "example": "示例 SQL 或 Spark 参数"
    }
  ],
  "expected_effect": "预期收益",
  "next_skill": "spark-sql | etl-pipeline | spark-job | null"
}
```

## 工作流程

1. 读取 Spark Job 的执行计划、Stage 指标和日志摘要。
2. 找出耗时最长的 Stage。
3. 判断是否存在大规模 Shuffle。
4. 判断 Task 耗时是否分布不均，识别数据倾斜。
5. 分析 Join 类型和 Join Key。
6. 判断是否存在全表扫描、无分区过滤或过多小文件。
7. 判断 OOM 是否由 Join、聚合、缓存或 Shuffle 引起。
8. 输出瓶颈列表和证据。
9. 给出按优先级排序的优化建议。
10. 如果需要改写 SQL，将任务交回 SparkSQLSkill。
11. 如果需要调整运行参数，将任务交回 SparkJobSkill。

## 诊断规则

### Shuffle 过大

出现以下情况时提示 Shuffle 风险：

1. Shuffle Read 或 Shuffle Write 远高于输入数据量。
2. group by、distinct、join、order by 出现在大表上。
3. shuffle partition 设置过小或过大。
4. 中间数据量明显膨胀。

### 数据倾斜

出现以下情况时判断可能存在数据倾斜：

1. 少数 Task 耗时远高于中位数。
2. 少数 Task 处理数据量远高于其他 Task。
3. Join Key 或 Group Key 存在热点值。
4. Stage 长时间卡在最后少量 Task。

### Join 问题

重点检查：

1. 是否大表 Join 大表。
2. 是否可以 Broadcast 小表。
3. Join Key 是否高基数。
4. Join 前是否可以过滤或预聚合。
5. 是否存在笛卡尔积风险。

### OOM 问题

重点检查：

1. 单 Task 输入数据是否过大。
2. 是否存在 collect、cache、groupByKey 等高风险操作。
3. Join 或聚合是否产生大量中间状态。
4. executor memory 和 memory overhead 是否不足。

## 优化建议类型

1. SQL 改写：提前过滤、预聚合、减少 distinct、避免 select *。
2. Join 优化：Broadcast Join、Skew Join、Salting、分桶。
3. 参数优化：调整 shuffle partitions、executor memory、executor cores。
4. 数据布局优化：分区、分桶、合并小文件。
5. 任务拆分：将超大 SQL 拆成多个可观测步骤。
6. 缓存策略：只缓存复用且成本高的中间结果。

## 示例

输入现象：

```text
某个 Stage 有 2000 个 Task，其中 1990 个在 20 秒内完成，剩余 10 个运行超过 20 分钟。
```

输出诊断：

```json
{
  "diagnosis_summary": "该任务存在明显长尾 Task，疑似 Join Key 或 Group Key 数据倾斜。",
  "bottlenecks": [
    {
      "type": "skew",
      "evidence": "少量 Task 耗时远高于大多数 Task",
      "impact": "拖慢整个 Stage 完成时间"
    }
  ],
  "optimization_suggestions": [
    {
      "priority": "high",
      "action": "检查 Join Key 热点值，并对热点 Key 使用 Salting",
      "reason": "热点 Key 导致部分 Reduce Task 数据量过大"
    },
    {
      "priority": "medium",
      "action": "Join 前先过滤无效数据并对大表预聚合",
      "reason": "减少进入 Shuffle 的数据量"
    }
  ]
}
```
