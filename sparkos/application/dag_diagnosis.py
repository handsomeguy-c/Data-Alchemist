from __future__ import annotations


class DagDiagnosisEngine:
    def diagnose(self, user_request: str, observation=None) -> dict[str, object]:
        text = user_request.lower()
        bottlenecks = []
        suggestions = []
        if observation is not None:
            observed = self._from_observation(observation)
            bottlenecks.extend(observed["bottlenecks"])
            suggestions.extend(observed["suggestions"])
        if self._has_any(text, ["shuffle", "distinct", "group by", "聚合", "排序"]):
            bottlenecks.append(
                {
                    "type": "shuffle",
                    "evidence": "需求中包含聚合、去重、排序或 Shuffle 相关描述。",
                    "impact": "可能导致 Stage 网络 IO 和磁盘 spill 增加。",
                }
            )
            suggestions.append(
                {
                    "priority": "high",
                    "action": "提前过滤并预聚合，检查 spark.sql.shuffle.partitions。",
                    "reason": "减少进入 Shuffle 的数据量并控制 Task 粒度。",
                    "example": "set spark.sql.shuffle.partitions=400;",
                }
            )
        if self._has_any(text, ["倾斜", "skew", "长尾", "热点"]):
            bottlenecks.append(
                {
                    "type": "skew",
                    "evidence": "需求提到倾斜、长尾或热点 Key。",
                    "impact": "少量 Task 可能拖慢整个 Stage。",
                }
            )
            suggestions.append(
                {
                    "priority": "high",
                    "action": "识别热点 Key，对热点 Key 做 salting 或开启 AQE skew join。",
                    "reason": "让热点数据拆分到更多 Reduce Task。",
                    "example": "set spark.sql.adaptive.skewJoin.enabled=true;",
                }
            )
        if self._has_any(text, ["join", "关联", "宽表"]):
            bottlenecks.append(
                {
                    "type": "join",
                    "evidence": "需求涉及多表关联或宽表构建。",
                    "impact": "大表 Join 可能产生 Shuffle 放大或笛卡尔积风险。",
                }
            )
            suggestions.append(
                {
                    "priority": "medium",
                    "action": "Join 前过滤无效数据，小表使用 broadcast hint。",
                    "reason": "降低 Join 输入规模并避免不必要 Shuffle。",
                    "example": "select /*+ broadcast(dim) */ ...",
                }
            )
        if self._has_any(text, ["oom", "内存", "溢出", "spill"]):
            bottlenecks.append(
                {
                    "type": "oom",
                    "evidence": "需求提到 OOM、内存或 spill。",
                    "impact": "Executor 可能因为单 Task 数据过大或中间状态过多失败。",
                }
            )
            suggestions.append(
                {
                    "priority": "high",
                    "action": "排查 collect/cache/groupByKey，调整 memoryOverhead 并拆分超大步骤。",
                    "reason": "OOM 通常需要减少单 Task 状态，而不是只加内存。",
                    "example": "set spark.executor.memoryOverhead=2g;",
                }
            )
        if not bottlenecks:
            bottlenecks.append(
                {
                    "type": "scan",
                    "evidence": "当前没有执行指标，先按全表扫描和分区缺失做保守诊断。",
                    "impact": "无分区过滤可能导致扫描成本不可控。",
                }
            )
            suggestions.append(
                {
                    "priority": "medium",
                    "action": "确认时间分区和业务过滤条件。",
                    "reason": "自然语言到 Job 的第一步要先压缩输入数据范围。",
                    "example": "where dt between '${start_dt}' and '${end_dt}'",
                }
            )
        return {
            "diagnosis_summary": "已基于需求、计划和可用指标生成 Spark DAG 诊断。",
            "bottlenecks": bottlenecks,
            "optimization_suggestions": suggestions,
            "expected_effect": "降低扫描和 Shuffle 成本，减少长尾 Task，提升 Job 可观测性。",
            "next_skill": "spark-sql",
        }

    def _has_any(self, text: str, keywords: list[str]) -> bool:
        return any(keyword in text for keyword in keywords)

    def _from_observation(self, observation) -> dict[str, list[dict[str, object]]]:
        bottlenecks = []
        suggestions = []
        if observation.total_shuffle_bytes > 512 * 1024 * 1024:
            bottlenecks.append(
                {
                    "type": "shuffle",
                    "evidence": f"观测到 Shuffle 总量 {observation.total_shuffle_bytes} bytes。",
                    "impact": "网络 IO 和磁盘 spill 可能成为瓶颈。",
                }
            )
            suggestions.append(
                {
                    "priority": "high",
                    "action": "检查聚合和 Join 前过滤，必要时提高 shuffle partitions。",
                    "reason": "真实指标显示 Shuffle 数据量偏大。",
                    "example": "set spark.sql.adaptive.enabled=true;",
                }
            )
        for stage in observation.stages:
            if stage.spilled_bytes > 0:
                bottlenecks.append(
                    {
                        "type": "oom",
                        "evidence": f"Stage {stage.stage_id} spill {stage.spilled_bytes} bytes。",
                        "impact": "内存压力可能导致任务慢或失败。",
                    }
                )
                suggestions.append(
                    {
                        "priority": "medium",
                        "action": "减少单 Task 状态，调整 executor memoryOverhead。",
                        "reason": "spill 表示执行阶段发生内存退化。",
                        "example": "set spark.executor.memoryOverhead=2g;",
                    }
                )
        return {"bottlenecks": bottlenecks, "suggestions": suggestions}
