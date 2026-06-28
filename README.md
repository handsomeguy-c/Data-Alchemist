SparkTUIAgent：面向数据工程场景的智能任务执行系统
1. 自然语言到 Spark Job 的端到端执行链路：设计 Agent Runtime，将用户自然语言任务拆解为意图识别、计划生成、Catalog 匹配、Spark SQL/ETL 生成、作业提交和结果总结。
难点在于避免模型直接生成不可控代码，通过多阶段校验和结构化执行计划，实现从 Query 到真实 Spark Job 的闭环。
2. 可组合 Skill 体系：把数据工程能力模块化：将 Spark SQL、ETL、数据清洗、特征工程、数据质量检查、DAG 诊断、Spark 执行封装为 7 类可组合 Skill。
通过 Skill Registry 和统一输入输出协议实现自动编排，避免大 Controller 耦合，方便后续扩展新的数据工程能力。
3. Spark 执行与任务生命周期管理：基于 Docker Compose 搭建 Spark Master、Worker、History Server，本地支持真实 Spark Job 提交和执行。
实现任务 queued/submitted/succeeded/failed 状态管理、SQLite 历史记录、执行产物落盘和容器路径映射，保证演示链路可复现。
4. DAG 观测与大数据性能诊断：构建 Spark Event Log / History Server 解析能力，分析 Shuffle、数据倾斜、Join 策略、长尾 Task、扫描量和 OOM 风险。
将 Spark 调优经验规则化，自动生成广播 Join、分区调整、过滤下推、倾斜 Key 处理等优化建议。
5. 面向长任务的 TUI Agent 交互设计：使用 Textual 实现极简 TUI，支持用户输入即时上屏、模型流式输出、顶部状态栏和 Token 使用量统计。
TUI 层只负责交互展示，任务执行通过 Application Service 异步调度，保证长时间 Spark 任务运行时界面不卡顿、反馈清晰。
