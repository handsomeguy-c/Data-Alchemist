from __future__ import annotations

from enum import Enum
from typing import List

from pydantic import BaseModel, Field

from sparkos.domain.problem import ProblemType


class ExecutionBackend(str, Enum):
    LOCAL = "local"
    SPARK = "spark"
    GRAPH = "graph"


class Capability(BaseModel):
    name: str
    label: str
    problem_types: List[ProblemType]
    preferred_backend: ExecutionBackend
    description: str
    required_semantics: List[str] = Field(default_factory=list)


DEFAULT_CAPABILITIES = [
    Capability(
        name="data_profile",
        label="数据画像",
        problem_types=[ProblemType.DATA_PROFILING],
        preferred_backend=ExecutionBackend.SPARK,
        description="识别字段结构、数据规模、缺失情况、基数和分布特征。",
    ),
    Capability(
        name="quality_scan",
        label="数据质量检查",
        problem_types=[ProblemType.DATA_QUALITY],
        preferred_backend=ExecutionBackend.SPARK,
        description="发现缺失、重复、非法记录和数据漂移。",
    ),
    Capability(
        name="relationship_network",
        label="关系网络构建",
        problem_types=[
            ProblemType.RELATIONSHIP_ANALYSIS,
            ProblemType.GRAPH_COMMUNITY,
            ProblemType.ANOMALY_DETECTION,
        ],
        preferred_backend=ExecutionBackend.SPARK,
        description="基于共享标识和事件行为构建实体之间的关系网络。",
        required_semantics=["entity"],
    ),
    Capability(
        name="community_detection",
        label="群体发现",
        problem_types=[ProblemType.GRAPH_COMMUNITY, ProblemType.ANOMALY_DETECTION],
        preferred_backend=ExecutionBackend.SPARK,
        description="识别关系网络中的高密度群体、异常群体和可疑连接结构。",
        required_semantics=["entity"],
    ),
    Capability(
        name="path_tracing",
        label="路径追踪",
        problem_types=[ProblemType.GRAPH_PATH],
        preferred_backend=ExecutionBackend.SPARK,
        description="追踪实体之间的关联路径、跳数和关键中介节点。",
        required_semantics=["entity"],
    ),
    Capability(
        name="feature_generation",
        label="批量特征生成",
        problem_types=[ProblemType.FEATURE_GENERATION],
        preferred_backend=ExecutionBackend.SPARK,
        description="生成可复用的聚合特征、关系特征和图结构特征。",
    ),
]
