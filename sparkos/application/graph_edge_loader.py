from __future__ import annotations

import csv
from pathlib import Path

from sparkos.domain.agent import AgentPlan
from sparkos.domain.graph import GraphEdge


class GraphEdgeLoader:
    def load_for_plan(self, plan: AgentPlan) -> list[GraphEdge]:
        graph_dataset = self._graph_dataset(plan)
        if graph_dataset and Path(graph_dataset.path).exists():
            return self.load_csv(Path(graph_dataset.path))
        datasets = plan.datasets
        if datasets and datasets[0].columns:
            entity_columns = [
                column.name
                for column in datasets[0].columns
                if column.semantic_type == "entity"
            ]
            if len(entity_columns) >= 2:
                return [
                    GraphEdge(src=entity_columns[0], dst=entity_columns[1]),
                    GraphEdge(src=entity_columns[1], dst="risk_group"),
                    GraphEdge(src=entity_columns[0], dst="risk_group"),
                ]
        return [
            GraphEdge(src="user_a", dst="device_1"),
            GraphEdge(src="user_b", dst="device_1"),
            GraphEdge(src="user_c", dst="device_2"),
        ]

    def load_csv(self, path: Path, limit: int = 5000) -> list[GraphEdge]:
        edges = []
        with path.open("r", encoding="utf-8", newline="") as file:
            for row in csv.DictReader(file):
                if not row.get("src") or not row.get("dst"):
                    continue
                edges.append(
                    GraphEdge(
                        src=row["src"],
                        dst=row["dst"],
                        weight=float(row.get("weight") or 1.0),
                    )
                )
                if len(edges) >= limit:
                    break
        return edges

    def _graph_dataset(self, plan: AgentPlan):
        for dataset in plan.datasets:
            if dataset.name == "graph_edges":
                return dataset
        return None
