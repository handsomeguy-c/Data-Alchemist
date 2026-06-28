from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path

from sparkos.domain.graph import GraphEdge, GraphResult


class GraphRuntime:
    def __init__(self, artifact_root: Path):
        self._artifact_root = artifact_root

    def run(
        self,
        run_id: str,
        edges: list[GraphEdge],
        algorithm: str = "connected_components",
        source: str = "",
        target: str = "",
    ) -> GraphResult:
        if algorithm == "bfs":
            rows = self._bfs(edges, source, target)
        elif algorithm == "degree":
            rows = self._degree(edges)
        else:
            rows = self._connected_components(edges)
            algorithm = "connected_components"
        vertices = sorted({edge.src for edge in edges} | {edge.dst for edge in edges})
        result = GraphResult(
            algorithm=algorithm,
            vertices=vertices,
            edges=edges,
            rows=rows,
            metrics={
                "vertex_count": len(vertices),
                "edge_count": len(edges),
                "execution_mode": "in_process_graph",
            },
        )
        artifact = self._write(run_id, result)
        return result.model_copy(update={"artifact_path": str(artifact)})

    def _connected_components(self, edges: list[GraphEdge]) -> list[dict[str, object]]:
        adjacency = self._adjacency(edges)
        visited = set()
        rows = []
        component_id = 0
        for node in sorted(adjacency):
            if node in visited:
                continue
            component_id += 1
            queue = deque([node])
            visited.add(node)
            members = []
            while queue:
                current = queue.popleft()
                members.append(current)
                for neighbor in adjacency[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)
            rows.append(
                {
                    "component_id": f"C-{component_id:03d}",
                    "size": len(members),
                    "members": members,
                }
            )
        return rows

    def _degree(self, edges: list[GraphEdge]) -> list[dict[str, object]]:
        degree = defaultdict(int)
        for edge in edges:
            degree[edge.src] += 1
            degree[edge.dst] += 1
        return [
            {"vertex": vertex, "degree": count}
            for vertex, count in sorted(degree.items(), key=lambda item: -item[1])
        ]

    def _bfs(
        self,
        edges: list[GraphEdge],
        source: str,
        target: str,
    ) -> list[dict[str, object]]:
        adjacency = self._adjacency(edges)
        queue = deque([(source, [source])])
        visited = {source}
        while queue:
            node, path = queue.popleft()
            if node == target:
                return [{"source": source, "target": target, "path": path}]
            for neighbor in adjacency[node]:
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                queue.append((neighbor, [*path, neighbor]))
        return [{"source": source, "target": target, "path": []}]

    def _adjacency(self, edges: list[GraphEdge]) -> dict[str, set[str]]:
        adjacency = defaultdict(set)
        for edge in edges:
            adjacency[edge.src].add(edge.dst)
            adjacency[edge.dst].add(edge.src)
        return adjacency

    def _write(self, run_id: str, result: GraphResult) -> Path:
        run_dir = self._artifact_root / "agent-runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "08_graph_result.json"
        path.write_text(
            json.dumps(result.model_dump(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path
