from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Optional

import yaml

from sparkos.domain.agent import SkillSpec


class SkillRegistry:
    def __init__(self, skills: Iterable[SkillSpec]):
        self._skills = {skill.name: skill for skill in skills}

    @classmethod
    def from_directory(cls, root: Optional[Path]) -> "SkillRegistry":
        if root is None or not root.exists():
            return cls([])

        manifest = root / "manifest.yaml"
        if manifest.exists():
            return cls(_parse_manifest(root, manifest))

        skills = []
        for path in sorted(root.glob("*/SKILL.md")):
            skills.append(_parse_skill(path))
        return cls(skills)

    def list(self) -> List[SkillSpec]:
        return list(self._skills.values())

    def get(self, name: str) -> Optional[SkillSpec]:
        return self._skills.get(name)

    def require(self, name: str) -> SkillSpec:
        skill = self.get(name)
        if skill is None:
            raise ValueError(f"Skill not found: {name}")
        return skill

    def has(self, name: str) -> bool:
        return name in self._skills


def _parse_skill(path: Path) -> SkillSpec:
    raw = path.read_text(encoding="utf-8")
    metadata = _front_matter(raw)
    return SkillSpec(
        name=metadata.get("name", path.parent.name),
        description=metadata.get("description", ""),
        body=raw,
        source_path=path,
    )


def _parse_manifest(root: Path, manifest: Path) -> List[SkillSpec]:
    raw = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
    skills = []
    for item in raw.get("skills", []):
        path = root / item["path"]
        skill = _parse_skill(path)
        if item.get("name") and item["name"] != skill.name:
            raise ValueError(
                f"Skill manifest name mismatch: {item['name']} != {skill.name}"
            )
        skills.append(skill)
    return skills


def _front_matter(markdown: str) -> dict[str, str]:
    match = re.match(r"\A---\n(?P<body>.*?)\n---\n", markdown, re.DOTALL)
    if not match:
        return {}

    metadata: dict[str, str] = {}
    for line in match.group("body").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()
    return metadata
