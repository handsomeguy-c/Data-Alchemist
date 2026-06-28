from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


def java_available() -> bool:
    if os.environ.get("PYSPARK_GATEWAY_PORT"):
        return True
    java_bin = Path(os.environ["JAVA_HOME"]) / "bin" / "java" if os.environ.get("JAVA_HOME") else None
    command = str(java_bin) if java_bin and java_bin.exists() else shutil.which("java")
    if not command:
        return False
    try:
        subprocess.run(
            [command, "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=5,
        )
        return True
    except Exception:
        return False
