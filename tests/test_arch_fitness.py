import os
import re
from pathlib import Path


def test_no_canvas_imports_in_agnostic_core():
    """
    Architecture Fitness Test:
    Asserts that core modules and generic platform packages under src/ucae/
    do not import Canvas-specific or Blackboard-specific modules directly.
    All format provider details must remain isolated inside their bounded contexts.
    """
    project_root = Path(__file__).parent.parent
    src_dir = project_root / "src"

    # Regex patterns looking for imports of provider models
    forbidden_patterns = [
        re.compile(r"^\s*(?:import|from)\s+(?:src\.)?models\.canvas_models\b"),
        re.compile(r"^\s*(?:import|from)\s+(?:src\.)?ucae\.providers\.canvas\b"),
        re.compile(r"^\s*(?:import|from)\s+(?:src\.)?models\.canvas_course\b"),
    ]

    # Directories that must remain generic and agnostic
    agnostic_dirs = [
        src_dir / "ucae" / "workflow",
        src_dir / "ucae" / "canonical",
        src_dir / "ucae" / "validation",
        src_dir / "ucae" / "reporting",
    ]

    violations = []

    for directory in agnostic_dirs:
        if not directory.exists():
            continue
        for root, _, files in os.walk(directory):
            for file in files:
                if not file.endswith(".py"):
                    continue
                file_path = Path(root) / file
                with open(file_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        for pattern in forbidden_patterns:
                            if pattern.match(line):
                                rel_path = file_path.relative_to(project_root)
                                violations.append(f"{rel_path}:L{line_num} - {line.strip()}")

    # Format output for quick diagnostic reading
    assert not violations, (
        "Architecture fitness violation: Generic engine code must not depend on specific provider implementations.\n"
        "Found violations:\n" + "\n".join(violations)
    )
