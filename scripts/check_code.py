"""Check Sprint 1–2 Python syntax and required module/class/function explanations.

This lightweight repository gate uses the standard library and never executes
Databricks notebooks or contacts services. The legacy smoke test is outside scope.
"""

import ast
from pathlib import Path


def main() -> int:
    """Fail when an owned Python file has invalid syntax or missing docstrings."""
    root = Path(__file__).resolve().parents[1]
    paths = [*root.joinpath("src/quake_pipeline").rglob("*.py"),
             *root.joinpath("tests").glob("test_sprints_01_02.py"),
             *root.joinpath("databricks").glob("sprint*0[12]*.py"), Path(__file__)]
    errors = []
    for path in sorted(set(paths)):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as error:
            errors.append(f"{path.relative_to(root)}:{error.lineno}: invalid Python syntax")
            continue
        if not ast.get_docstring(tree):
            errors.append(f"{path.relative_to(root)}: missing file purpose description")
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and not ast.get_docstring(node):
                errors.append(f"{path.relative_to(root)}:{node.lineno}: {node.name} needs a docstring")
    if errors:
        print("\n".join(errors))
        return 1
    print(f"PASS syntax and explanatory docstrings: {len(set(paths))} Python files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
