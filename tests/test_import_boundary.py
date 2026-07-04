"""Architecture guard: the truth path stays pure and the dependency arrow is one-way.

* ``sluice.control`` imports nothing outside the standard library.
* The shell (``proxy``/``usage``/``cli``) may import ``control``; ``control`` imports none
  of them.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

CORE = Path(__file__).resolve().parent.parent / "src" / "sluice" / "control.py"
SHELL_MODULES = {"sluice.proxy", "sluice.usage", "sluice.cli", "sluice.gate", "sluice.reconcile", "sluice.singleton", "sluice.status", "sluice.providers", "sluice.history", "sluice.history_store", "sluice.admin", "sluice.lifecycle"}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_control_imports_stdlib_only():
    stdlib = set(sys.stdlib_module_names)
    offenders = {m for m in _imported_modules(CORE) if m not in stdlib and m != "sluice"}
    assert not offenders, f"sluice.control may import stdlib only, found: {sorted(offenders)}"


def test_control_does_not_import_shell():
    tree = ast.parse(CORE.read_text(encoding="utf-8"))
    referenced = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not (referenced & SHELL_MODULES), "control must not import the shell layer"
