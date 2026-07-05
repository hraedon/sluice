"""Tests for the committed-identifier gate.

Ported from the sibling project (gpo-lens/tests/test_check_identifiers.py) so
the gate stays consistent across the tool family.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from subprocess import CompletedProcess
from types import ModuleType

import pytest


def _load_checker() -> ModuleType:
    repo_root = Path(__file__).resolve().parent.parent
    path = repo_root / "scripts" / "check_committed_identifiers.py"
    spec = importlib.util.spec_from_file_location("check_committed_identifiers", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_committed_identifiers"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def checker() -> ModuleType:
    return _load_checker()


def test_parse_identifiers_filters_short_and_empty_tokens(checker: ModuleType) -> None:
    raw = "   abc  WORK-DOMAIN.local  \n   x   FAKEDOM  "
    identifiers = checker.parse_identifier_set(raw)
    assert identifiers == frozenset({"work-domain.local", "fakedom"})


def test_parse_identifiers_normalizes_case(checker: ModuleType) -> None:
    identifiers = checker.parse_identifier_set("SYNTHETIC-DOMAIN synthetic.example.com")
    assert identifiers == frozenset({"synthetic-domain", "synthetic.example.com"})


def test_scan_text_no_identifiers_yields_nothing(checker: ModuleType) -> None:
    assert list(checker.scan_text("contains SYNTHETIC-DOMAIN", frozenset())) == []


def test_scan_text_finds_identifier_with_line_details(checker: ModuleType) -> None:
    text = "first line\nThis mentions SYNTHETIC-DOMAIN here.\nthird line"
    identifiers = frozenset({"synthetic-domain"})
    violations = list(checker.scan_text(text, identifiers))
    assert len(violations) == 1
    v = violations[0]
    assert v.identifier == "synthetic-domain"
    assert v.line_number == 2
    assert v.line == "This mentions SYNTHETIC-DOMAIN here."


def test_scan_text_is_case_insensitive(checker: ModuleType) -> None:
    text = "upper SYNTHETIC-DOMAIN lower synthetic-domain mixed SyNtHeTiC-dOmAiN"
    identifiers = frozenset({"synthetic-domain"})
    violations = list(checker.scan_text(text, identifiers))
    assert len(violations) == 3
    assert {v.line_number for v in violations} == {1}


def test_scan_text_matches_substring(checker: ModuleType) -> None:
    text = "prefix-SYNTHETIC-DOMAIN-suffix"
    identifiers = frozenset({"synthetic-domain"})
    violations = list(checker.scan_text(text, identifiers))
    assert len(violations) == 1


def test_scan_text_absent_identifier_yields_nothing(checker: ModuleType) -> None:
    text = "Nowhere in this text is the magic word."
    identifiers = frozenset({"synthetic-domain"})
    assert list(checker.scan_text(text, identifiers)) == []


def test_scan_files_finds_violation_in_text_file(checker: ModuleType, tmp_path: Path) -> None:
    file_path = tmp_path / "notes.md"
    file_path.write_text("Data from SYNTHETIC-DOMAIN\nMore data\n", encoding="utf-8")
    identifiers = frozenset({"synthetic-domain"})
    violations = checker.scan_files(identifiers, [file_path])
    assert len(violations) == 1
    v = violations[0]
    assert v.path == file_path
    assert v.line_number == 1
    assert "SYNTHETIC-DOMAIN" in v.line


def test_scan_files_skips_binary_file(checker: ModuleType, tmp_path: Path) -> None:
    # Identifier is present in BOTH files — the binary one must be skipped.
    binary = tmp_path / "data.bin"
    binary.write_bytes(b"SYNTHETIC-DOMAIN in binary\x00null")
    text_file = tmp_path / "notes.md"
    text_file.write_text("SYNTHETIC-DOMAIN appears here.\n", encoding="utf-8")
    identifiers = frozenset({"synthetic-domain"})
    violations = checker.scan_files(identifiers, [binary, text_file])
    assert len(violations) == 1
    assert violations[0].path == text_file


def test_scan_files_reads_utf16_file(checker: ModuleType, tmp_path: Path) -> None:
    utf16_file = tmp_path / "utf16.txt"
    utf16_file.write_text("SYNTHETIC-DOMAIN in UTF-16\n", encoding="utf-16")
    identifiers = frozenset({"synthetic-domain"})
    violations = checker.scan_files(identifiers, [utf16_file])
    assert len(violations) == 1
    assert violations[0].path == utf16_file


def test_scan_files_ignores_short_identifiers(checker: ModuleType, tmp_path: Path) -> None:
    file_path = tmp_path / "notes.md"
    file_path.write_text("abc is short and should not match\n", encoding="utf-8")
    identifiers = frozenset({"abc"})
    assert checker.scan_files(identifiers, [file_path]) == []


def test_main_exits_zero_when_env_var_empty(
    monkeypatch: pytest.MonkeyPatch, checker: ModuleType
) -> None:
    monkeypatch.setenv("SLUICE_FORBIDDEN_IDENTIFIERS", "")

    def fake_run(*args: object, **kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(args=[], returncode=0, stdout="")

    monkeypatch.setattr(checker.subprocess, "run", fake_run)
    assert checker.main([]) == 0


def test_main_exits_one_on_violation(
    monkeypatch: pytest.MonkeyPatch, checker: ModuleType, tmp_path: Path
) -> None:

    file_path = tmp_path / "leaked.txt"
    file_path.write_text("Secret FAKEDOM value\n", encoding="utf-8")
    monkeypatch.setenv("SLUICE_FORBIDDEN_IDENTIFIERS", "FAKEDOM")

    def fake_run(*args: object, **kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(args=[], returncode=0, stdout=f"{file_path}\0")

    monkeypatch.setattr(checker.subprocess, "run", fake_run)
    assert checker.main([]) == 1


def test_main_exits_zero_when_no_violation(
    monkeypatch: pytest.MonkeyPatch, checker: ModuleType, tmp_path: Path
) -> None:
    file_path = tmp_path / "clean.txt"
    file_path.write_text("Nothing sensitive here.\n", encoding="utf-8")
    monkeypatch.setenv("SLUICE_FORBIDDEN_IDENTIFIERS", "FAKEDOM")

    def fake_run(*args: object, **kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(args=[], returncode=0, stdout=f"{file_path}\0")

    monkeypatch.setattr(checker.subprocess, "run", fake_run)
    assert checker.main([]) == 0


def test_staged_mode_scans_staged_diff(
    monkeypatch: pytest.MonkeyPatch, checker: ModuleType, tmp_path: Path
) -> None:
    """--staged routes through `git diff --cached` and still flags violations."""
    file_path = tmp_path / "staged.txt"
    file_path.write_text("Secret FAKEDOM value\n", encoding="utf-8")
    monkeypatch.setenv("SLUICE_FORBIDDEN_IDENTIFIERS", "FAKEDOM")

    seen: dict[str, list[str]] = {}

    def fake_run(args: list[str], **kwargs: object) -> CompletedProcess[str]:
        seen["args"] = args
        return CompletedProcess(args=args, returncode=0, stdout=f"{file_path}\0")

    monkeypatch.setattr(checker.subprocess, "run", fake_run)
    assert checker.main(["--staged"]) == 1
    assert seen["args"][:3] == ["git", "diff", "--cached"]
