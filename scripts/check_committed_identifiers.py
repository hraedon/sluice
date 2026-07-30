"""Mechanical gate against committing work-domain identifiers.

Two complementary checks:

1. Always-on (no configuration): no tracked file may live under ``samples/``.
   ``.gitignore`` is advisory — ``git add -f`` bypasses it — so this guard makes
   an accidental force-add of a real identifier-bearing data file fail CI. The
   ``samples/`` directory holds real environment data (hostnames, service
   accounts, principal handles) that must never be committed (AGENTS.md).

2. Secret-driven: when ``SLUICE_FORBIDDEN_IDENTIFIERS`` is set (a
   whitespace-separated list of real identifiers — hostnames, emails, service
   accounts, principal handles, personal names), every tracked text file
   outside ``samples/`` is scanned for those identifiers. This catches real
   names that leaked into docs, tests, or reflections. It is a no-op (exit 0)
   until the secret is configured, so it never blocks a fresh clone or a fork
   without the secret.

   **Multi-word identifiers are double-quoted** (``"two words"``) and match any
   separator run — spaced, hyphenated, underscored, dotted, or wrapped across a
   line break. Before this, the parser split unconditionally on whitespace, so a
   multi-word identifier could not be expressed at all: its halves became short
   tokens that the length filter dropped. A real two-word work-domain name sat
   undetected in sixteen repositories — eight of them public — because of that
   blind spot. Any denylist entry containing a space must stay quoted.

Run locally: python scripts/check_committed_identifiers.py
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path

MIN_IDENTIFIER_LENGTH = 4
# Separators a multi-word identifier may be written with. A two-word domain name
# appears in the wild as "two words", "two-words", "two_words", "two.words", and
# — in wrapped prose — with a line break between the words. A phrase entry
# matches all of those forms; see _phrase_pattern.
_PHRASE_SEPARATOR = r"[\s._\-]+"
_BINARY_SNIFF_LEN = 8192
# Dirs skipped by the identifier scan: .venv is build output. The always-on
# guard below handles root-level samples/ (which holds real identifier-bearing
# data); nested directories named samples/ (e.g. tests/samples/) are legitimate
# code dirs and SHOULD be scanned.
_SKIP_DIRS = frozenset({".venv"})
# Root-level gitignored data dirs that must never contain a tracked file. The
# guard matches the first path component so a legitimate nested code dir named
# ``samples`` (e.g. ``tests/samples/``) is not a false positive.
_GUARDED_DIRS = frozenset({"samples"})


@dataclass(frozen=True)
class Violation:
    identifier: str
    path: Path
    line_number: int
    line: str


class GateError(Exception):
    """A condition that prevents the gate from judging the tree.

    Raised instead of letting a traceback escape: a publication gate that cannot
    complete its scan must fail *clean* (exit 1), never look like a pass and
    never bury the reason in a stack trace.
    """


def _filter_identifiers(identifiers: frozenset[str]) -> frozenset[str]:
    """Lowercase, collapse internal whitespace, drop empty or short identifiers.

    Internal whitespace is collapsed to a single space so a phrase entry is
    normalized regardless of how it was spaced in the denylist; scan_text then
    matches any separator run.
    """
    return frozenset(
        " ".join(token.lower().split())
        for token in (i.strip() for i in identifiers)
        if len(" ".join(token.split())) >= MIN_IDENTIFIER_LENGTH
    )


def parse_identifier_set(raw: str) -> frozenset[str]:
    """Build a normalized set of identifiers from the raw denylist.

    Accepts whitespace-separated tokens (the CI-secret form) and/or one token
    per line. Full-line and trailing ``#`` comments are stripped, so a
    human-maintained denylist file may document itself without every comment
    word becoming a forbidden token.

    **Multi-word identifiers must be double-quoted** (``"two words"``). Before
    this, the parser split unconditionally on whitespace, so a multi-word
    identifier could not be *expressed* — the two halves became two short
    tokens, each dropped by the length filter. A real two-word work-domain name
    sat undetected in sixteen repositories because of that. Quoted entries are
    kept whole and matched with a flexible separator (see scan_text). The audit
    that found the leak is recorded in docs/publication-review.md.

    Raises ValueError on unbalanced quoting: a denylist we cannot parse must
    fail the gate loudly, never degrade to a partial token set.
    """
    tokens: set[str] = set()
    for line in raw.splitlines() or [raw]:
        content = line.split("#", 1)[0].strip()
        if not content:
            continue
        try:
            tokens.update(shlex.split(content))
        except ValueError as exc:  # unbalanced quote
            raise ValueError(
                f"denylist entry could not be parsed (check quoting): {exc}"
            ) from exc
    return _filter_identifiers(frozenset(tokens))


def _phrase_pattern(identifier: str) -> re.Pattern[str]:
    """Compile a multi-word identifier into a flexible-separator regex.

    Internal whitespace matches any run of whitespace, ``.``, ``_``, or ``-``,
    so one denylist entry covers the spaced, hyphenated, underscored, dotted,
    and line-wrapped spellings. Everything else is escaped literally.
    """
    parts = [re.escape(word) for word in identifier.split()]
    return re.compile(_PHRASE_SEPARATOR.join(parts), re.IGNORECASE)


def scan_text(text: str, identifiers: frozenset[str]) -> Iterator[Violation]:
    """Yield a violation for every occurrence of one of *identifiers*.

    The match is case-insensitive and counts any substring occurrence; real
    identifiers such as ``WORK-DOMAIN`` can legitimately appear inside longer
    tokens.

    Single-word identifiers are matched line by line. Multi-word identifiers are
    matched against the whole text with a flexible separator, so a phrase that
    prose wrapped across a line break is still caught; the reported line is the
    one the match starts on.
    """
    identifiers = _filter_identifiers(identifiers)
    if not identifiers:
        return
    words = frozenset(i for i in identifiers if " " not in i)
    phrases = frozenset(i for i in identifiers if " " in i)

    lines = text.splitlines()
    for line_number, line in enumerate(lines, start=1):
        lower = line.lower()
        for identifier in words:
            start = 0
            while True:
                offset = lower.find(identifier, start)
                if offset == -1:
                    break
                yield Violation(
                    identifier=identifier,
                    path=Path("."),
                    line_number=line_number,
                    line=line,
                )
                start = offset + len(identifier)

    if not phrases:
        return
    for identifier in phrases:
        for match in _phrase_pattern(identifier).finditer(text):
            line_number = text.count("\n", 0, match.start()) + 1
            yield Violation(
                identifier=identifier,
                path=Path("."),
                line_number=line_number,
                line=lines[line_number - 1] if line_number <= len(lines) else "",
            )


def _sniff_encoding(chunk: bytes) -> str | None:
    """Return the text encoding if *chunk* starts with a known BOM, else None."""
    if chunk.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if chunk.startswith(b"\xfe\xff"):
        return "utf-16-be"
    if chunk.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return None


def _is_binary(chunk: bytes) -> bool:
    """Heuristic: null byte present without a recognized text BOM → binary."""
    if _sniff_encoding(chunk) is not None:
        return False
    return b"\x00" in chunk


def scan_files(
    identifiers: frozenset[str],
    paths: list[Path],
    *,
    unreadable: list[Path] | None = None,
) -> list[Violation]:
    """Scan every readable text file in *paths* for forbidden identifiers.

    UTF-16 files (common in Windows tooling output) are detected via BOM and
    decoded correctly rather than misclassified as binary by the null-byte
    heuristic.

    Returns the violations. A tracked file the gate could not read is collected
    into *unreadable* when a list is supplied (WI-027): silently skipping an
    unreadable file lets one containing a forbidden identifier pass, which is
    precisely the fails-open case this gate exists to prevent.

    The out-parameter is deliberate. This script is COPIED into every repo in the
    estate and several of them test ``scan_files`` directly, so returning a tuple
    instead of a list broke seven repositories' test suites at once. An optional
    keyword collector keeps the signature backward compatible while still letting
    the CLI fail closed on an unreadable file.
    """
    violations: list[Violation] = []
    if unreadable is None:
        unreadable = []
    for path in paths:
        # A tracked symlink's blob content is its target path, not file data.
        # Scan the target string without following the link: following it either
        # leaves the repo (wrong thing to scan) or fails on a broken link and
        # looks like an unreadable file. The target itself can carry a forbidden
        # identifier, so it is scanned rather than skipped.
        if path.is_symlink():
            target = os.readlink(path)
            for violation in scan_text(target, identifiers):
                violations.append(replace(violation, path=path, line=target))
            continue
        try:
            with path.open("rb") as f:
                chunk = f.read(_BINARY_SNIFF_LEN)
        except OSError:
            unreadable.append(path)
            continue
        if _is_binary(chunk):
            continue
        encoding = _sniff_encoding(chunk) or "utf-8"
        try:
            text = path.read_text(encoding=encoding, errors="replace")
        except OSError:
            unreadable.append(path)
            continue
        for violation in scan_text(text, identifiers):
            violations.append(replace(violation, path=path))
    return violations


def _run_git(args: list[str]) -> str:
    """Run a git command and return stdout.

    A git failure raises GateError so the gate exits 1 with a readable reason
    (WI-027): a CI gate must fail clean, not emit a CalledProcessError traceback
    that reads as an infrastructure crash rather than a blocked publication.
    """
    # argv is passed through verbatim, bare "git" included. Resolving it to an
    # absolute path via shutil.which is arguably better hygiene, but this script is
    # COPIED into every repo and several of them assert on the exact argv
    # (`== ["git", "diff", "--cached"]`), so absolute paths broke three test
    # suites. The S607 partial-path finding that motivated it only ever applied to
    # check_publication_plumbing.py, whose literal argv ruff can see statically;
    # here the list is a parameter, so the rule does not fire. Respect the
    # fleet-wide contract.
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise GateError(
            f"git command failed ({' '.join(args)}): "
            f"exit {exc.returncode}: {(exc.stderr or '').strip()}"
        ) from exc
    except OSError as exc:
        raise GateError(f"could not run git ({' '.join(args)}): {exc}") from exc
    return result.stdout


def _paths_from_git(args: list[str]) -> list[Path]:
    """Run a NUL-delimited git path command and return Paths.

    No filtering is applied here — the always-on samples/ guard needs to see
    every tracked path so it can detect a force-add. The identifier scan
    filters out _SKIP_DIRS separately.
    """
    paths: list[Path] = []
    for raw in _run_git(args).split("\0"):
        if not raw:
            continue
        paths.append(Path(raw))
    return paths


def collect_tracked_paths() -> list[Path]:
    """Return tracked file paths from ``git ls-files``, excluding obvious skips."""
    return _paths_from_git(["git", "ls-files", "-z"])


def collect_range_messages(rev_range: str) -> list[tuple[str, str]]:
    """Return ``(sha, message)`` for every commit in *rev_range*.

    Commit messages are a publication channel the content gate never covered:
    the tracked-tree scan reads files, so an identifier named only in a message
    is invisible to it. That blind spot is not hypothetical — a public repo
    carried work-domain identifiers in three commit messages, two of which were
    the very commits that redacted those identifiers from the files. The message
    described what the diff removed.
    """
    # rev_range may carry several git-log arguments (the pre-push new-branch case
    # passes "<sha> --not --remotes=<name>"), so it is split rather than passed
    # as one opaque argument.
    result = _run_git(
        ["git", "log", "--format=%H%x1f%B%x1e", *rev_range.split()],
    )
    messages: list[tuple[str, str]] = []
    for record in result.split("\x1e"):
        if "\x1f" not in record:
            continue
        sha, body = record.split("\x1f", 1)
        messages.append((sha.strip(), body))
    return messages


def collect_staged_paths() -> list[Path]:
    """Return staged (added/copied/modified/renamed) paths for the pre-commit hook.

    Scans only what is about to be committed rather than the whole tree, so the
    local gate is fast enough to run on every commit. Deletions are excluded
    (``--diff-filter=ACM``) because there is nothing to scan. ``--no-renames``
    decomposes renames into add+delete so the new path (e.g. a file moved into
    ``samples/``) is included as an addition and caught by the always-on guard.
    """
    return _paths_from_git(
        [
            "git", "diff", "--cached", "--name-only",
            "--diff-filter=ACM", "--no-renames", "-z",
        ]
    )


def print_report(violations: list[Violation]) -> None:
    violations.sort(key=lambda v: (str(v.path), v.line_number, v.identifier))
    print("Committed identifier violations detected:", file=sys.stderr)
    for v in violations:
        print(f"  {v.path}:{v.line_number}: {v.identifier!r}", file=sys.stderr)
        print(f"      {v.line.rstrip()}", file=sys.stderr)
    print(f"\nTotal: {len(violations)} violation(s)", file=sys.stderr)


def leaked_tracked_files(paths: list[Path], guarded: frozenset[str]) -> list[Path]:
    """Tracked files whose root component is a guarded (gitignored) data dir.

    Matches only the first path component so a nested code directory that happens
    to be named ``samples`` (e.g. ``tests/samples/``) is not a false positive.
    """
    return [p for p in paths if p.parts and p.parts[0] in guarded]


def _resolve_identifiers() -> frozenset[str] | None:
    """Return the configured denylist, or None if the gate should no-op.

    Shared by the message-scanning modes so they honor exactly the same
    configured/unconfigured semantics as the tracked-tree scan.
    """
    raw = os.environ.get("SLUICE_FORBIDDEN_IDENTIFIERS", "")
    if not raw.strip():
        print(
            "SLUICE_FORBIDDEN_IDENTIFIERS is empty or unset; skipping identifier gate.",
            file=sys.stderr,
        )
        return None
    identifiers = parse_identifier_set(raw)
    if not identifiers:
        print(
            "SLUICE_FORBIDDEN_IDENTIFIERS contained no usable identifiers (minimum "
            f"length is {MIN_IDENTIFIER_LENGTH} characters); skipping gate.",
            file=sys.stderr,
        )
        return None
    return identifiers


def _report_message_violations(label: str, violations: list[Violation]) -> None:
    print(f"Forbidden identifier in {label}:", file=sys.stderr)
    for v in sorted(violations, key=lambda v: (v.line_number, v.identifier)):
        print(f"  line {v.line_number}: {v.identifier!r}", file=sys.stderr)
        print(f"      {v.line.rstrip()}", file=sys.stderr)
    print(
        "\nA commit message is published with the commit. Rewrite the message "
        "without the identifier (the canonical denylist is the authority on what "
        "may not appear).",
        file=sys.stderr,
    )


def _scan_message_file(path: Path) -> int:
    """commit-msg hook mode: scan the proposed commit message."""
    identifiers = _resolve_identifiers()
    if identifiers is None:
        return 0
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise GateError(f"could not read the commit message file {path}: {exc}") from exc
    # git puts everything after a scissors line out of the commit; comment lines
    # are stripped too. Scan only what will actually be recorded.
    kept = [ln for ln in text.splitlines() if not ln.startswith("#")]
    violations = list(scan_text("\n".join(kept), identifiers))
    if violations:
        _report_message_violations("the proposed commit message", violations)
        return 1
    return 0


def _scan_rev_range(rev_range: str) -> int:
    """pre-push mode: scan every commit message about to be published."""
    identifiers = _resolve_identifiers()
    if identifiers is None:
        return 0
    failed = False
    for sha, body in collect_range_messages(rev_range):
        violations = list(scan_text(body, identifiers))
        if violations:
            _report_message_violations(f"commit message {sha[:9]}", violations)
            failed = True
    return 1 if failed else 0


def _run(args: argparse.Namespace) -> int:
    if args.message_file is not None:
        return _scan_message_file(Path(args.message_file))
    if args.rev_range is not None:
        return _scan_rev_range(args.rev_range)

    paths = collect_staged_paths() if args.staged else collect_tracked_paths()

    # 1. Always-on: no tracked file under a guarded (gitignored) data dir. This
    #    catches a ``git add -f samples/...`` leak regardless of secret config.
    leaked = leaked_tracked_files(paths, _GUARDED_DIRS)
    if leaked:
        print("Tracked files under a gitignored data directory detected:", file=sys.stderr)
        for p in sorted(leaked, key=str):
            print(f"  {p}", file=sys.stderr)
        print(
            "\nThese paths are gitignored by convention (samples/ holds real "
            "identifier-bearing data — hostnames, service accounts, principal "
            "handles). Remove them from the index: git rm --cached -r <path>.",
            file=sys.stderr,
        )
        return 1

    # 2. Secret-driven: scan tracked text files (outside guarded dirs) for
    #    forbidden identifiers. No-op until the secret is configured.
    raw = os.environ.get("SLUICE_FORBIDDEN_IDENTIFIERS", "")
    if not raw.strip():
        print(
            "SLUICE_FORBIDDEN_IDENTIFIERS is empty or unset; skipping identifier gate.",
            file=sys.stderr,
        )
        return 0

    identifiers = parse_identifier_set(raw)
    if not identifiers:
        print(
            "SLUICE_FORBIDDEN_IDENTIFIERS contained no usable identifiers (minimum "
            f"length is {MIN_IDENTIFIER_LENGTH} characters); skipping gate.",
            file=sys.stderr,
        )
        return 0

    scan_paths = [p for p in paths if not any(part in _SKIP_DIRS for part in p.parts)]
    unreadable: list[Path] = []
    violations = scan_files(identifiers, scan_paths, unreadable=unreadable)
    if violations:
        print_report(violations)
        return 1
    if unreadable:
        print("Tracked files could not be read; the gate cannot clear them:", file=sys.stderr)
        for p in sorted(unreadable, key=str):
            print(f"  {p}", file=sys.stderr)
        print(
            "\nAn unreadable tracked file may contain a forbidden identifier. Fix the "
            "permissions (or untrack the file) and re-run; the gate will not pass a "
            "tree it could not fully scan.",
            file=sys.stderr,
        )
        return 1

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gate that prevents committing forbidden domain identifiers.",
    )
    parser.add_argument(
        "--staged",
        action="store_true",
        help="Scan only staged files (for the pre-commit hook) instead of the "
        "full tracked tree (the CI default).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--message-file",
        metavar="PATH",
        help="Scan a proposed commit message (for the commit-msg hook) instead "
        "of files. Comment lines are ignored, as git strips them.",
    )
    mode.add_argument(
        "--rev-range",
        metavar="RANGE",
        help="Scan the commit messages in a git rev range (for the pre-push "
        "hook), e.g. origin/main..HEAD.",
    )
    args = parser.parse_args(argv)

    try:
        return _run(args)
    except GateError as exc:
        print(f"identifier gate could not complete: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        # Unparseable denylist (bad quoting). Fail closed, loudly.
        print(f"identifier gate denylist is invalid: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
