"""Pre-push plumbing guard: is this push going where the repo says it may?

The content gates (``check_committed_identifiers.py``) answer "may these bytes be
published". They cannot answer "may this repository be published *here*, by this
identity, at this visibility" — the plumbing accident class:

* pushing a private-until-review repo to a public remote;
* pushing with the wrong author identity on a multi-identity box;
* pushing to a remote owned by someone other than the declared owner.

Content and plumbing guards overlap but neither subsumes the other. A repository
was recreated twice for content leaks whose *plumbing* was fine, and separately
carried work-domain identifiers in commit messages that every content scan
missed. This guard covers the plumbing half, driven by a small tracked
declaration (``publication.toml``) so the expected state is reviewable in-repo
rather than living in an operator's habits.

**Honest threat model** (adapted from PropterMaltwo's gh-identity-guard): this is
accident prevention, layer 3/4 in the process-calibration enforcement order
(store > CI > githooks > harness hooks). A git hook is not a security boundary —
it is bypassable with ``--no-verify`` and absent in a fresh clone until
``scripts/install-git-hooks.sh`` runs. It stops the realistic accident, not a
determined operator.

Run manually:  python scripts/check_publication_plumbing.py --remote-url <url>
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import assert_never

DECLARATION_FILENAME = "publication.toml"


class Visibility(StrEnum):
    """What a repository declares about its own publication state.

    A closed set dispatched with match/assert_never, so adding a state that the
    guard does not handle is a type error rather than a silent pass.
    """

    PRIVATE_UNTIL_REVIEW = "private-until-review"
    PUBLIC = "public"


class PlumbingError(Exception):
    """The guard could not evaluate the push. Fails closed (exit 1), no traceback."""


@dataclass(frozen=True)
class Declaration:
    remote_owner: str
    # One or more expected author identities. A list is not a convenience: a repo
    # with real history legitimately carries several (a web-UI commit lands as
    # <user>@users.noreply.github.com, a CI bot has its own address), and the
    # check covers every commit in the push range, not just the tip. A
    # single-identity field would force either a false refusal or no check at all.
    author_emails: tuple[str, ...]
    visibility: Visibility


def load_declaration(repo_root: Path) -> Declaration | None:
    """Read publication.toml, or None when the repo has not declared one.

    A missing declaration is a no-op, not a failure: the guard must not brick a
    fresh clone or a repo that has not opted in. That is a deliberate fail-open
    on *absence* — and the reason CI, not this hook, is the hard gate.
    """
    path = repo_root / DECLARATION_FILENAME
    if not path.is_file():
        return None
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PlumbingError(f"{DECLARATION_FILENAME} could not be parsed: {exc}") from exc

    section = raw.get("publication")
    if not isinstance(section, dict):
        raise PlumbingError(f"{DECLARATION_FILENAME} has no [publication] table")

    missing = [
        k for k in ("remote_owner", "author_email", "visibility") if k not in section
    ]
    if missing:
        raise PlumbingError(
            f"{DECLARATION_FILENAME} [publication] is missing: {', '.join(missing)}"
        )

    raw_visibility = str(section["visibility"])
    try:
        visibility = Visibility(raw_visibility)
    except ValueError as exc:
        raise PlumbingError(
            f"{DECLARATION_FILENAME} declares visibility={raw_visibility!r}; "
            f"expected one of {[v.value for v in Visibility]}"
        ) from exc
    raw_authors = section["author_email"]
    authors = (
        tuple(str(a) for a in raw_authors)
        if isinstance(raw_authors, list)
        else (str(raw_authors),)
    )
    if not authors or not all(a.strip() for a in authors):
        raise PlumbingError(
            f"{DECLARATION_FILENAME} author_email must be a non-empty address "
            f"or list of addresses"
        )
    return Declaration(
        remote_owner=str(section["remote_owner"]),
        author_emails=authors,
        visibility=visibility,
    )


def parse_remote_owner(url: str) -> str | None:
    """Extract the owner from an https or ssh GitHub remote URL.

    Returns None when the URL shape is unrecognized; the caller treats that as a
    refusal rather than a pass, because an unparseable remote is exactly the case
    where a guard must not guess.
    """
    url = url.strip()
    # git@host:owner/repo.git  |  ssh://git@host/owner/repo.git
    # https://host/owner/repo(.git)
    match = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", url)
    if not match:
        return None
    return match.group(1)


def git_output(args: list[str], repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise PlumbingError(
            f"git {' '.join(args)} failed: exit {exc.returncode}: {(exc.stderr or '').strip()}"
        ) from exc
    except OSError as exc:
        raise PlumbingError(f"could not run git: {exc}") from exc
    return result.stdout.strip()


def author_emails_in_range(repo_root: Path, rev_range: str | None) -> list[str]:
    """Distinct author emails about to be published.

    Checks every commit in the push range, not just HEAD: a wrong-identity commit
    buried mid-branch is published just as surely as the tip.
    """
    args = ["log", "--format=%ae"]
    # Split for the same reason as collect_range_messages: the new-branch range
    # is several git-log arguments, not one revision.
    args.extend(rev_range.split() if rev_range else ["-1"])
    out = git_output(args, repo_root)
    return sorted({line.strip() for line in out.splitlines() if line.strip()})


def remote_visibility(owner: str, repo: str) -> str | None:
    """Actual visibility per the GitHub CLI, or None if it cannot be determined.

    Returns None when ``gh`` is absent or the call fails — offline pushes must
    stay possible. The caller warns rather than blocks in that case and says so,
    because a guard that silently cannot check is worse than one that admits it.
    """
    if shutil.which("gh") is None:
        return None
    try:
        result = subprocess.run(
            ["gh", "repo", "view", f"{owner}/{repo}", "--json", "visibility"],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        payload = json.loads(result.stdout)
    except (subprocess.SubprocessError, OSError, json.JSONDecodeError):
        return None
    visibility = payload.get("visibility")
    return str(visibility).upper() if visibility else None


def parse_remote_repo(url: str) -> str | None:
    match = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", url.strip())
    if not match:
        return None
    return match.group(2)


def check(
    repo_root: Path,
    remote_url: str,
    rev_range: str | None,
) -> list[str]:
    """Return a list of refusal reasons; empty means the push may proceed."""
    declaration = load_declaration(repo_root)
    if declaration is None:
        print(
            f"publication plumbing guard: no {DECLARATION_FILENAME} in this repo; "
            "skipping (CI remains the hard gate).",
            file=sys.stderr,
        )
        return []

    problems: list[str] = []

    owner = parse_remote_owner(remote_url)
    if owner is None:
        problems.append(
            f"could not parse an owner from the push URL {remote_url!r}; refusing rather "
            f"than guessing. Expected a GitHub https or ssh remote."
        )
    elif owner.lower() != declaration.remote_owner.lower():
        problems.append(
            f"remote owner mismatch: pushing to {owner!r} but {DECLARATION_FILENAME} "
            f"declares {declaration.remote_owner!r}. If the new remote is intended, "
            f"update the declaration in the same commit."
        )

    emails = author_emails_in_range(repo_root, rev_range)
    allowed = {a.lower() for a in declaration.author_emails}
    unexpected = [e for e in emails if e.lower() not in allowed]
    if unexpected:
        declared = ", ".join(repr(a) for a in declaration.author_emails)
        problems.append(
            f"author identity mismatch: {', '.join(unexpected)} in the commits being "
            f"pushed, but {DECLARATION_FILENAME} declares {declared}. Fix with: "
            f"git commit --amend --reset-author (or rebase to correct history). If the "
            f"identity is legitimate, add it to author_email."
        )

    match declaration.visibility:
        case Visibility.PRIVATE_UNTIL_REVIEW:
            repo_name = parse_remote_repo(remote_url)
            actual = remote_visibility(owner, repo_name) if owner and repo_name else None
            if actual is None:
                print(
                    "publication plumbing guard: could not verify remote visibility "
                    "(gh unavailable or call failed). The declaration says "
                    "private-until-review; verify by hand before publishing.",
                    file=sys.stderr,
                )
            elif actual != "PRIVATE":
                problems.append(
                    f"visibility mismatch: {DECLARATION_FILENAME} declares "
                    f"{Visibility.PRIVATE_UNTIL_REVIEW.value} but the remote is {actual}. "
                    f"Either make "
                    f"the remote private again, or — if a publication review has cleared "
                    f"it — set visibility = \"public\" in the declaration."
                )
        case Visibility.PUBLIC:
            pass
        case _ as unreachable:  # pragma: no cover - validated in load_declaration
            assert_never(unreachable)

    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Pre-push guard: verify remote owner, author identity, and "
        "declared visibility before a publication-sensitive push.",
    )
    parser.add_argument("--remote-url", required=True, help="The URL git is pushing to.")
    parser.add_argument(
        "--rev-range",
        default=None,
        help="Commit range being pushed (e.g. origin/main..HEAD). Defaults to HEAD only.",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (defaults to the git toplevel of the cwd).",
    )
    args = parser.parse_args(argv)

    try:
        if args.repo_root:
            repo_root = Path(args.repo_root)
        else:
            repo_root = Path(git_output(["rev-parse", "--show-toplevel"], Path.cwd()))
        problems = check(repo_root, args.remote_url, args.rev_range)
    except PlumbingError as exc:
        print(f"publication plumbing guard could not complete: {exc}", file=sys.stderr)
        return 1

    if problems:
        print("Push refused by the publication plumbing guard:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\nThis guard prevents accidents (wrong remote, wrong identity, "
            "unreviewed visibility); it is not a security boundary. Override with "
            "git push --no-verify only if you are certain.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
