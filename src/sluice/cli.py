"""`sluice` command-line entry point.

Charter-stage stub: argument surface is defined so the console script resolves and the
shape is reviewable; `serve`/`status` are implemented in Plan 001 (WI-005).
"""

from __future__ import annotations

import argparse
import sys

from sluice import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sluice", description=__doc__)
    parser.add_argument("--version", action="version", version=f"sluice {__version__}")
    sub = parser.add_subparsers(dest="command")

    serve = sub.add_parser("serve", help="run the concurrency-metering reverse proxy")
    serve.add_argument("--upstream", required=True, help="upstream base URL, e.g. https://api.code.umans.ai")
    serve.add_argument("--listen", default="127.0.0.1:8800", help="host:port to listen on")
    serve.add_argument("--target", type=int, default=3, help="target max observed concurrency (default: 3)")
    serve.add_argument("--poll-interval", type=float, default=5.0, help="seconds between /v1/usage polls")
    serve.add_argument("--release-cooldown", type=float, default=2.0, help="seconds a freed permit rests")
    serve.add_argument("--usage-key-env", default="SLUICE_USAGE_KEY", help="env var holding the usage API key")

    sub.add_parser("status", help="print current reading, computed permits, and band")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in (None, "serve", "status"):
        # Implemented in Plan 001 (WI-005). Fail loudly until then.
        print(f"sluice {__version__}: '{args.command or 'help'}' not yet implemented (see plans/001).", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
