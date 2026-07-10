"""sluice — concurrency-metering reverse proxy for LLM APIs.

The deterministic decision logic lives in :mod:`sluice.control` and is stdlib-only.
The async proxy/usage/CLI shell imports the core, never the reverse.
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    # Single source of truth: the installed package metadata (from pyproject).
    # This keeps __version__ from drifting out of sync with pyproject.toml —
    # the banner/status version is always whatever was built and installed.
    __version__ = _pkg_version("sluice")
except PackageNotFoundError:  # pragma: no cover - only when running uninstalled
    __version__ = "1.3.0"
