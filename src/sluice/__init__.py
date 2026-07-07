"""sluice — concurrency-metering reverse proxy for LLM APIs.

The deterministic decision logic lives in :mod:`sluice.control` and is stdlib-only.
The async proxy/usage/CLI shell imports the core, never the reverse.
"""

__version__ = "1.2.2"
