"""Windows Service wrapper for sluice.

Requires pywin32 (``pip install sluice[windows]``). On non-Windows platforms
the module imports but the service class is inert.

The service runs the uvicorn ASGI server **in-process** (not as a subprocess):
``SvcDoRun`` builds the same app ``sluice serve`` would and drives a
:class:`uvicorn.Server`; ``SvcStop`` sets ``should_exit`` so uvicorn performs a
*graceful* shutdown (stop accepting, drain in-flight, run the ASGI lifespan
shutdown) rather than a hard kill. This means the SCM supervises the real
server — if it dies, the service dies — instead of a wrapper that can report
Running while a child process is dead.

Logging (Windows service only — elsewhere sluice logs to stdout and the
platform owns rotation): the full ``logging`` stream (sluice + uvicorn) goes to
a size-rotated ``logs\\service.log``; notable events (``WARNING`` and above)
are *also* mirrored to the Windows **Event Log** under the ``sluice`` source so
Event Viewer / WEF / monitoring agents see crashes, breaker trips, box
detection, etc. without tailing a file.

Usage (from an elevated PowerShell, in the venv)::

    python -m sluice.win_service install
    python -m sluice.win_service start

Or via install-windows.ps1, which registers the service to launch this module.
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import uvicorn

_LOG_PREFIX = "sluice-service"

# service.log rotation: 5 MB × 5 files = 25 MB ceiling.
_LOG_MAX_BYTES = 5 * 1024 * 1024
_LOG_BACKUPS = 5


def _log_dir() -> str:
    return os.path.join(
        os.environ.get("PROGRAMDATA", r"C:\ProgramData"), "sluice", "logs"
    )


def _redirect_std_streams() -> None:
    """Guard against a console-less service.

    A Windows service has no console — ``sys.stdout``/``sys.stderr`` may be
    ``None``, so a stray ``print`` or an interpreter-level traceback would
    raise. Point both at a small per-run sink (truncated each start, so it
    stays bounded); the *structured* logs go to the rotated ``service.log`` set
    up in :func:`_configure_service_logging`. Best-effort: never fatal.
    """
    try:
        os.makedirs(_log_dir(), exist_ok=True)
        stream = open(  # noqa: SIM115 - long-lived for the process lifetime
            os.path.join(_log_dir(), "service.stdio.log"),
            "w",
            buffering=1,
            encoding="utf-8",
            errors="replace",
        )
        sys.stdout = stream
        sys.stderr = stream
    except OSError:
        pass


def _configure_service_logging(level: int = logging.INFO) -> None:
    """Attach the rotated file handler and the Event Log handler to root.

    Called before building the app, so ``logging.basicConfig`` in the shared
    serve path becomes a no-op (root already has handlers) and everything —
    including uvicorn, which is run with ``log_config=None`` so its loggers
    propagate to root — lands in these handlers.
    """
    root = logging.getLogger()
    root.setLevel(level)

    try:
        os.makedirs(_log_dir(), exist_ok=True)
        file_handler = RotatingFileHandler(
            os.path.join(_log_dir(), "service.log"),
            maxBytes=_LOG_MAX_BYTES,
            backupCount=_LOG_BACKUPS,
            encoding="utf-8",
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        root.addHandler(file_handler)
    except OSError:
        pass

    # Mirror notable events (WARNING+) to the Windows Event Log. Best-effort:
    # registering the source or ReportEvent can fail; the file log still works.
    try:
        from logging.handlers import NTEventLogHandler

        evt_handler = NTEventLogHandler("sluice")
        evt_handler.setLevel(logging.WARNING)
        evt_handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        root.addHandler(evt_handler)
    except Exception as exc:  # noqa: BLE001 - Event Log is optional
        logging.getLogger("sluice.win_service").warning(
            "Windows Event Log handler unavailable: %r", exc
        )


class _StoppableServer(uvicorn.Server):
    """A uvicorn server that survives running off the main thread.

    ``SvcDoRun`` executes on a pywin32 worker thread, where Python cannot
    install signal handlers (and the SCM delivers stop via ``SvcStop``, not
    signals). Overriding the installer to a no-op avoids the ``ValueError``
    uvicorn would otherwise raise at startup.
    """

    def install_signal_handlers(self) -> None:  # pragma: no cover - Windows only
        return None


if sys.platform == "win32":
    import servicemanager  # type: ignore[import-not-found]
    import win32event  # type: ignore[import-not-found]
    import win32service  # type: ignore[import-not-found]
    import win32serviceutil  # type: ignore[import-not-found]

    class SluiceService(win32serviceutil.ServiceFramework):  # type: ignore[misc]
        _svc_name_ = "sluice"
        _svc_display_name_ = "sluice"
        _svc_description_ = (
            "sluice -- concurrency-metering reverse proxy for LLM APIs"
        )

        def __init__(self, args: list[str]) -> None:
            super().__init__(args)
            self._stop_event = win32event.CreateEvent(None, 0, 0, None)
            self._server: _StoppableServer | None = None

        def SvcStop(self) -> None:
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            server = self._server
            if server is not None:
                # uvicorn polls should_exit ~10x/s, then drains gracefully.
                server.should_exit = True
            win32event.SetEvent(self._stop_event)

        def SvcDoRun(self) -> None:
            _redirect_std_streams()
            servicemanager.LogInfoMsg(f"{_LOG_PREFIX}: starting")
            try:
                self._serve()
            except Exception as exc:  # noqa: BLE001 - surface to SCM + event log
                servicemanager.LogErrorMsg(f"{_LOG_PREFIX}: crashed: {exc!r}")
                raise
            servicemanager.LogInfoMsg(f"{_LOG_PREFIX}: stopped")

        def _serve(self) -> None:
            from sluice.cli import build_service_app

            _configure_service_logging()
            app, host, port, log_level = build_service_app()
            logging.getLogger().setLevel(
                getattr(logging, log_level.upper(), logging.INFO)
            )
            config = uvicorn.Config(
                app,
                host=host,
                port=port,
                log_level=log_level,
                timeout_graceful_shutdown=30,
                # Propagate uvicorn's loggers to root so they hit the rotated
                # file + Event Log handlers instead of uvicorn's own stdout.
                log_config=None,
            )
            self._server = _StoppableServer(config)
            self._server.run()

    def _win_main() -> int:
        if len(sys.argv) == 1:
            # Launched by the SCM: become the service control dispatcher.
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(SluiceService)
            servicemanager.StartServiceCtrlDispatcher()
            return 0
        # install / remove / start / stop / debug verbs.
        win32serviceutil.HandleCommandLine(SluiceService)
        return 0

else:

    def _win_main() -> int:
        return 1


def main() -> int:
    if sys.platform != "win32":
        print("sluice win_service is only available on Windows", file=sys.stderr)
        return 1
    return _win_main()


if __name__ == "__main__":
    raise SystemExit(main())
