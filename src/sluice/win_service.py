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

Usage (from an elevated PowerShell, in the venv)::

    python -m sluice.win_service install
    python -m sluice.win_service start

Or via install-windows.ps1, which registers the service to launch this module.
"""

from __future__ import annotations

import os
import sys

import uvicorn

_LOG_PREFIX = "sluice-service"


def _redirect_std_streams() -> None:
    """Point stdout/stderr at a log file.

    A Windows service has no console — ``sys.stdout``/``sys.stderr`` may be
    ``None``, so uvicorn's and sluice's logging (and any traceback) would be
    lost or raise. Rebinding both to a file, *before* the app and uvicorn build
    their logging handlers, captures everything. Best-effort: never fatal.
    """
    logdir = os.path.join(
        os.environ.get("PROGRAMDATA", r"C:\ProgramData"), "sluice", "logs"
    )
    try:
        os.makedirs(logdir, exist_ok=True)
        stream = open(  # noqa: SIM115 - long-lived for the process lifetime
            os.path.join(logdir, "service.log"),
            "a",
            buffering=1,
            encoding="utf-8",
            errors="replace",
        )
        sys.stdout = stream
        sys.stderr = stream
    except OSError:
        pass


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

            app, host, port, log_level = build_service_app()
            config = uvicorn.Config(
                app,
                host=host,
                port=port,
                log_level=log_level,
                timeout_graceful_shutdown=30,
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
