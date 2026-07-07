"""Windows Service wrapper for sluice.

Requires pywin32 (``pip install sluice[windows]``).  On non-Windows platforms
the module imports but the service class is inert (no pywin32 dependency).

The service spawns ``sluice serve`` as a subprocess and waits for it.  On
stop, the subprocess is terminated.  This is the simplest reliable approach
that requires no refactoring of the CLI or uvicorn integration.

Usage (from an elevated PowerShell, in the venv)::

    python -m sluice.win_service install
    python -m sluice.win_service start

Or via the install-windows.ps1 script which handles everything.
"""

from __future__ import annotations

import os
import subprocess
import sys

_log_prefix = "sluice-service"

if sys.platform == "win32":
    import win32service  # type: ignore[import-not-found]
    import win32serviceutil  # type: ignore[import-not-found]
    import win32event  # type: ignore[import-not-found]
    import servicemanager  # type: ignore[import-not-found]

    _DEFAULT_CONFIG = os.path.join(
        os.environ.get("PROGRAMDATA", r"C:\ProgramData"), "sluice", "sluice.toml"
    )

    class SluiceService(win32serviceutil.ServiceFramework):  # type: ignore[misc]
        _svc_name_ = "sluice"
        _svc_display_name_ = "sluice"
        _svc_description_ = (
            "sluice -- concurrency-metering reverse proxy for LLM APIs"
        )

        def __init__(self, args: list[str]) -> None:
            super().__init__(args)
            self._stop_event = win32event.CreateEvent(None, 0, 0, None)
            self._proc: subprocess.Popen[bytes] | None = None
            self._config_path = os.environ.get("SLUICE_CONFIG", _DEFAULT_CONFIG)

        def SvcStop(self) -> None:
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            servicemanager.LogInfoMsg(f"{_log_prefix}: stop requested")
            self._terminate_proc()
            win32event.SetEvent(self._stop_event)

        def SvcDoRun(self) -> None:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            try:
                self._run_sluice()
            except Exception:
                servicemanager.LogErrorMsg(
                    f"{_log_prefix}: unhandled exception", exc_info=True
                )

        def _find_sluice_exe(self) -> tuple[str, list[str]]:
            exe_dir = os.path.dirname(sys.executable)
            sluice_exe = os.path.join(exe_dir, "sluice.exe")
            if os.path.exists(sluice_exe):
                return sluice_exe, ["serve", "--config", self._config_path]
            return sys.executable, [
                "-m", "sluice", "serve", "--config", self._config_path
            ]

        def _run_sluice(self) -> None:
            exe, base_args = self._find_sluice_exe()
            cmd = [exe] + base_args
            servicemanager.LogInfoMsg(f"{_log_prefix}: starting {cmd}")
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            while True:
                result = win32event.WaitForSingleObject(self._stop_event, 2000)
                if result == win32event.WAIT_OBJECT_0:
                    break
                if self._proc.poll() is not None:
                    rc = self._proc.returncode
                    servicemanager.LogWarningMsg(
                        f"{_log_prefix}: sluice exited (code {rc}), service stopping"
                    )
                    break
            self._terminate_proc()

        def _terminate_proc(self) -> None:
            if self._proc is None or self._proc.poll() is not None:
                return
            try:
                self._proc.terminate()
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                servicemanager.LogWarningMsg(
                    f"{_log_prefix}: sluice did not exit in 10s, killing"
                )
                self._proc.kill()
                self._proc.wait(timeout=5)
            except Exception:
                pass

    def _win_main() -> int:
        if len(sys.argv) == 1:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(SluiceService)
            servicemanager.StartServiceCtrlDispatcher()
            return 0
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
