# Plan 014 — Windows Service Support

**Target:** v1.2.0  
**MVP:** Install as a Windows service, dashboard available on localhost.

## Motivation

sluice is deployed on Kubernetes for the team, but individual Windows
workstations that want local concurrency metering (a developer pointing
opencode at sluice on their own machine) have no supported install path.
The Docker quickstart works but is heavyweight for a single-user box.

## Approach -- pywin32 Windows Service

sluice runs as a native Windows Service via `pywin32`. A small
`src/sluice/win_service.py` module subclasses
`win32serviceutil.ServiceFramework` and runs the uvicorn ASGI server
**in-process**: `SvcDoRun` builds the same app `sluice serve` would (via
the shared `build_service_app()` helper) and drives a `uvicorn.Server`;
`SvcStop` sets `should_exit` so uvicorn shuts down *gracefully* (stop
accepting, drain in-flight, run the ASGI lifespan shutdown).

**Why not NSSM?** The NSSM website (nssm.cc) was returning 503 during
development. NSSM is also an external binary download, whereas pywin32
is pip-installable (works offline once cached). The pywin32 approach
keeps everything in the Python ecosystem.

**Why not IIS (like cert-watch)?** cert-watch is a web app behind IIS.
sluice is a reverse proxy -- it needs to own its own listener, not be
hosted inside IIS's pipeline.

**Why in-process, not a `sluice serve` subprocess?** An earlier draft
spawned `sluice serve` as a child and hard-terminated it on stop. That
had two defects: the stop was non-graceful (in-flight streams dropped),
and the SCM supervised only the wrapper — if the child died the service
still read *Running*. In-process fixes both: the SCM supervises the real
server, and `should_exit` gives a clean drain. The cost was a small
refactor — `_cmd_serve` now splits config-resolution/app-building
(`_build_serve_app`, shared) from the `uvicorn.run` call, so the service
reuses the exact same app. Two Windows details this requires:
`_StoppableServer` overrides `install_signal_handlers` (SvcDoRun is not
the main thread), and the module redirects stdout/stderr to
`logs\service.log` (a service has no console).

## Install script: `scripts/install-windows.ps1`

Borrows the Python-finding logic from cert-watch's
`install-windows.ps1` (which handles the Python Install Manager,
per-user runtimes, Windows Store alias stubs, and shared-Python
copying). Adapted for sluice:

1. **Find Python 3.12+** — same probe sequence as cert-watch:
   shared Python from a prior install → Python Install Manager
   per-user runtimes → per-machine installs → bare PATH launchers.
   Skip Windows Store alias stubs (WI-050 equivalent).

2. **Copy user-scoped Python to a shared location** under InstallDir
   so the service (running as LocalSystem) can access it.

3. **Create venv** at `C:\ProgramData\sluice\venv`, install sluice with
   the `[windows]` extra (pulls in pywin32), then run
   `pywin32_postinstall.py -install` to register the service host DLLs.

4. **Create a TOML config file** at `C:\ProgramData\sluice\sluice.toml`
   if one does not exist (template with commented-out defaults).

5. **Register the Windows service** with `New-Service`:
   - Service name: `sluice`
   - Binary: `"<venv>\Scripts\python.exe" -m sluice.win_service`
   - Start mode: Automatic

   This deliberately points the service binary at bare `python.exe -m
   sluice.win_service` rather than pywin32's `pythonservice.exe`, which
   has DLL-resolution problems inside a venv. `win_service._win_main`
   handles the no-argument launch the SCM performs by calling
   `servicemanager.StartServiceCtrlDispatcher()` directly.

6. **Set the service environment and recovery.** Write `SLUICE_CONFIG`
   plus any of `SLUICE_USAGE_KEY` / `SLUICE_UPSTREAM` /
   `SLUICE_ADMIN_TOKEN` into the service's `Environment` MultiString
   registry value under
   `HKLM:\SYSTEM\CurrentControlSet\Services\sluice`; set recovery
   (restart after 10s) via `sc.exe failure`.

7. **Start the service** (`sc.exe start sluice`). Dashboard at
   `http://localhost:8800/`.

## Uninstall script: `scripts/uninstall-windows.ps1`

Stops and removes the service, optionally removes the data directory.

## Config

The service reads from a TOML config file (`C:\ProgramData\sluice\sluice.toml`)
plus environment variables. Secrets (`SLUICE_USAGE_KEY`, `SLUICE_ADMIN_TOKEN`)
are written by the install script into the service's `Environment`
MultiString value under `HKLM:\SYSTEM\CurrentControlSet\Services\sluice`, so
they are scoped to the service process and not visible in the machine-wide
environment.

## Windows compatibility notes

- `signal.SIGHUP` (used for config reload) does not exist on Windows.
  The handler in `lifecycle.py` already has a `try/except` guard that
  logs a warning and skips registration. `POST /admin/reload` works
  as the Windows alternative.
- No `os.fork`, `fcntl`, `grp`, `pwd`, or other Unix-only APIs are used.
- uvicorn's asyncio event loop works on Windows (ProactorEventLoop on 3.12+).

## Files

- `scripts/install-windows.ps1` — install + service creation
- `scripts/uninstall-windows.ps1` — service removal
- `deploy/windows/README.md` — Windows deployment docs
- `README.md` — add Windows quickstart section
- `CHANGELOG.md` — v1.2.0 entry
