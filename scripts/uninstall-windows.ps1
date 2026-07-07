<#
STYLE (same constraints as install-windows.ps1): Never embed single quotes
inside double-quoted strings. PowerShell 5.1 reads this file via the system
ANSI codearea when the UTF-8 BOM is missing. Keep this file ASCII-only and
prefer single-quoted literals. No non-ASCII characters -- use -- instead of
em-dash. Avoid PS 7+ syntax.

.SYNOPSIS
    Remove a sluice Windows service deployment.

.DESCRIPTION
    Stops and removes the sluice Windows service (created by
    install-windows.ps1 via pywin32). Re-running is safe: missing resources
    are skipped, never errored.

    Data is preserved by default. Pass -RemoveData to also delete the
    data directory (venv, config, logs, shared Python, history store).

.PARAMETER InstallDir
    Data directory used by the deployment. Default: C:\ProgramData\sluice

.PARAMETER ServiceName
    Windows service name. Default: sluice

.PARAMETER RemoveData
    DESTRUCTIVE. Also remove the data directory: venv, config, logs,
    shared Python, and history store. Without this flag the data is
    preserved so a re-install resumes where it left off.

.PARAMETER Force
    Skip the interactive confirmation that -RemoveData otherwise requires.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\uninstall-windows.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\uninstall-windows.ps1 -RemoveData -Force
#>
[CmdletBinding()]
param(
    [string]$InstallDir = "C:\ProgramData\sluice",
    [string]$ServiceName = "sluice",
    [switch]$RemoveData,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# --- Must be elevated ---
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run from an elevated (Administrator) PowerShell."
}

# --- Confirm destructive data removal ---
if ($RemoveData -and -not $Force) {
    Write-Warning "-RemoveData will delete $InstallDir, including the venv, config,"
    Write-Warning "logs, shared Python, and history store."
    $answer = Read-Host "Type the word remove to proceed, anything else to keep data"
    if ($answer -ne "remove") {
        Write-Host "Keeping data directory. Continuing with service removal only."
        $RemoveData = $false
    }
}

# --- 1. Stop the service ---
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc) {
    if ($svc.Status -eq "Running") {
        Write-Host "Stopping service `"$ServiceName`" ..."
        Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 3
    }
    $svcState = (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue).Status
    Write-Host "  Service status: $svcState"
} else {
    Write-Host "Service `"$ServiceName`" not found; skipping stop."
}

# --- 2. Remove the service via pywin32 (or sc.exe fallback) ---
$venvPy = Join-Path $InstallDir "venv\Scripts\python.exe"
if ($svc) {
    $removed = $false
    if (Test-Path $venvPy) {
        Write-Host "Removing service `"$ServiceName`" via pywin32 ..."
        & $venvPy -m win32serviceutil remove $ServiceName 2>$null
        if ($LASTEXITCODE -eq 0) { $removed = $true }
    }
    if (-not $removed) {
        Write-Host "Removing service via sc.exe ..."
        & sc.exe delete $ServiceName 2>$null | Out-Null
    }
    Start-Sleep -Seconds 2
    $svcCheck = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($svcCheck) {
        Write-Host "  [warn] Service still exists after removal -- it may be marked for deletion."
        Write-Host "         A reboot or closing the Services MMC may complete the removal."
    } else {
        Write-Host "  Service removed."
    }
} else {
    Write-Host "Service `"$ServiceName`" not found; skipping removal."
}

# --- 3. Optionally remove the data directory ---
if ($RemoveData) {
    if (Test-Path $InstallDir) {
        Write-Host "Removing data directory $InstallDir ..."
        Remove-Item $InstallDir -Recurse -Force -ErrorAction SilentlyContinue
        if (Test-Path $InstallDir) {
            Write-Host "  [warn] Some files could not be removed (locked?). Reboot and re-run."
        } else {
            Write-Host "  Data directory removed."
        }
    } else {
        Write-Host "Data directory $InstallDir not found; skipping."
    }
} else {
    Write-Host "Data directory $InstallDir preserved (pass -RemoveData to delete it)."
}

Write-Host ""
Write-Host "Done. sluice removed."
if (-not $RemoveData) {
    Write-Host "Re-run install-windows.ps1 to redeploy (venv and config are preserved)."
}
