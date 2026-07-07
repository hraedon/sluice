<#
STYLE: Never embed single quotes inside double-quoted strings. PowerShell 5.1
reads this file via the system ANSI codearea when the UTF-8 BOM is missing
(e.g. GitHub zip download), and multi-byte UTF-8 sequences corrupt the
parser quote-tracking state -- every subsequent quote inside "..." becomes a
fatal parse error. Use `" `"` (escaped double quotes) or restructure instead.
Also: this script must run on PowerShell 5.1 (the Windows default). Avoid
PS 7+ syntax: no ?? (null-coalescing), no ternary operator, no pipeline
chain operators (&& / ||). Use if/else and -or/-and instead. No non-ASCII
characters -- use -- instead of em-dash.

.SYNOPSIS
    Install sluice as a Windows service via pywin32.

.DESCRIPTION
    Creates the data directory, a virtualenv, installs sluice (with the
    [windows] extra for pywin32), registers sluice as a Windows service,
    and starts it. The dashboard is then available at
    http://localhost:8800/.

    Re-running is safe: existing venv is upgraded, config is preserved,
    and the service is reconfigured in place.

.PARAMETER InstallDir
    Base directory for data, venv, logs, shared Python.
    Default: C:\ProgramData\sluice

.PARAMETER ServiceName
    Windows service name. Default: sluice

.PARAMETER Upstream
    Upstream base URL (e.g. https://api.code.umans.ai).

.PARAMETER UsageKey
    API key for /v1/usage polling. Sets SLUICE_USAGE_KEY on the service.

.PARAMETER AdminToken
    Token to gate the dashboard and admin routes. Optional.

.PARAMETER Listen
    host:port to listen on. Default: 127.0.0.1:8800

.PARAMETER Target
    Target max concurrency. Default: 3

.PARAMETER Provider
    Upstream provider type: umans, anthropic, openai, generic. Default: umans

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\install-windows.ps1 -Upstream https://api.code.umans.ai -UsageKey sk-...

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\install-windows.ps1 -Upstream https://api.anthropic.com -Provider anthropic

.NOTES
    This script is not signed. If your execution policy blocks unsigned scripts,
    either bypass it per-invocation (see example above) or sign the script.
#>
[CmdletBinding()]
param(
    [string]$InstallDir = "C:\ProgramData\sluice",
    [string]$ServiceName = "sluice",
    [string]$Upstream = "",
    [string]$UsageKey = "",
    [string]$AdminToken = "",
    [string]$Listen = "127.0.0.1:8800",
    [int]$Target = 3,
    [string]$Provider = "umans"
)

$ErrorActionPreference = "Stop"

# --- Must be elevated ---
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run from an elevated (Administrator) PowerShell."
}

$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$venv     = Join-Path $InstallDir "venv"
$logs     = Join-Path $InstallDir "logs"
$configPath = Join-Path $InstallDir "sluice.toml"

# ===========================================================================
# 1. Find Python 3.12+
#    (Borrowed from cert-watch/scripts/install-windows.ps1)
# ===========================================================================

function Invoke-PyProbe {
    param([string]$Exe, [string[]]$Arguments)
    $argStr = ($Arguments | ForEach-Object { if ($_ -match "\s") { "`"$_`"" } else { $_ } }) -join " "
    $tmp = Join-Path $env:TEMP "sluice-py-probe.txt"
    & cmd /c "`"$Exe`" $argStr > `"$tmp`" 2>&1"
    $exit = $LASTEXITCODE
    $out = ""
    if (Test-Path $tmp) {
        $out = (Get-Content $tmp -Raw)
        Remove-Item $tmp -Force
    }
    @{ ExitCode = $exit; Output = if ($out) { $out.Trim() } else { "" } }
}

$launchers = @()
$sharedCandidate = Join-Path $InstallDir "python\python.exe"
if (Test-Path $sharedCandidate) { $launchers += @{ Exe = $sharedCandidate; Args = @() } }
$imRoot = Join-Path $env:LOCALAPPDATA "Python"
foreach ($pc in (Get-ChildItem $imRoot -Filter "pythoncore-*" -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending)) {
    $p = Join-Path $pc.FullName "python.exe"
    if (Test-Path $p) { $launchers += @{ Exe = $p; Args = @() } }
}
foreach ($base in @($env:ProgramFiles, ${env:ProgramFiles(x86)})) {
    if (-not $base) { continue }
    foreach ($d in (Get-ChildItem $base -Filter "Python3*" -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending)) {
        $p = Join-Path $d.FullName "python.exe"
        if (Test-Path $p) { $launchers += @{ Exe = $p; Args = @() } }
    }
}
foreach ($n in @("python3.exe", "python.exe")) {
    $p = Join-Path (Join-Path $imRoot "bin") $n
    if (Test-Path $p) { $launchers += @{ Exe = $p; Args = @() } }
}
$launchers += @(
    @{ Exe = "py";      Args = @("-3.14") },
    @{ Exe = "py";      Args = @("-3.12") },
    @{ Exe = "py";      Args = @("-3") },
    @{ Exe = "python";  Args = @() },
    @{ Exe = "python3"; Args = @() }
)

$python = $null
$major = 0
$minor = 0
foreach ($l in $launchers) {
    $label = "$($l.Exe) $($l.Args -join `" `")"
    $cmd = Get-Command $l.Exe -ErrorAction SilentlyContinue
    if (-not $cmd) { continue }
    if ($cmd.Source -and $cmd.Source -match "\\WindowsApps\\") { continue }
    $probeArgs = $l.Args + @("--version")
    $r = Invoke-PyProbe -Exe $l.Exe -Arguments $probeArgs
    if ($r.ExitCode -ne 0) { continue }
    $ver = ($r.Output -split "`n" | Where-Object { $_ -match "^Python\s+\d" } | Select-Object -First 1).Trim()
    if ($ver -match "Python\s+(\d+)\.(\d+)") {
        $major = [int]$Matches[1]; $minor = [int]$Matches[2]
        if ($major -ge 3 -and $minor -ge 12) {
            $resolved = ""
            try {
                $selfProbe = Invoke-PyProbe -Exe $l.Exe -Arguments ($l.Args + @("-c", "import sys; print(sys.executable)"))
                if ($selfProbe.ExitCode -eq 0) {
                    $candidate = ($selfProbe.Output -split "`n" | Select-Object -First 1).Trim()
                    if ($candidate -and (Test-Path $candidate -ErrorAction SilentlyContinue)) {
                        $resolved = $candidate
                    }
                }
            } catch { }
            if ($resolved) {
                Write-Host "  [ok]   $label -- $ver (resolved: $resolved)"
                $python = @{ Exe = $resolved; Args = @() }
            } else {
                Write-Host "  [ok]   $label -- $ver (using launcher directly)"
                $python = $l
            }
            break
        }
    }
}
if (-not $python) {
    throw "Python 3.12+ not found. Install it (winget install Python.Python.3.14) and re-run."
}

# ===========================================================================
# 2. Ensure Python is in a shared (non-user-profile) location
# ===========================================================================

$sharedPyDir = Join-Path $InstallDir "python"
$sharedPyExe = Join-Path $sharedPyDir "python.exe"
if ($python.Exe -like "*\AppData\*" -or $python.Exe -like "*\WindowsApps\*") {
    if (Test-Path $sharedPyExe) {
        Write-Host "Using existing shared Python at $sharedPyDir"
    } else {
        Write-Host "Python is user-scoped ($($python.Exe)); copying to shared location ..."
        $tag = "$major.$minor"
        $r = Invoke-PyProbe -Exe "py" -Arguments @("install", "--target=$sharedPyDir", $tag)
        if ($r.ExitCode -ne 0) {
            Write-Host "  py install --target failed; copying manually ..."
            $pySrc = Split-Path $python.Exe
            if (Test-Path $pySrc) {
                Copy-Item -Path $pySrc -Destination $sharedPyDir -Recurse -Force
            }
        }
        if (-not (Test-Path $sharedPyExe)) {
            $nested = Get-ChildItem -Path $sharedPyDir -Filter "python.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($nested) {
                $sharedPyDir = Split-Path $nested.FullName
                $sharedPyExe = $nested.FullName
            }
        }
        if (-not (Test-Path $sharedPyExe)) {
            throw "Failed to create shared Python at $sharedPyDir. Copy $($python.Exe) manually."
        }
        $launcher = Join-Path $sharedPyDir "Lib\venv\scripts\nt\venvlauncher.exe"
        $wlauncher = Join-Path $sharedPyDir "Lib\venv\scripts\nt\venvwlauncher.exe"
        if (Test-Path $launcher) { attrib -H -S $launcher 2>$null | Out-Null }
        if (Test-Path $wlauncher) { attrib -H -S $wlauncher 2>$null | Out-Null }
        Write-Host "  Shared Python ready at $sharedPyExe"
    }
    $python = @{ Exe = $sharedPyExe; Args = @() }
}

# ===========================================================================
# 3. Create directories and venv
# ===========================================================================

Write-Host "Creating directories under $InstallDir ..."
foreach ($d in @($InstallDir, $logs)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

# Stop the service before touching the venv
$svcExists = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svcExists -and $svcExists.Status -eq "Running") {
    Write-Host "Stopping service `"$ServiceName`" to release files before upgrade ..."
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3
}

$pyPrefix = Split-Path $python.Exe
foreach ($vl in @("Lib\venv\scripts\nt\venvlauncher.exe", "Lib\venv\scripts\nt\venvwlauncher.exe")) {
    $vlPath = Join-Path $pyPrefix $vl
    if (Test-Path $vlPath) { attrib -H -S $vlPath 2>$null | Out-Null }
}

Write-Host "Creating virtualenv at $venv ..."
$venvOut = & $python.Exe @($python.Args + @("-m", "venv", $venv)) 2>&1
if ($LASTEXITCODE -ne 0 -or -not (Test-Path (Join-Path $venv "Scripts\python.exe"))) {
    if ($venvOut) { Write-Host ($venvOut | Out-String) }
    throw "Failed to create virtualenv at $venv using $($python.Exe)."
}

$venvPy = Join-Path $venv "Scripts\python.exe"
$venvProbe = & $venvPy -c "import sys; print(sys.executable)" 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "venv created but python.exe is not functional (exit $LASTEXITCODE): $venvProbe"
}
Write-Host "  venv verified: $venvProbe"

Write-Host "Installing sluice (with [windows] extra for pywin32) ..."
& $venvPy -m pip install --upgrade pip | Out-Null
$installTarget = $repoRoot
if (-not (Test-Path (Join-Path $repoRoot "pyproject.toml"))) {
    $installTarget = "git+https://github.com/hraedon/sluice.git@main"
}
# Install with [windows] extra so pywin32 is available for the service
& $venvPy -m pip install --upgrade "$installTarget[windows]"
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [warn] pip install with [windows] extra failed; trying without ..."
    & $venvPy -m pip install --upgrade $installTarget
    if ($LASTEXITCODE -ne 0) {
        throw "pip install of sluice failed (exit $LASTEXITCODE)."
    }
    & $venvPy -m pip install "pywin32>=306"
}
$installedVer = ((& $venvPy -m pip show sluice 2>$null | Select-String "^Version:") -replace "^Version:\s*", "").Trim()
Write-Host "  Installed sluice version: $installedVer"

# Run pywin32 post-install script (registers DLLs, copies pythonservice.exe)
$pywin32PostInstall = & $venvPy -c "import os, sys; print(os.path.join(os.path.dirname(sys.executable), 'Scripts', 'pywin32_postinstall.py'))" 2>$null
if ($pywin32PostInstall -and (Test-Path $pywin32PostInstall)) {
    Write-Host "Running pywin32 post-install ..."
    & $venvPy $pywin32PostInstall -install 2>&1 | ForEach-Object { Write-Host "  $_" }
}

# ===========================================================================
# 4. Create or update the TOML config file
# ===========================================================================

if (-not (Test-Path $configPath)) {
    Write-Host "Creating config template at $configPath ..."
    $configContent = @"
# sluice configuration -- edit and restart the service:
#   Restart-Service sluice
# All settings can also be set via environment variables (SLUICE_ prefix).

[serve]
# Upstream LLM API base URL (REQUIRED).
upstream = "$Upstream"

# Provider type: umans, anthropic, openai, generic
provider = "$Provider"

# Listen address -- 127.0.0.1:8800 for localhost-only dashboard access
listen = "$Listen"

# Target max concurrency (default: 3, one below umans Code limit of 4)
target = $Target

# Poll interval for /v1/usage (seconds)
# poll_interval = 5.0

# Idle poll interval -- slows down when no traffic (seconds)
# poll_interval_idle = 30.0

# Release cooldown -- seconds a freed permit rests before reuse
# release_cooldown = 2.0

# Queue timeout -- max seconds to wait for a permit before 503
# queue_timeout = 30.0

# Admin token to gate dashboard/admin routes (leave empty for open access)
# admin_token = "$AdminToken"

# QoS reserve -- set aside permit slots for a priority class
# reserve = "interactive=1"

# Trusted proxies (CIDR/IP allowlist for x-sluice-client-label)
# trusted_proxies = "127.0.0.1"

# History persistence (SQLite)
# history_store = "$InstallDir\sluice-history.sqlite3"
# history_size = 2880
# history_ttl = 604800
"@
    [System.IO.File]::WriteAllText($configPath, $configContent, (New-Object System.Text.UTF8Encoding $false))
    Write-Host "  Config written. Edit $configPath to customise, then restart the service."
} else {
    Write-Host "Keeping existing config at $configPath"
}

# ===========================================================================
# 5. Register the Windows service
#    Uses New-Service with python.exe -m sluice.win_service as the binary.
#    This bypasses pythonservice.exe (which has DLL resolution issues in
#    venvs) and runs the service module directly via python.exe.
# ===========================================================================

# Remove existing service (if present) for clean reconfiguration
$existingSvc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingSvc) {
    Write-Host "Removing existing service `"$ServiceName`" for reconfiguration ..."
    if ($existingSvc.Status -ne "Stopped") {
        Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 3
    }
    & sc.exe delete $ServiceName 2>$null | Out-Null
    Start-Sleep -Seconds 2
}

$pythonExe = Join-Path $venv "Scripts\python.exe"
$binPath = "`"$pythonExe`" -m sluice.win_service"

Write-Host "Creating service `"$ServiceName`" (binPath: $binPath) ..."
New-Service -Name $ServiceName -BinaryPathName $binPath -StartupType Automatic -Description "sluice -- concurrency-metering reverse proxy for LLM APIs" | Out-Null
if ($?) {
    Write-Host "  Service created."
} else {
    throw "Failed to create service."
}

# Set environment variables on the service via registry
$svcRegKey = "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName"
$envLines = @()
$envLines += "SLUICE_CONFIG=$configPath"
if ($UsageKey) {
    $envLines += "SLUICE_USAGE_KEY=$UsageKey"
}
if ($Upstream) {
    $envLines += "SLUICE_UPSTREAM=$Upstream"
}
# Force the provider via env too, so re-installing over an existing config
# actually applies -Provider. The config file (below) is only written on a
# fresh install; without this env var a re-install silently keeps the old
# provider while -Upstream (also env-forced) changes, yielding an incoherent
# provider/upstream pair. Env precedence (flag → env → config) makes this win.
$envLines += "SLUICE_PROVIDER=$Provider"
if ($AdminToken) {
    $envLines += "SLUICE_ADMIN_TOKEN=$AdminToken"
}
$envMultiSz = $envLines -join "`0"
Set-ItemProperty -Path $svcRegKey -Name "Environment" -Value $envMultiSz -Type MultiString -ErrorAction SilentlyContinue
Write-Host "  Environment variables set on service ($($envLines.Count) values)."

# Set recovery actions: restart after 10 seconds
& sc.exe failure $ServiceName reset= 60 actions= restart/10000 2>$null | Out-Null

# ===========================================================================
# 6. Start the service
# ===========================================================================

Write-Host "Starting service `"$ServiceName`" ..."
& sc.exe start $ServiceName 2>&1
Start-Sleep -Seconds 8

$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc) {
    Write-Host "  Service status: $($svc.Status)"
    if ($svc.Status -ne "Running") {
        Write-Host "  [warn] Service is not Running. Check event log:"
        Write-Host "    Get-WinEvent -FilterHashtable @{LogName='System'; ProviderName='Service Control Manager'} -MaxEvents 5"
    }
}

# ===========================================================================
# 7. Done
# ===========================================================================

Write-Host ""
Write-Host "Done. sluice installed to $venv"
Write-Host "Data dir: $InstallDir   Logs: $logs   Config: $configPath"
Write-Host ""
Write-Host "Service: $ServiceName (Automatic)"
Write-Host "Dashboard: http://localhost:8800/"
Write-Host ""
if (-not $Upstream) {
    Write-Host "[action] Edit $configPath to set the upstream URL,"
    Write-Host "         then: Restart-Service $ServiceName"
}
if (-not $UsageKey -and $Provider -eq "umans") {
    Write-Host "[action] Set the usage API key:"
    Write-Host "  Set-ItemProperty -Path '$svcRegKey' -Name Environment -Value (`"SLUICE_CONFIG=$configPath`0SLUICE_USAGE_KEY=sk-...`") -Type MultiString"
    Write-Host "  Restart-Service $ServiceName"
}
Write-Host ""
Write-Host "Manage the service:"
Write-Host "  Start-Service $ServiceName"
Write-Host "  Stop-Service $ServiceName"
Write-Host "  Restart-Service $ServiceName"
Write-Host "  Get-Service $ServiceName"
Write-Host ""
Write-Host "View logs:"
Write-Host "  Get-EventLog -LogName Application -Source sluice -Newest 20"
Write-Host "  Get-Content $logs\stdout.log -Tail 50  (if stdout redirect is configured)"
