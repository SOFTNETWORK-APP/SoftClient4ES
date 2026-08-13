@echo off
rem ===========================================================================
rem  SoftClient4ES Installation Script - cmd.exe entry point
rem ===========================================================================
rem
rem  Why this file exists: on a host whose PowerShell execution policy is
rem  Restricted or AllSigned (the Windows client default is Restricted),
rem  double-clicking or calling install.ps1 fails before it runs a single line.
rem  This wrapper is the supported way in: -ExecutionPolicy Bypass applies to
rem  THIS process only, changes nothing on the machine, and needs no elevation.
rem
rem  It is a wrapper and nothing else - every option, default, fallback and
rem  message lives in install.ps1, so the two entry points can never drift.
rem  When install.ps1 is not sitting next to it, it downloads one, so
rem  install.cmd on its own is a complete install. Pass the same flags you
rem  would pass to install.ps1:
rem
rem      install.cmd
rem      install.cmd -ListVersions -EsVersion 8
rem      install.cmd -Target "C:\tools\softclient4es" -EsVersion 8 -Version 1.0.0
rem      install.cmd -EsVersion 7 -Version 0.20.4 -NoExtensions
rem      install.cmd -Help
rem
rem  Caveat worth knowing: -ExecutionPolicy Bypass is overridden when the policy
rem  is enforced through Group Policy (the MachinePolicy / UserPolicy scopes).
rem  That is deliberate on Microsoft's side and no wrapper can work around it -
rem  on such a host, ask your administrator, or run the REPL from the jar by
rem  hand (see documentation/client/repl.md).
rem ===========================================================================

setlocal

set "PS1_URL=https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/refs/heads/main/install.ps1"
set "PS1=%~dp0install.ps1"

rem A local install.ps1 always wins: a downloaded pair must stay self-consistent,
rem and a released bundle must never be silently mixed with main.
rem Written as goto + a separate errorlevel test rather than `if ... call ... ||`:
rem how cmd binds `||` inside an `if` body is ambiguous, and this is not a
rem platform where a subtlety can be settled by running it.
if exist "%PS1%" goto run
call :fetch_ps1
if errorlevel 1 exit /b 1

:run
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
exit /b %ERRORLEVEL%

rem ---------------------------------------------------------------------------
rem Each line of a subroutine is parsed when it is reached, so %PS1% below sees
rem the value assigned on the previous line. The same code inside the `if not
rem exist (...)` block above would NOT: cmd expands every %VAR% in a
rem parenthesised block in ONE parse pass, before running any line in it.
rem ---------------------------------------------------------------------------
:fetch_ps1
where curl.exe >nul 2>&1
if errorlevel 1 (
    echo [ERROR] install.ps1 is not next to install.cmd and curl.exe is unavailable. 1>&2
    echo [ERROR] curl.exe ships with Windows 10 build 1803+ and Windows Server 2019+. 1>&2
    echo [ERROR] Download install.ps1 manually into the same directory as install.cmd. 1>&2
    exit /b 1
)
set "PS1=%TEMP%\softclient4es-install.ps1"
echo [INFO] install.ps1 not found next to install.cmd - downloading it...
echo [INFO] URL: %PS1_URL%
curl.exe -fsSL -o "%PS1%" "%PS1_URL%"
if errorlevel 1 (
    echo [ERROR] Could not download install.ps1 from %PS1_URL% 1>&2
    exit /b 1
)
echo [INFO] Using %PS1%
exit /b 0
