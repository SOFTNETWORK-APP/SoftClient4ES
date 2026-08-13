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
rem  Pass the same flags you would pass to install.ps1:
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

set "PS1=%~dp0install.ps1"

if not exist "%PS1%" (
    echo [ERROR] install.ps1 was not found next to install.cmd. 1>&2
    echo [ERROR] Expected: %PS1% 1>&2
    echo [ERROR] Download both files from the same release and keep them together. 1>&2
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
exit /b %ERRORLEVEL%
