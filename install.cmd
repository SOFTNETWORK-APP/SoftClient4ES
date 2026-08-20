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
rem  When install.ps1 is not sitting next to it, it downloads one - from a
rem  pinned release tag, checked against a pinned SHA-256 before it is run - so
rem  install.cmd on its own is a complete install. Pass the same flags you would
rem  pass to install.ps1:
rem
rem      install.cmd
rem      install.cmd -ListVersions -EsVersion 8
rem      install.cmd -Target "C:\tools\softclient4es" -EsVersion 8 -Version 1.0.0
rem      install.cmd -EsVersion 7 -Version 0.21.0 -NoExtensions
rem      install.cmd -Help
rem
rem  Caveat worth knowing: -ExecutionPolicy Bypass is overridden when the policy
rem  is enforced through Group Policy (the MachinePolicy / UserPolicy scopes).
rem  That is deliberate on Microsoft's side and no wrapper can work around it -
rem  on such a host, ask your administrator, or run the REPL from the jar by
rem  hand (see documentation/client/repl.md).
rem ===========================================================================

setlocal

rem ---------------------------------------------------------------------------
rem  Where a missing install.ps1 is fetched from.
rem
rem  PINNED TO A RELEASE TAG, not to main. This file also travels inside release
rem  bundles, and one that quietly pulled whatever happens to be on main would
rem  produce exactly the mix that the "a local install.ps1 always wins" rule
rem  below exists to prevent - silently, since a user who ran a released .cmd has
rem  no reason to think they are running main. A tag is also immutable, which is
rem  what makes the SHA-256 worth checking at all: the script is about to be run
rem  with -ExecutionPolicy Bypass, and "it arrived over TLS" only says the
rem  transport was sound, not that the file is the one the release intended.
rem
rem  RELEASE RITUAL: bump BOTH lines together when cutting a release.
rem      curl -fsSL https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/<tag>/install.ps1 | shasum -a 256
rem  A mismatch is a hard failure, so a bumped tag with a stale hash breaks the
rem  fallback loudly instead of running something unverified.
rem
rem  To track development instead:  set SOFTCLIENT4ES_INSTALL_REF=main
rem  The pinned hash belongs to the pinned tag, so overriding the ref drops the
rem  integrity check unless SOFTCLIENT4ES_INSTALL_SHA256 supplies another one -
rem  and says so when it does not.
rem ---------------------------------------------------------------------------
set "PS1_REF=v0.21.0"
set "PS1_SHA256=e1275bd269c8bb922d28827b6c74e965bbe7b5139b4e16138bacaac7c03912ae"

rem One `if` per line, never a parenthesised block: cmd expands every %VAR% in a
rem block in ONE parse pass, before running any line in it.
if defined SOFTCLIENT4ES_INSTALL_REF set "PS1_REF=%SOFTCLIENT4ES_INSTALL_REF%"
if defined SOFTCLIENT4ES_INSTALL_REF set "PS1_SHA256=%SOFTCLIENT4ES_INSTALL_SHA256%"

set "PS1_URL=https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/%PS1_REF%/install.ps1"
set "PS1=%~dp0install.ps1"
set "PS1_TMPDIR="
set "RC=1"

rem A local install.ps1 always wins: a downloaded pair must stay self-consistent,
rem and a released bundle must never be silently mixed with main.
rem Written as goto + a separate errorlevel test rather than `if ... call ... ||`:
rem how cmd binds `||` inside an `if` body is ambiguous, and this is not a
rem platform where a subtlety can be settled by running it.
if exist "%PS1%" goto run
call :fetch_ps1
if errorlevel 1 goto cleanup

:run
powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "RC=%ERRORLEVEL%"

rem Single exit point, so a downloaded install.ps1 is removed however this ends -
rem including the paths that never got as far as running it.
:cleanup
if defined PS1_TMPDIR if exist "%PS1_TMPDIR%" rd /s /q "%PS1_TMPDIR%"
exit /b %RC%

rem ---------------------------------------------------------------------------
rem Each line of a subroutine is parsed when it is reached, so %PS1_TMPDIR% below
rem sees the value assigned on the previous line. The same code inside an `if not
rem exist (...)` block would NOT: cmd expands every %VAR% in a parenthesised block
rem in ONE parse pass, before running any line in it.
rem ---------------------------------------------------------------------------
:fetch_ps1
where curl.exe >nul 2>&1
if errorlevel 1 (
    echo [ERROR] install.ps1 is not next to install.cmd and curl.exe is unavailable. 1>&2
    echo [ERROR] curl.exe ships with Windows 10 build 1803+ and Windows Server 2019+. 1>&2
    echo [ERROR] Download install.ps1 manually into the same directory as install.cmd. 1>&2
    exit /b 1
)

rem A fresh directory of our own, rather than the fixed %TEMP%\softclient4es-install.ps1
rem this used to write: that was a predictable path in a shared directory, executed
rem with -ExecutionPolicy Bypass, and left behind after every run. mkdir fails when
rem the name already exists, so nothing pre-created can be substituted for it.
set "PS1_TMPDIR=%TEMP%\softclient4es-install-%RANDOM%%RANDOM%"
mkdir "%PS1_TMPDIR%" 2>nul
if errorlevel 1 (
    echo [ERROR] Could not create a temporary directory: %PS1_TMPDIR% 1>&2
    set "PS1_TMPDIR="
    exit /b 1
)
set "PS1=%PS1_TMPDIR%\install.ps1"

echo [INFO] install.ps1 not found next to install.cmd - downloading it...
echo [INFO] URL: %PS1_URL%
curl.exe -fsSL -o "%PS1%" "%PS1_URL%"
if errorlevel 1 (
    echo [ERROR] Could not download install.ps1 from %PS1_URL% 1>&2
    exit /b 1
)

call :verify_ps1
if errorlevel 1 exit /b 1

echo [INFO] Using %PS1%
exit /b 0

rem ---------------------------------------------------------------------------
rem Integrity check, before the file is handed to powershell.exe.
rem ---------------------------------------------------------------------------
:verify_ps1
if not defined PS1_SHA256 (
    echo [WARN] SOFTCLIENT4ES_INSTALL_REF is set without SOFTCLIENT4ES_INSTALL_SHA256, 1>&2
    echo [WARN] so the downloaded install.ps1 was NOT checked against a known hash. 1>&2
    exit /b 0
)

where certutil.exe >nul 2>&1
if errorlevel 1 (
    echo [ERROR] certutil.exe is unavailable, so install.ps1 cannot be verified - it was NOT run. 1>&2
    echo [ERROR] Download install.ps1 manually into the same directory as install.cmd. 1>&2
    exit /b 1
)

rem certutil prints the digest on the second line. The FOR must set PS1_HASH on a
rem line of its own, and the space-stripping must live OUTSIDE any block, or the
rem one-parse-pass rule above would have it read the previous value.
set "PS1_HASH="
for /f "skip=1 tokens=* delims=" %%h in ('certutil -hashfile "%PS1%" SHA256') do if not defined PS1_HASH set "PS1_HASH=%%h"
rem Older certutil builds print the digest as space-separated byte pairs.
set "PS1_HASH=%PS1_HASH: =%"

if /i not "%PS1_HASH%"=="%PS1_SHA256%" (
    echo [ERROR] install.ps1 failed its integrity check - it was NOT run. 1>&2
    echo [ERROR]   expected: %PS1_SHA256% 1>&2
    echo [ERROR]   actual:   %PS1_HASH% 1>&2
    echo [ERROR]   url:      %PS1_URL% 1>&2
    echo [ERROR] Download install.ps1 manually into the same directory as install.cmd. 1>&2
    exit /b 1
)

echo [INFO] install.ps1 SHA-256 verified against %PS1_REF%
exit /b 0
