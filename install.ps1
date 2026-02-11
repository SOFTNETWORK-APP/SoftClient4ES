#
# SoftClient4ES Installation Script
# For Windows (PowerShell)
#

param(
    [string]$Target = "$env:USERPROFILE\softclient4es",
    [string]$EsVersion = "8",
    [string]$Version = "0.16-SNAPSHOT",
    [string]$ScalaVersion = "2.13",
    [switch]$Help
)

# =============================================================================
# Configuration
# =============================================================================

$JFROG_REPO_URL = "https://softnetwork.jfrog.io/artifactory/releases/app/softnetwork/elastic"

# =============================================================================
# Help
# =============================================================================

function Show-Help {
    Write-Host @"

SoftClient4ES Installation Script

Usage: .\install.ps1 [OPTIONS]

Options:
  -Target <dir>       Installation directory (default: $env:USERPROFILE\softclient4es)
  -EsVersion <ver>    Elasticsearch major version: 6, 7, 8, 9 (default: 8)
  -Version <ver>      SoftClient4ES version (default: 0.1.0-SNAPSHOT)
  -ScalaVersion <ver> Scala version (default: 2.13)
  -Help               Show this help message

Examples:
  .\install.ps1
  .\install.ps1 -Target "C:\tools\softclient4es" -EsVersion 8 -Version 1.0.0
  .\install.ps1 -EsVersion 7 -Version 0.2.0

"@
    exit 0
}

if ($Help) {
    Show-Help
}

# =============================================================================
# Output Functions
# =============================================================================

function Write-Info($msg)    { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Success($msg) { Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Warn($msg)    { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Error($msg)   { Write-Host "[ERROR] $msg" -ForegroundColor Red }

# =============================================================================
# Validate Inputs
# =============================================================================

if ($EsVersion -notmatch '^[6-9]$') {
    Write-Error "Invalid Elasticsearch version: $EsVersion (must be 6, 7, 8, or 9)"
    exit 1
}

# =============================================================================
# Derived Variables
# =============================================================================

$ARTIFACT_NAME = "softclient4es${EsVersion}-cli_${ScalaVersion}"
$JAR_NAME = "${ARTIFACT_NAME}-${Version}-assembly.jar"
$DOWNLOAD_URL = "${JFROG_REPO_URL}/${ARTIFACT_NAME}/${Version}/${JAR_NAME}"

# =============================================================================
# Check Prerequisites
# =============================================================================

function Check-Prerequisites {
    Write-Info "Checking prerequisites..."

    # Check Java
    try {
        $javaVersion = & java -version 2>&1 | Select-String -Pattern 'version "(\d+)' | ForEach-Object { $_.Matches.Groups[1].Value }
        if ([int]$javaVersion -lt 11) {
            Write-Error "Java 11 or higher is required. Found: Java $javaVersion"
            exit 1
        }
        Write-Success "Java $javaVersion found"
    }
    catch {
        Write-Error "Java is not installed. Please install Java 11 or higher."
        exit 1
    }
}

# =============================================================================
# Create Directory Structure
# =============================================================================

function Create-Directories {
    Write-Info "Creating directory structure..."

    New-Item -ItemType Directory -Force -Path "$Target\bin" | Out-Null
    New-Item -ItemType Directory -Force -Path "$Target\conf" | Out-Null
    New-Item -ItemType Directory -Force -Path "$Target\lib" | Out-Null

    Write-Success "Created $Target\{bin,conf,lib}"
}

# =============================================================================
# Download JAR
# =============================================================================

function Download-Jar {
    Write-Info "Downloading $JAR_NAME..."
    Write-Info "URL: $DOWNLOAD_URL"

    $dest = "$Target\lib\$JAR_NAME"

    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $DOWNLOAD_URL -OutFile $dest -UseBasicParsing
        Write-Success "Downloaded to $dest"
    }
    catch {
        Write-Error "Failed to download JAR from $DOWNLOAD_URL"
        Write-Error $_.Exception.Message
        exit 1
    }
}

# =============================================================================
# Create Configuration File
# =============================================================================

function Create-Config {
    Write-Info "Creating configuration file..."

    $configContent = @'
# SoftClient4ES Configuration
# Override these settings or use command-line options

elastic {
  credentials {
    scheme       = "http"
    scheme       = ${?ELASTIC_SCHEME}

    host         = "localhost"
    host         = ${?ELASTIC_HOST}

    port         = 9200
    port         = ${?ELASTIC_PORT}

    username     = ""
    username     = ${?ELASTIC_USERNAME}

    password     = ""
    password     = ${?ELASTIC_PASSWORD}

    api-key      = ""
    api-key      = ${?ELASTIC_API_KEY}

    bearer-token = ""
    bearer-token = ${?ELASTIC_BEARER_TOKEN}
  }
}
'@

    $configContent | Out-File -FilePath "$Target\conf\application.conf" -Encoding UTF8

    Write-Success "Created $Target\conf\application.conf"
}

# =============================================================================
# Create Launcher Scripts
# =============================================================================

function Create-Launcher {
    Write-Info "Creating launcher scripts..."

    # Batch file
    $batchContent = @"
@echo off
setlocal

set SCRIPT_DIR=%~dp0
set BASE_DIR=%SCRIPT_DIR%..
set JAR_FILE=%BASE_DIR%\lib\$JAR_NAME
set CONFIG_FILE=%BASE_DIR%\conf\application.conf

if not exist "%JAR_FILE%" (
    echo Error: JAR file not found: %JAR_FILE% >&2
    exit /b 1
)

if "%JAVA_OPTS%"=="" set JAVA_OPTS=-Xmx512m

java %JAVA_OPTS% -Dconfig.file="%CONFIG_FILE%" -jar "%JAR_FILE%" %*

endlocal
"@

    $batchContent | Out-File -FilePath "$Target\bin\softclient4es.bat" -Encoding ASCII

    # PowerShell launcher
    $psContent = @"
`$ScriptDir = Split-Path -Parent `$MyInvocation.MyCommand.Path
`$BaseDir = Split-Path -Parent `$ScriptDir
`$JarFile = "`$BaseDir\lib\$JAR_NAME"
`$ConfigFile = "`$BaseDir\conf\application.conf"

if (-not (Test-Path `$JarFile)) {
    Write-Error "JAR file not found: `$JarFile"
    exit 1
}

`$JavaOpts = if (`$env:JAVA_OPTS) { `$env:JAVA_OPTS } else { "-Xmx512m" }

& java `$JavaOpts "-Dconfig.file=`$ConfigFile" -jar `$JarFile `$args
"@

    $psContent | Out-File -FilePath "$Target\bin\softclient4es.ps1" -Encoding UTF8

    Write-Success "Created $Target\bin\softclient4es.bat"
    Write-Success "Created $Target\bin\softclient4es.ps1"
}

# =============================================================================
# Create Uninstall Script
# =============================================================================

function Create-Uninstaller {
    Write-Info "Creating uninstall script..."

    $uninstallContent = @"
`$Target = "$Target"

`$confirm = Read-Host "This will remove `$Target. Continue? [y/N]"
if (`$confirm -eq 'y' -or `$confirm -eq 'Y') {
    Remove-Item -Recurse -Force `$Target
    Write-Host "SoftClient4ES has been uninstalled."
} else {
    Write-Host "Uninstall cancelled."
}
"@

    $uninstallContent | Out-File -FilePath "$Target\uninstall.ps1" -Encoding UTF8

    Write-Success "Created $Target\uninstall.ps1"
}

# =============================================================================
# Print Summary
# =============================================================================

function Print-Summary {
    Write-Host ""
    Write-Host "==================================================================" -ForegroundColor Green
    Write-Host "  SoftClient4ES Installation Complete!" -ForegroundColor Green
    Write-Host "==================================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Installation directory: $Target"
    Write-Host "  Elasticsearch version:  $EsVersion"
    Write-Host "  SoftClient4ES version:  $Version"
    Write-Host ""
    Write-Host "  Directory structure:"
    Write-Host "    $Target\"
    Write-Host "    ├── bin\"
    Write-Host "    │   ├── softclient4es.bat"
    Write-Host "    │   └── softclient4es.ps1"
    Write-Host "    ├── conf\"
    Write-Host "    │   └── application.conf"
    Write-Host "    ├── lib\"
    Write-Host "    │   └── $JAR_NAME"
    Write-Host "    └── uninstall.ps1"
    Write-Host ""
    Write-Host "  To start the REPL:"
    Write-Host "    $Target\bin\softclient4es.bat" -ForegroundColor Cyan
    Write-Host "    or"
    Write-Host "    $Target\bin\softclient4es.ps1" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Or add to your PATH:"
    Write-Host "    `$env:PATH += `";$Target\bin`"" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Configuration:"
    Write-Host "    Edit $Target\conf\application.conf"
    Write-Host "    Or use command-line options (run with --help)"
    Write-Host ""
    Write-Host "  To uninstall:"
    Write-Host "    $Target\uninstall.ps1" -ForegroundColor Cyan
    Write-Host ""
}

# =============================================================================
# Main
# =============================================================================

Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  SoftClient4ES Installer" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host ""

Check-Prerequisites
Create-Directories
Download-Jar
Create-Config
Create-Launcher
Create-Uninstaller
Print-Summary
