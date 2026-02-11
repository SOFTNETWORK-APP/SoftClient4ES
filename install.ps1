#
# SoftClient4ES Installation Script
# For Windows (PowerShell)
#

param(
    [string]$Target = "$env:USERPROFILE\softclient4es",
    [string]$EsVersion = "8",
    [string]$Version = "latest",
    [string]$ScalaVersion = "2.13",
    [switch]$ListVersions,
    [switch]$Help
)

# =============================================================================
# Configuration
# =============================================================================

$JFROG_REPO_URL = "https://softnetwork.jfrog.io/artifactory/releases/app/softnetwork/elastic"
$JFROG_API_URL = "https://softnetwork.jfrog.io/artifactory/api/storage/releases/app/softnetwork/elastic"

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
  -Version <ver>      SoftClient4ES version (default: latest)
  -ScalaVersion <ver> Scala version (default: 2.13)
  -ListVersions       List available versions for the specified ES version
  -Help               Show this help message

Java Requirements:
  ES 6, 7, 8  ->  Java 8 or higher
  ES 9        ->  Java 17 or higher

Examples:
  .\install.ps1
  .\install.ps1 -ListVersions -EsVersion 8
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
function Write-Err($msg)     { Write-Host "[ERROR] $msg" -ForegroundColor Red }

# =============================================================================
# Validate Inputs
# =============================================================================

if ($EsVersion -notmatch '^[6-9]$') {
    Write-Err "Invalid Elasticsearch version: $EsVersion (must be 6, 7, 8, or 9)"
    exit 1
}

# =============================================================================
# Derived Variables
# =============================================================================

$ARTIFACT_NAME = "softclient4es${EsVersion}-cli_${ScalaVersion}"

# =============================================================================
# Get Required Java Version
# =============================================================================

function Get-RequiredJavaVersion {
    param([string]$EsVer)
    if ($EsVer -eq "9") {
        return 17
    } else {
        return 8
    }
}

$REQUIRED_JAVA_VERSION = Get-RequiredJavaVersion -EsVer $EsVersion

# =============================================================================
# List Available Versions
# =============================================================================

function Get-AvailableVersions {
    $apiUrl = "${JFROG_API_URL}/${ARTIFACT_NAME}"

    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        $response = Invoke-RestMethod -Uri $apiUrl -UseBasicParsing

        $versions = $response.children |
            Where-Object { $_.folder -eq $true } |
            ForEach-Object { $_.uri.TrimStart('/') } |
            Where-Object { $_ -notmatch '^\.' } |
            Sort-Object { [Version]($_ -replace '-SNAPSHOT', '.0' -replace '[^0-9.]', '') }

        return $versions
    }
    catch {
        Write-Err "Failed to fetch versions from repository"
        Write-Err "Artifact: $ARTIFACT_NAME"
        Write-Err $_.Exception.Message
        exit 1
    }
}

if ($ListVersions) {
    Write-Info "Fetching available versions for ES$EsVersion..."

    $versions = Get-AvailableVersions

    if (-not $versions -or $versions.Count -eq 0) {
        Write-Err "No versions found for $ARTIFACT_NAME"
        exit 1
    }

    Write-Host ""
    Write-Host "==================================================================" -ForegroundColor Cyan
    Write-Host "  Available SoftClient4ES Versions for Elasticsearch $EsVersion" -ForegroundColor Cyan
    Write-Host "==================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Artifact: " -NoNewline; Write-Host $ARTIFACT_NAME -ForegroundColor Yellow
    Write-Host "  Java required: " -NoNewline; Write-Host "${REQUIRED_JAVA_VERSION}+" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Versions:" -ForegroundColor Green
    Write-Host ""

    foreach ($ver in $versions) {
        Write-Host "    * $ver"
    }

    Write-Host ""
    Write-Host "  Total: $($versions.Count) version(s)" -ForegroundColor Blue
    Write-Host ""
    Write-Host "  To install a specific version:"
    Write-Host "    .\install.ps1 -EsVersion $EsVersion -Version <version>" -ForegroundColor Cyan
    Write-Host ""

    exit 0
}

# =============================================================================
# Resolve Latest Version
# =============================================================================

function Resolve-LatestVersion {
    Write-Info "Resolving latest version..."

    $versions = Get-AvailableVersions

    if (-not $versions -or $versions.Count -eq 0) {
        Write-Err "No versions found"
        exit 1
    }

    # Prefer non-snapshot versions
    $releaseVersions = $versions | Where-Object { $_ -notmatch 'SNAPSHOT' }

    if ($releaseVersions -and $releaseVersions.Count -gt 0) {
        return $releaseVersions[-1]
    }

    # Fallback to any version
    return $versions[-1]
}

if ($Version -eq "latest") {
    $Version = Resolve-LatestVersion
    Write-Success "Resolved latest version: $Version"
}

$JAR_NAME = "${ARTIFACT_NAME}-${Version}-assembly.jar"
$DOWNLOAD_URL = "${JFROG_REPO_URL}/${ARTIFACT_NAME}/${Version}/${JAR_NAME}"

# =============================================================================
# Check Prerequisites
# =============================================================================

function Check-Prerequisites {
    Write-Info "Checking prerequisites..."

    # Check Java
    try {
        $javaVersionOutput = & java -version 2>&1 | Select-String -Pattern 'version'
        $javaVersionString = $javaVersionOutput.ToString()

        # Extract version number
        if ($javaVersionString -match '"1\.(\d+)') {
            # Old format: 1.8.x
            $javaVersion = [int]$Matches[1]
        } elseif ($javaVersionString -match '"(\d+)') {
            # New format: 11.x, 17.x
            $javaVersion = [int]$Matches[1]
        } else {
            Write-Warn "Could not determine Java version"
            $javaVersion = 0
        }

        if ($javaVersion -gt 0 -and $javaVersion -lt $REQUIRED_JAVA_VERSION) {
            Write-Err "Java $REQUIRED_JAVA_VERSION or higher is required for ES$EsVersion."
            Write-Err "Found: Java $javaVersion"
            exit 1
        }

        Write-Success "Java $javaVersion found (required: ${REQUIRED_JAVA_VERSION}+)"
    }
    catch {
        Write-Err "Java is not installed."
        Write-Err "ES$EsVersion requires Java $REQUIRED_JAVA_VERSION or higher."
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
        Write-Err "Failed to download JAR from $DOWNLOAD_URL"
        Write-Err "Please check that version '$Version' exists."
        Write-Err "Run with -ListVersions to see available versions."
        Write-Err $_.Exception.Message
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
set REQUIRED_JAVA=$REQUIRED_JAVA_VERSION

if not exist "%JAR_FILE%" (
    echo Error: JAR file not found: %JAR_FILE% >&2
    exit /b 1
)

REM Check Java
java -version >nul 2>&1
if errorlevel 1 (
    echo Error: Java is not installed. Java %REQUIRED_JAVA%+ is required. >&2
    exit /b 1
)

if "%JAVA_OPTS%"=="" set JAVA_OPTS=-Xmx512m

java %JAVA_OPTS% -Dconfig.file="%CONFIG_FILE%" -jar "%JAR_FILE%" %*

endlocal
"@

    $batchContent | Out-File -FilePath "$Target\bin\softclient4es.bat" -Encoding ASCII

    # PowerShell launcher
    $psContent = @"
#
# SoftClient4ES Launcher
# Elasticsearch version: $EsVersion
# Required Java: ${REQUIRED_JAVA_VERSION}+
#

`$ScriptDir = Split-Path -Parent `$MyInvocation.MyCommand.Path
`$BaseDir = Split-Path -Parent `$ScriptDir
`$JarFile = "`$BaseDir\lib\$JAR_NAME"
`$ConfigFile = "`$BaseDir\conf\application.conf"
`$RequiredJava = $REQUIRED_JAVA_VERSION

if (-not (Test-Path `$JarFile)) {
    Write-Error "JAR file not found: `$JarFile"
    exit 1
}

# Check Java
try {
    `$javaVersionOutput = & java -version 2>&1 | Select-String -Pattern 'version'
    `$javaVersionString = `$javaVersionOutput.ToString()

    if (`$javaVersionString -match '"1\.(\d+)') {
        `$javaVersion = [int]`$Matches[1]
    } elseif (`$javaVersionString -match '"(\d+)') {
        `$javaVersion = [int]`$Matches[1]
    } else {
        `$javaVersion = 0
    }

    if (`$javaVersion -gt 0 -and `$javaVersion -lt `$RequiredJava) {
        Write-Error "Java `$RequiredJava+ is required. Found: Java `$javaVersion"
        exit 1
    }
}
catch {
    Write-Error "Java is not installed. Java `$RequiredJava+ is required."
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
# Create Version Info File
# =============================================================================

function Create-VersionInfo {
    $versionContent = @"
SoftClient4ES Installation Info
================================
Installed:          $(Get-Date -Format "yyyy-MM-dd HH:mm:ss UTC")
Elasticsearch:      $EsVersion
Version:            $Version
Scala:              $ScalaVersion
Java Required:      ${REQUIRED_JAVA_VERSION}+
Artifact:           $ARTIFACT_NAME
"@

    $versionContent | Out-File -FilePath "$Target\VERSION" -Encoding UTF8

    Write-Success "Created $Target\VERSION"
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
    Write-Host "  Java required:          ${REQUIRED_JAVA_VERSION}+"
    Write-Host ""
    Write-Host "  Directory structure:"
    Write-Host "    $Target\"
    Write-Host "    +-- bin\"
    Write-Host "    |   +-- softclient4es.bat"
    Write-Host "    |   \-- softclient4es.ps1"
    Write-Host "    +-- conf\"
    Write-Host "    |   \-- application.conf"
    Write-Host "    +-- lib\"
    Write-Host "    |   \-- $JAR_NAME"
    Write-Host "    +-- VERSION"
    Write-Host "    \-- uninstall.ps1"
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
Create-VersionInfo
Print-Summary
