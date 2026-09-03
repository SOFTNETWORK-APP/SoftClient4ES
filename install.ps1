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
    [switch]$NoExtensions,
    [switch]$Help
)

# =============================================================================
# Configuration
# =============================================================================

$JFROG_REPO_URL = "https://softnetwork.jfrog.io/artifactory/releases/app/softnetwork/elastic"
$JFROG_API_URL = "https://softnetwork.jfrog.io/artifactory/api/storage/releases/app/softnetwork/elastic"

$GITHUB_RAW_URL = "https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/refs/heads/main"
$README_URL = "${GITHUB_RAW_URL}/documentation/client/repl.md"
$LICENSE_URL = "${GITHUB_RAW_URL}/LICENSE"

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
  -Version <ver>      SoftClient4ES version (default: latest). By default this is
                      a version of the -all bundle (it has its own version line);
                      with -NoExtensions it is an engine version.
  -ScalaVersion <ver> Scala version (default: 2.13)
  -ListVersions       List available versions of the artifact that would be
                      installed (the -all bundle, or the engine with
                      -NoExtensions)
  -NoExtensions       Install the plain, pure Apache-2.0 engine only
  -Help               Show this help message

Default install:
  ONE self-contained -all assembly (engine + community extensions + the
  cross-index JOIN extension). The bundle contains components under the Elastic
  License 2.0 plus the proprietary JOIN engine (free to use) - it is NOT a pure
  Apache-2.0 artifact. -NoExtensions always yields a pure Apache-2.0 install.

Java Requirements:
  ES 6, 7, 8  ->  Java 11 or higher
  ES 9        ->  Java 17 or higher
  The -all bundle requires Java 11+ (Arrow / logback bytecode).

Examples:
  .\install.ps1
  .\install.ps1 -ListVersions -EsVersion 8
  .\install.ps1 -Target "C:\tools\softclient4es" -EsVersion 8 -Version 1.0.0
  .\install.ps1 -EsVersion 7 -Version 0.22.0 -NoExtensions

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

# Plain artifact: the pure Apache-2.0 engine assembly (published by elasticsql).
$PLAIN_ARTIFACT_NAME = "softclient4es${EsVersion}-cli_${ScalaVersion}"
# Bundle artifact: the self-contained -all assembly (engine + community
# extensions + arrow JOIN extension + all dependencies), published by the
# softclient4es-repl packaging repo on its OWN version line.
$BUNDLE_ARTIFACT_NAME = "softclient4es${EsVersion}-cli-all_${ScalaVersion}"
# Finalized by the bundle-selection block below; until then it refers to the
# plain artifact (fallback/legacy paths).
$ARTIFACT_NAME = $PLAIN_ARTIFACT_NAME

$WITH_EXTENSIONS = -not $NoExtensions

# =============================================================================
# Get Required Java Version
# =============================================================================

function Get-RequiredJavaVersion {
    param([string]$EsVer)
    if ($EsVer -eq "9") {
        return 17
    } else {
        # The 0.20+ CLI bundles logback 1.5.x (Java-11 bytecode): the REPL does
        # not start on Java 8 (--help crashes with UnsupportedClassVersionError).
        return 11
    }
}

# Major version reported by a SPECIFIC java executable, or 0 when it cannot be
# determined. Taking the exe as a parameter is what lets JAVA_HOME and the PATH
# `java` be probed by the same code - they routinely disagree.
function Get-JavaMajorFromExe {
    param([string]$Exe)
    if (-not $Exe) { return 0 }
    try {
        # Select-Object -First 1 matters: with two matching lines (e.g. a
        # deprecation notice from _JAVA_OPTIONS) the pipeline yields an array whose
        # ToString() is "System.Object[]", no regex matches, and the version reads 0.
        $out = (& $Exe -version 2>&1 | Select-String -Pattern 'version' | Select-Object -First 1).ToString()
        if ($out -match '"1\.(\d+)')   { return [int]$Matches[1] }   # 1.8.x
        elseif ($out -match '"(\d+)')  { return [int]$Matches[1] }   # 11.x, 17.x
    }
    catch { }
    return 0
}

# Major version of the `java` on PATH, or 0 when it cannot be determined.
function Get-JavaMajorVersion {
    $onPath = Get-Command java -ErrorAction SilentlyContinue
    if (-not $onPath) { return 0 }
    return (Get-JavaMajorFromExe -Exe $onPath.Source)
}

$REQUIRED_JAVA_VERSION = Get-RequiredJavaVersion -EsVer $EsVersion

# The JDK this installer bootstraps when the host cannot satisfy the floor. 17
# covers BOTH floors (11 for ES 6/7/8, 17 for ES 9), so there is one download to
# reason about rather than two.
$BOOTSTRAP_JAVA_VERSION = 17
# Adoptium redirects this to the current GA Temurin 17 JDK *zip* for windows/x64 -
# an archive, deliberately not an MSI: unpacking needs no administrator rights.
$TEMURIN_ZIP_URL = "https://api.adoptium.net/v3/binary/latest/$BOOTSTRAP_JAVA_VERSION/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"
# Lives INSIDE the install tree: `uninstall.ps1` then removes it with everything
# else, and the launcher finds it by relative path with no machine-wide state.
$EMBEDDED_JDK_DIR = Join-Path $Target "jdk"

# =============================================================================
# Java resolution — probe, then bootstrap a portable JDK rather than give up
# =============================================================================
# Resolution order, and it is the SAME order the generated launcher uses, which
# is what keeps "the installer worked" and "the REPL starts" from disagreeing:
#
#     <install>\jdk\bin\java.exe   (bootstrapped here, if it was needed)
#          -> %JAVA_HOME%\bin\java.exe
#               -> `java` on PATH
#
# So JAVA_HOME, when it is defined, is the thing actually tested — not the PATH
# `java`, which is frequently a different and older JVM.

# Set by Resolve-Java; read by Check-Prerequisites, the launcher writer and the
# summary. $script:EmbeddedJdkHome is also what tells the failure path that this
# install depends on a JDK under $Target.
$script:JavaMajor = 0
$script:JavaSource = "not found"
$script:EmbeddedJdkHome = $null

# Point this run at a JDK inside the install tree - the one the launcher will use.
# SESSION scope only, deliberately not [Environment]::SetEnvironmentVariable(...,"User"):
# a machine-wide JAVA_HOME would silently repoint every other tool on the box, and
# would dangle after uninstall. Later sessions need nothing - the launcher finds
# <install>\jdk by relative path.
function Use-EmbeddedJdk {
    param([string]$JdkHome, [int]$Major)

    $script:EmbeddedJdkHome = $JdkHome
    $script:JavaMajor = $Major
    $script:JavaSource = "bundled JDK ($JdkHome)"

    $env:JAVA_HOME = $JdkHome
    $env:PATH = (Join-Path $JdkHome "bin") + ";" + $env:PATH
}

function Install-EmbeddedJdk {
    Write-Info "Installing a portable Temurin $BOOTSTRAP_JAVA_VERSION JDK (zip, no administrator rights)..."

    $zip = Join-Path $env:TEMP "softclient4es-temurin$BOOTSTRAP_JAVA_VERSION.zip"
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        # Same reason as the JAR download: Write-Progress per chunk makes a
        # ~180 MB download on Windows PowerShell 5.1 look hung.
        $previousProgress = $ProgressPreference
        $ProgressPreference = 'SilentlyContinue'
        try {
            Invoke-WebRequest -Uri $TEMURIN_ZIP_URL -OutFile $zip -UseBasicParsing -ErrorAction Stop
        }
        finally { $ProgressPreference = $previousProgress }

        # The archive unpacks as jdk-17.x.y+z\ — a version-stamped directory. Unpack
        # to a staging dir and MOVE that one level up, so the final JAVA_HOME is the
        # fixed path <install>\jdk. The launcher hard-codes `%BASE_DIR%\jdk\bin`, and
        # it must not have to glob for a name that changes with every Temurin build.
        #
        # Stage, verify, THEN swap: an existing <install>\jdk is removed only once a
        # usable replacement is on disk. The unpack needs the 180 MB zip plus ~300 MB
        # expanded plus the ~309 MB jar in the same tree, so running out of disk lands
        # squarely in this window, and deleting first left a previously working
        # install with no JVM at all — broken by the very installer the user would
        # then re-run to fix it (issue #234). $staging is scratch by construction, so
        # clearing that one up front is fine.
        $staging = "$EMBEDDED_JDK_DIR.unpack"
        if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
        New-Item -ItemType Directory -Force -Path $staging | Out-Null

        try {
            Expand-Archive -Path $zip -DestinationPath $staging -Force

            $inner = Get-ChildItem $staging -Directory | Select-Object -First 1
            if (-not $inner) {
                Write-Err "The Temurin archive did not unpack as expected (no directory inside $staging)"
                return $null
            }
            if (-not (Test-Path (Join-Path (Join-Path $inner.FullName "bin") "java.exe"))) {
                Write-Err "The Temurin archive did not unpack as expected (no bin\java.exe under $($inner.FullName))"
                return $null
            }

            # Known-good from here: the previous JDK, if any, can go.
            if (Test-Path $EMBEDDED_JDK_DIR) { Remove-Item -Recurse -Force $EMBEDDED_JDK_DIR }
            Move-Item -Path $inner.FullName -Destination $EMBEDDED_JDK_DIR
        }
        finally {
            Remove-Item -Recurse -Force $staging -ErrorAction SilentlyContinue
            Remove-Item $zip -ErrorAction SilentlyContinue
        }

        $exe = Join-Path (Join-Path $EMBEDDED_JDK_DIR "bin") "java.exe"
        if (-not (Test-Path $exe)) {
            Write-Err "No java.exe under $EMBEDDED_JDK_DIR after unpacking"
            return $null
        }

        Write-Success "Installed Temurin $BOOTSTRAP_JAVA_VERSION to $EMBEDDED_JDK_DIR"
        return $EMBEDDED_JDK_DIR
    }
    catch {
        Write-Err "Could not download or unpack the Temurin JDK: $($_.Exception.Message)"
        Write-Err "URL: $TEMURIN_ZIP_URL"
        return $null
    }
}

function Resolve-Java {
    Write-Info "Resolving Java (ES$EsVersion requires ${REQUIRED_JAVA_VERSION}+)..."

    # <install>\jdk first — the JDK a previous run of this installer bootstrapped,
    # and the first thing both generated launchers look at. Without this probe every
    # re-run downloads the ~180 MB Temurin zip again and unpacks it over a perfectly
    # good JDK (JAVA_HOME is set for the session only, so a later shell has nothing
    # pointing at it either), and the installer contradicts the resolution order its
    # own launchers document (issue #233).
    $bundledExe = Join-Path (Join-Path $EMBEDDED_JDK_DIR "bin") "java.exe"
    $bundledMajor = if (Test-Path $bundledExe) { Get-JavaMajorFromExe -Exe $bundledExe } else { 0 }

    if ($bundledMajor -ge $REQUIRED_JAVA_VERSION) {
        Use-EmbeddedJdk -JdkHome $EMBEDDED_JDK_DIR -Major $bundledMajor
        Write-Success "Java $bundledMajor found via $($script:JavaSource) (required: ${REQUIRED_JAVA_VERSION}+) — nothing to download"
        return $true
    }

    if ($bundledMajor -gt 0) {
        # The launcher prefers <install>\jdk whenever java.exe is there, so a bundled
        # JDK below the floor cannot be left in place and worked around with JAVA_HOME
        # — it has to be replaced. Straight to the bootstrap, which swaps it out.
        Write-Warn "The JDK bundled at $EMBEDDED_JDK_DIR is Java $bundledMajor — below the required ${REQUIRED_JAVA_VERSION}+; replacing it"
    }
    else {
        # Then JAVA_HOME, exactly as the launcher will. Probing the PATH `java` when
        # JAVA_HOME is set would validate a JVM the REPL is never going to run.
        $javaHomeExe = if ($env:JAVA_HOME) { Join-Path (Join-Path $env:JAVA_HOME "bin") "java.exe" } else { "" }
        $jhMajor = if ($javaHomeExe -and (Test-Path $javaHomeExe)) { Get-JavaMajorFromExe -Exe $javaHomeExe } else { 0 }

        if ($jhMajor -gt 0) {
            $script:JavaMajor = $jhMajor
            $script:JavaSource = "JAVA_HOME ($env:JAVA_HOME)"
        }
        else {
            if ($env:JAVA_HOME) {
                Write-Warn "JAVA_HOME is set to '$env:JAVA_HOME' but no usable java.exe was found under it"
            }
            $pathMajor = Get-JavaMajorVersion
            if ($pathMajor -gt 0) {
                $script:JavaMajor = $pathMajor
                $script:JavaSource = "PATH"
            }
        }

        if ($script:JavaMajor -ge $REQUIRED_JAVA_VERSION) {
            Write-Success "Java $($script:JavaMajor) found via $($script:JavaSource) (required: ${REQUIRED_JAVA_VERSION}+)"
            return $true
        }

        if ($script:JavaMajor -eq 0) {
            Write-Warn "No usable Java found"
        } else {
            Write-Warn "Java $($script:JavaMajor) found via $($script:JavaSource) — below the required ${REQUIRED_JAVA_VERSION}+"
        }
    }

    $jdkHome = Install-EmbeddedJdk
    if (-not $jdkHome) {
        Write-Err "Java $REQUIRED_JAVA_VERSION or higher is required for ES$EsVersion and could not be installed."
        Write-Err "Install a JDK ${REQUIRED_JAVA_VERSION}+ manually and re-run, or set JAVA_HOME to one."
        return $false
    }

    Use-EmbeddedJdk -JdkHome $jdkHome -Major (Get-JavaMajorFromExe -Exe (Join-Path (Join-Path $jdkHome "bin") "java.exe"))
    Write-Success "Java $($script:JavaMajor) ready — JAVA_HOME and PATH updated for THIS session"

    return $true
}

# =============================================================================
# List Available Versions
# =============================================================================

# One listing per artifact per run. The pre-flight below consults the same lists
# the bundle-selection block does, and an HTTP call that has already been answered
# must not be paid for - or answered differently - twice in one run.
$script:VersionListings = @{}

# Why a listing failed, kept for the pre-flight: -Quiet has to stay silent (a
# missing -all bundle is a normal, expected 404) but a genuine outage must not
# reach the user as a bare "no versions found".
$script:LastListingError = $null

function Get-AvailableVersions {
    param(
        # Default to the PLAIN artifact, never $ARTIFACT_NAME: that one is
        # deliberately mutated by the bundle-selection block below.
        [string]$Artifact = $PLAIN_ARTIFACT_NAME,
        # Bundle probing must be able to fail softly and fall back to the plain
        # artifact; the plain path keeps the original fail-hard behaviour.
        [switch]$Quiet
    )

    if ($script:VersionListings.ContainsKey($Artifact)) {
        return $script:VersionListings[$Artifact]
    }

    $apiUrl = "${JFROG_API_URL}/${Artifact}"

    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        $response = Invoke-RestMethod -Uri $apiUrl -UseBasicParsing

        $versions = $response.children |
            Where-Object { $_.folder -eq $true } |
            ForEach-Object { $_.uri.TrimStart('/') } |
            Where-Object { $_ -notmatch '^\.' } |
            Sort-Object { [Version]($_ -replace '-SNAPSHOT', '.0' -replace '[^0-9.]', '') }

        # Successes only: a listing that failed keeps its own semantics (fail hard,
        # or empty under -Quiet) if it is asked for again.
        $versions = @($versions)
        if ($versions.Count -gt 0) { $script:VersionListings[$Artifact] = $versions }

        return $versions
    }
    catch {
        $script:LastListingError = $_.Exception.Message
        if ($Quiet) { return @() }
        Write-Err "Failed to fetch versions from repository"
        Write-Err "Artifact: $Artifact"
        Write-Err $_.Exception.Message
        exit 1
    }
}

# HEAD probe: belt-and-braces over the listing check (covers a pruned scala
# variant or a listing/artifact race).
function Test-UrlExists {
    param([string]$Url)
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $Url -Method Head -UseBasicParsing -ErrorAction Stop | Out-Null
        return $true
    }
    catch { return $false }
}

if ($ListVersions) {
    Write-Info "Fetching available versions for ES$EsVersion..."

    # List the versions of the artifact the install would actually download: the
    # -all bundle by default (its OWN version line), the plain artifact under
    # -NoExtensions or when no bundle is published.
    if ($WITH_EXTENSIONS) {
        $listedArtifact = $BUNDLE_ARTIFACT_NAME
        $versions = Get-AvailableVersions -Artifact $listedArtifact -Quiet
        if (-not $versions -or $versions.Count -eq 0) {
            Write-Warn "No -all bundle versions found for $listedArtifact - listing the plain artifact instead"
            $listedArtifact = $PLAIN_ARTIFACT_NAME
            $versions = Get-AvailableVersions -Artifact $listedArtifact
        }
    } else {
        $listedArtifact = $PLAIN_ARTIFACT_NAME
        $versions = Get-AvailableVersions -Artifact $listedArtifact
    }
    $ARTIFACT_NAME = $listedArtifact

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

    # Latest of the PLAIN artifact (engine version line) - used on the fallback
    # and -NoExtensions paths; the bundle path resolves its latest from the
    # bundle listing in the bundle-selection block below.
    $versions = Get-AvailableVersions -Artifact $PLAIN_ARTIFACT_NAME

    if (-not $versions -or $versions.Count -eq 0) {
        Write-Err "No versions found"
        exit 1
    }

    # Prefer non-snapshot versions.
    # @(...) matters: with a single match the pipeline yields a bare string, and
    # [-1] on a string returns its last CHARACTER ("0.20.3" -> "3").
    $releaseVersions = @($versions | Where-Object { $_ -notmatch 'SNAPSHOT' })

    if ($releaseVersions.Count -gt 0) {
        return $releaseVersions[-1]
    }

    # Fallback to any version
    return @($versions)[-1]
}

# =============================================================================
# Pre-flight: settle the inputs before anything large is downloaded
# =============================================================================
# Resolving Java can write ~300 MB into $Target, and it used to be the FIRST thing
# to touch the disk - so a typo in -Version, an artifact that does not exist for
# the chosen -EsVersion / -ScalaVersion, or an unreachable repository was only
# discovered by Download-Jar, leaving an orphaned jdk\ behind that nothing in the
# failure output even mentioned (issue #236). The listings are the same two cheap
# calls bundle selection makes below, memoised, so this costs nothing.
function Test-RequestedVersion {
    $artifacts = @()
    if ($WITH_EXTENSIONS) { $artifacts += $BUNDLE_ARTIFACT_NAME }
    $artifacts += $PLAIN_ARTIFACT_NAME

    $known = @()
    foreach ($artifact in $artifacts) {
        $known += @(Get-AvailableVersions -Artifact $artifact -Quiet)
    }

    if ($known.Count -eq 0) {
        Write-Err "No versions found for $($artifacts -join ' or ')"
        Write-Err "Check -EsVersion $EsVersion and -ScalaVersion $ScalaVersion, and that the repository is reachable:"
        Write-Err $JFROG_API_URL
        if ($script:LastListingError) { Write-Err $script:LastListingError }
        exit 1
    }

    if ($Version -ne "latest" -and $known -notcontains $Version) {
        Write-Err "Version '$Version' is not published for $($artifacts -join ' or ')"
        Write-Err "Run with -ListVersions to see available versions."
        exit 1
    }
}

Test-RequestedVersion

# =============================================================================
# Resolve Java before anything else that depends on it
# =============================================================================
# Runs AFTER the -ListVersions early exit (listing versions must not download a
# JDK), AFTER the pre-flight above (a bad -Version must not cost a JDK download)
# and BEFORE bundle selection, which reads the resolved major.
if (-not (Resolve-Java)) { exit 1 }

# =============================================================================
# Bundle Selection: default install = ONE self-contained -all assembly
# =============================================================================
# This block owns latest-resolution for BOTH paths: the bundle listing first,
# the plain listing on fallback. The version list consulted is always the list
# of the artifact actually downloaded - the bundle has its OWN version line.

# Remember whether the user asked for "latest": if the bundle path is abandoned
# later by the existence probe, the plain latest must be re-resolved.
$REQUESTED_VERSION = $Version

$USE_BUNDLE = $false
if ($WITH_EXTENSIONS) {
    $bundleVersions = @(Get-AvailableVersions -Artifact $BUNDLE_ARTIFACT_NAME -Quiet |
        Where-Object { $_ -notmatch 'SNAPSHOT' })
    if ($bundleVersions.Count -gt 0) {
        if ($Version -eq "latest") {
            $Version = $bundleVersions[-1]      # bundle-version line (0.20.2, 0.20.3, 0.20.4, ...)
            $USE_BUNDLE = $true
            Write-Success "Resolved latest -all bundle version: $Version"
        }
        elseif ($bundleVersions -contains $Version) {
            $USE_BUNDLE = $true                 # -Version selects a BUNDLE version
        }
        else {
            Write-Warn "No -all bundle for version $Version - falling back to the plain artifact"
        }
    }
    else {
        Write-Warn "No -all bundles published for $BUNDLE_ARTIFACT_NAME - falling back to the plain artifact"
    }

    # The bundle needs Java 11+ (Arrow / logback bytecode). Resolve-Java has already
    # guaranteed >= $REQUIRED_JAVA_VERSION (11 or 17), bootstrapping a JDK if the
    # host could not supply one, so this can only fire if that guarantee is ever
    # weakened. Kept as a guard rather than deleted: silently shipping the bundle
    # to a Java 8 host is a crash at first launch, not a warning.
    if ($USE_BUNDLE -and $script:JavaMajor -gt 0 -and $script:JavaMajor -lt 11) {
        Write-Warn "Java $($script:JavaMajor) found - the -all bundle requires Java 11+; falling back to the plain artifact"
        $USE_BUNDLE = $false
        $Version = $REQUESTED_VERSION
    }
}

# Every fallback above lands on the plain engine. Unlike install.sh, this script
# has no coursier resolution to populate lib/ with the extensions, so the plain
# path yields an install with NO cross-index JOIN - say so rather than let the
# user rediscover it as a missing feature (that is issue #179's own symptom).
if ($WITH_EXTENSIONS -and -not $USE_BUNDLE) {
    Write-Warn "Cross-index JOIN and materialized-view extensions are NOT installed on this path"
    Write-Warn "(the Windows installer has no dependency-resolution fallback - only the -all bundle carries them)."
}

if (-not $USE_BUNDLE -and $Version -eq "latest") {
    # Plain/fallback path: resolve latest from the PLAIN artifact's listing
    $Version = Resolve-LatestVersion
    Write-Success "Resolved latest version: $Version"
}

if ($USE_BUNDLE) {
    $ARTIFACT_NAME = $BUNDLE_ARTIFACT_NAME
} else {
    $ARTIFACT_NAME = $PLAIN_ARTIFACT_NAME
}
$JAR_NAME = "${ARTIFACT_NAME}-${Version}-assembly.jar"
$DOWNLOAD_URL = "${JFROG_REPO_URL}/${ARTIFACT_NAME}/${Version}/${JAR_NAME}"

if ($USE_BUNDLE -and -not (Test-UrlExists -Url $DOWNLOAD_URL)) {
    Write-Warn "-all bundle not reachable at $DOWNLOAD_URL"
    Write-Warn "Falling back to the plain artifact"
    $USE_BUNDLE = $false
    $ARTIFACT_NAME = $PLAIN_ARTIFACT_NAME
    $Version = $REQUESTED_VERSION
    if ($Version -eq "latest") {
        $Version = Resolve-LatestVersion
        Write-Success "Resolved latest version: $Version"
    }
    $JAR_NAME = "${ARTIFACT_NAME}-${Version}-assembly.jar"
    $DOWNLOAD_URL = "${JFROG_REPO_URL}/${ARTIFACT_NAME}/${Version}/${JAR_NAME}"
}

# =============================================================================
# Check Prerequisites
# =============================================================================

# Resolve-Java already probed, and bootstrapped a JDK if it had to. This asserts
# the outcome rather than re-deriving it: ONE parse of `java -version` per run, so
# a localised or multi-line output cannot be read two different ways.
function Check-Prerequisites {
    Write-Info "Checking prerequisites..."

    if ($script:JavaMajor -lt $REQUIRED_JAVA_VERSION) {
        Write-Err "Java $REQUIRED_JAVA_VERSION or higher is required for ES$EsVersion."
        Write-Err "Resolved: Java $($script:JavaMajor) via $($script:JavaSource)"
        exit 1
    }

    Write-Success "Java $($script:JavaMajor) via $($script:JavaSource) (required: ${REQUIRED_JAVA_VERSION}+)"
}

# =============================================================================
# Create Directory Structure
# =============================================================================

function Create-Directories {
    Write-Info "Creating directory structure..."

    New-Item -ItemType Directory -Force -Path "$Target\bin" | Out-Null
    New-Item -ItemType Directory -Force -Path "$Target\conf" | Out-Null
    New-Item -ItemType Directory -Force -Path "$Target\lib" | Out-Null
    New-Item -ItemType Directory -Force -Path "$Target\logs" | Out-Null

    Write-Success "Created $Target\{bin,conf,lib,logs}"
}

# =============================================================================
# Download File Helper
# =============================================================================

function Download-File {
    param(
        [string]$Url,
        [string]$Dest,
        [string]$Description
    )

    Write-Info "Downloading $Description..."

    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $Url -OutFile $Dest -UseBasicParsing -ErrorAction Stop
        Write-Success "Downloaded $Description"
        return $true
    }
    catch {
        Write-Warn "Failed to download $Description from $Url"
        return $false
    }
}

# =============================================================================
# Download JAR
# =============================================================================

function Download-Jar {
    Write-Info "Downloading $JAR_NAME..."
    Write-Info "URL: $DOWNLOAD_URL"

    $dest = "$Target\lib\$JAR_NAME"

    # The bundle is ~309 MB. On Windows PowerShell 5.1, Invoke-WebRequest calls
    # Write-Progress per chunk, which slows a download of this size by an order of
    # magnitude while showing nothing in many hosts - it just looks hung.
    $previousProgress = $ProgressPreference
    $ProgressPreference = 'SilentlyContinue'
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
    finally { $ProgressPreference = $previousProgress }
}

# =============================================================================
# Extract the licence bundle (bundle installs only)
# =============================================================================
# The -all jar mixes Apache-2.0 + ELv2 + proprietary: materialize licenses/ and
# NOTICE into the install root so the visible tree is not just the jar.

function Extract-BundleLicenses {
    if (-not $USE_BUNDLE) { return }

    # Join-Path, not "$Target\lib\...": the ZipFile APIs below are raw .NET and
    # do not normalise separators the way the PowerShell cmdlets do.
    $jar = Join-Path (Join-Path $Target 'lib') $JAR_NAME
    Write-Info "Extracting licence bundle (licenses/ + NOTICE) from $JAR_NAME..."

    try {
        # PowerShell 5.1 needs the assembly loaded; on PowerShell 7 the types are
        # already present and Add-Type is a no-op.
        Add-Type -AssemblyName System.IO.Compression.FileSystem -ErrorAction SilentlyContinue
        $zip = [System.IO.Compression.ZipFile]::OpenRead($jar)
        try {
            $wanted = $zip.Entries | Where-Object {
                $_.FullName -eq 'NOTICE' -or $_.FullName -like 'licenses/*'
            }
            foreach ($entry in $wanted) {
                if ([string]::IsNullOrEmpty($entry.Name)) { continue }   # directory entry
                # Refuse traversal entries: -like 'licenses/*' happily matches
                # 'licenses/../../evil.txt', and ExtractToFile overwrites.
                if ($entry.FullName -match '(^|/)\.\.(/|$)') {
                    Write-Warn "Skipping suspicious archive entry: $($entry.FullName)"
                    continue
                }
                $dest = Join-Path $Target ($entry.FullName -replace '/', [IO.Path]::DirectorySeparatorChar)
                $destDir = Split-Path -Parent $dest
                if (-not (Test-Path $destDir)) {
                    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
                }
                [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $dest, $true)
            }
        }
        finally { $zip.Dispose() }

        Write-Success "Extracted licenses/ and NOTICE to $Target"
    }
    catch {
        # Never fail the install over the licence copy: the canonical copies
        # ship inside the jar.
        Write-Warn "Could not extract the licence bundle: $($_.Exception.Message)"
        Write-Warn "The canonical copies remain inside the jar: licenses/ and NOTICE."
    }
}

# =============================================================================
# License Notice
# =============================================================================

function Show-LicenseNotice {
    if ($USE_BUNDLE) {
        Write-Host "  License: this bundle contains the Apache-2.0 SoftClient4ES engine PLUS"
        Write-Host "  SoftClient4ES extensions under the Elastic License 2.0 and the proprietary"
        Write-Host "  cross-index JOIN engine (free to use; see the licenses/ directory and NOTICE"
        Write-Host "  in the install root - canonical copies ship inside the jar)."
        Write-Host "  Quota enforcement is active. For a pure Apache-2.0 install re-run with -NoExtensions."
    }
    else {
        Write-Host "  License: pure Apache-2.0 engine (no extensions)."
    }
}

# =============================================================================
# Download Documentation and License
# =============================================================================

function Download-Docs {
    Write-Info "Downloading documentation and license..."

    # Download README.md
    $readmeResult = Download-File -Url $README_URL -Dest "$Target\README.md" -Description "README.md"
    if (-not $readmeResult) {
        Write-Warn "README.md download failed, creating minimal version"
        Create-MinimalReadme
    }

    # Download LICENSE
    Download-File -Url $LICENSE_URL -Dest "$Target\LICENSE" -Description "LICENSE" | Out-Null
}

# =============================================================================
# Create Minimal README (Fallback)
# =============================================================================

function Create-MinimalReadme {
    $readmeContent = @'
# SoftClient4ES

SQL Gateway for Elasticsearch

## Quick Start

```powershell
# Start the REPL
.\bin\softclient4es.bat

# Execute a single command
.\bin\softclient4es.bat -c "SHOW TABLES"

# Get help
.\bin\softclient4es.bat --help
```

## Configuration

Edit `conf\application.conf` to configure default connection settings.

## Documentation

Full documentation available at:
https://github.com/SOFTNETWORK-APP/SoftClient4ES

## License

See LICENSE file for details.
'@

    $readmeContent | Out-File -FilePath "$Target\README.md" -Encoding UTF8
    Write-Success "Created minimal README.md"
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
# Create Logback Configuration
# =============================================================================

function Create-LogbackConfig {
    Write-Info "Creating logback configuration..."

    $logbackContent = @'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

    <variable name="LOG_DIR" value="${log.dir:-logs}" />
    <variable name="LOG_FILE" value="softclient4es" />

    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${LOG_DIR}/${LOG_FILE}.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>${LOG_DIR}/${LOG_FILE}-%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>7</maxHistory>
            <totalSizeCap>1GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%date{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="ASYNC" class="ch.qos.logback.classic.AsyncAppender">
        <queueSize>8192</queueSize>
        <neverBlock>true</neverBlock>
        <appender-ref ref="FILE" />
    </appender>

    <!-- File-only by design: the REPL owns stdout. An appender that no
         <appender-ref> points at is reported as unreferenced, and that single
         WARN makes logback dump its entire status log to stdout on every
         launch. Reference any new appender from <root> or a <logger>, or
         leave it out. -->

    <logger name="app.softnetwork.elastic" level="INFO" />
    <logger name="org.apache.http" level="WARN" />
    <logger name="org.elasticsearch" level="WARN" />

    <root level="INFO">
        <appender-ref ref="ASYNC" />
    </root>

</configuration>
'@

    $logbackContent | Out-File -FilePath "$Target\conf\logback.xml" -Encoding UTF8

    Write-Success "Created $Target\conf\logback.xml"
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
set LOGBACK_FILE=%BASE_DIR%\conf\logback.xml
set LOG_DIR=%BASE_DIR%\logs
set REQUIRED_JAVA=$REQUIRED_JAVA_VERSION

if not exist "%JAR_FILE%" (
    echo Error: JAR file not found: %JAR_FILE% >&2
    exit /b 1
)

REM Create logs directory if it doesn't exist
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Java resolution, in the SAME order the installer used:
REM   1. the JDK bundled into this install (present only when the installer had to
REM      bootstrap one because the host had no Java, or too old a Java)
REM   2. %JAVA_HOME%
REM   3. whatever `java` is on PATH
REM Prepending to PATH rather than calling an absolute exe keeps every `java`
REM below unchanged and sidesteps quoting a path that contains spaces. `setlocal`
REM at the top means this PATH edit dies with the script.
REM Each `if` is its own line: cmd expands every %%VAR%% in a parenthesised block
REM in ONE parse pass, so a block would not see the value just assigned.
if exist "%BASE_DIR%\jdk\bin\java.exe" set "JAVA_HOME=%BASE_DIR%\jdk"
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "PATH=%JAVA_HOME%\bin;%PATH%"

if "%JAVA_OPTS%"=="" set JAVA_OPTS=-Xmx512m

REM Java major version (1.8.x -> 8, 11.x -> 11), also our Java presence check.
REM cmd expands every %VAR% in a parenthesised block in ONE parse pass, so the
REM second FOR must stay OUTSIDE the first block to see JVER. %%~v strips the
REM quotes FOR/F leaves around the version token; the per-iteration `if not
REM defined` keeps the FIRST matching line (parity with install.sh's head -n 1).
set JAVA_MAJOR=0
set JVER=
for /f tokens^=3 %%v in ('java -version 2^>^&1 ^| findstr /i "version"') do if not defined JVER set JVER=%%~v
if not defined JVER (
    echo Error: Java is not installed. Java %REQUIRED_JAVA%+ is required. >&2
    exit /b 1
)
for /f "tokens=1,2 delims=." %%a in ("%JVER%") do if "%%a"=="1" (set JAVA_MAJOR=%%b) else (set JAVA_MAJOR=%%a)

REM Refuse a JVM below the floor instead of letting it fail with
REM UnsupportedClassVersionError, which names a class-file version rather than a
REM Java one and reads as a broken install. The .ps1 launcher has always had this
REM check; the .bat computed JAVA_MAJOR and spent it only on --add-opens, so a
REM stale %JAVA_HOME% - which outranks the PATH here, deliberately, because that
REM is the JVM the installer probed - silently won (issue #235).
REM The "not 0" guard keeps parity with the .ps1 launcher: refuse only a version
REM we positively read as too low, never one we failed to parse.
if not "%JAVA_MAJOR%"=="0" if %JAVA_MAJOR% LSS %REQUIRED_JAVA% (
    echo Error: Java %REQUIRED_JAVA%+ is required. Found: Java %JAVA_MAJOR% >&2
    if defined JAVA_HOME echo        JAVA_HOME takes precedence over the PATH here - point it at a Java %REQUIRED_JAVA%+ JDK, or clear it. >&2
    exit /b 1
)

REM The extensions (Apache Arrow / DuckDB) need reflective access on Java 9+.
REM JAVA_MAJOR is initialised to 0 above so this comparison always has a left
REM operand (an empty one is a cmd syntax error that aborts the whole script).
set EXTRA_OPTS=
if %JAVA_MAJOR% GEQ 9 set EXTRA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true

REM Logback configuration (quoted: the default target lives under %USERPROFILE%,
REM which routinely contains a space)
set LOGBACK_OPTS=
if exist "%LOGBACK_FILE%" set LOGBACK_OPTS="-Dlogback.configurationFile=%LOGBACK_FILE%"

REM The engine assembly comes first on the classpath so it wins any conflict;
REM lib\* brings the extension jars, discovered via the ServiceLoader SPI.
REM (java -jar would ignore the classpath entirely - do not switch back.)
java %JAVA_OPTS% %EXTRA_OPTS% -Dconfig.file="%CONFIG_FILE%" -Dlog.dir="%LOG_DIR%" %LOGBACK_OPTS% -cp "%JAR_FILE%;%BASE_DIR%\lib\*" app.softnetwork.elastic.client.Cli %*

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
`$LogbackFile = "`$BaseDir\conf\logback.xml"
`$LogDir = "`$BaseDir\logs"
`$RequiredJava = $REQUIRED_JAVA_VERSION

if (-not (Test-Path `$JarFile)) {
    Write-Error "JAR file not found: `$JarFile"
    exit 1
}

# Create logs directory if it doesn't exist
if (-not (Test-Path `$LogDir)) {
    New-Item -ItemType Directory -Path `$LogDir | Out-Null
}

# Java resolution, in the SAME order the installer used:
#   1. the JDK bundled into this install (present only when the installer had to
#      bootstrap one because the host had no Java, or too old a Java)
#   2. `$env:JAVA_HOME
#   3. whatever `java` is on PATH
# Prepending to PATH keeps every `java` below unchanged; the assignment is
# process-scoped, so it dies with this script.
`$BundledJdk = Join-Path `$BaseDir "jdk"
if (Test-Path (Join-Path `$BundledJdk "bin\java.exe")) {
    `$env:JAVA_HOME = `$BundledJdk
}
if (`$env:JAVA_HOME -and (Test-Path (Join-Path `$env:JAVA_HOME "bin\java.exe"))) {
    `$env:PATH = (Join-Path `$env:JAVA_HOME "bin") + ";" + `$env:PATH
}

# Check Java. Select-Object -First 1 matters: with two matching lines (e.g. a
# deprecation notice from _JAVA_OPTIONS) the pipeline yields an array whose
# ToString() is "System.Object[]", no regex matches, and the version reads 0.
try {
    `$javaVersionOutput = & java -version 2>&1 | Select-String -Pattern 'version' | Select-Object -First 1
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
        if (`$env:JAVA_HOME) {
            Write-Error "JAVA_HOME takes precedence over the PATH here - point it at a Java `$RequiredJava+ JDK, or clear it."
        }
        exit 1
    }
}
catch {
    Write-Error "Java is not installed. Java `$RequiredJava+ is required."
    exit 1
}

# JAVA_OPTS may carry several flags: split it so each becomes its own argument
# (a single string would reach the JVM as one unparsable option). Where-Object
# drops the empty element a leading space produces - java would read it as the
# main class name and refuse to start.
`$JavaOpts = if (`$env:JAVA_OPTS) { @(`$env:JAVA_OPTS -split '\s+' | Where-Object { `$_ }) } else { @("-Xmx512m") }

# The extensions (Apache Arrow / DuckDB) need reflective access on Java 9+
`$ExtraOpts = @()
if (`$javaVersion -ge 9) {
    `$ExtraOpts = @("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
}

# Logback configuration
`$LogbackOpts = @()
if (Test-Path `$LogbackFile) {
    `$LogbackOpts = @("-Dlogback.configurationFile=`$LogbackFile")
}

# The engine assembly comes first on the classpath so it wins any conflict;
# lib\* brings the extension jars, discovered via the ServiceLoader SPI.
# (java -jar would ignore the classpath entirely - do not switch back.)
& java @JavaOpts @ExtraOpts "-Dconfig.file=`$ConfigFile" "-Dlog.dir=`$LogDir" @LogbackOpts -cp "`$JarFile;`$BaseDir\lib\*" app.softnetwork.elastic.client.Cli `$args

# Propagate the CLI's exit status (install.sh uses `exec java` for this)
exit `$LASTEXITCODE
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
    if ($USE_BUNDLE) {
        $installType = "-all bundle (engine + extensions; the bundle has its OWN version line)"
        $licenseLine = @"

License:            Apache-2.0 engine PLUS extensions under the Elastic License 2.0
                    and the proprietary cross-index JOIN engine (free to use).
                    See licenses\ and NOTICE in the install root.
"@
    }
    else {
        $installType = "plain engine (pure Apache-2.0, no extensions)"
        $licenseLine = @"

License:            pure Apache-2.0 engine (no extensions).
"@
    }

    $versionContent = @"
SoftClient4ES Installation Info
================================
Installed:          $(Get-Date -Format "yyyy-MM-dd HH:mm:ss UTC")
Elasticsearch:      $EsVersion
Version:            $Version
Scala:              $ScalaVersion
Java Required:      ${REQUIRED_JAVA_VERSION}+
Java In Use:        $($script:JavaMajor) via $($script:JavaSource)
Artifact:           $ARTIFACT_NAME
Install type:       $installType
$licenseLine
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
    Write-Host "  Java in use:            $($script:JavaMajor) via $($script:JavaSource)"
    if ($USE_BUNDLE) {
        Write-Host "  Install type:           -all bundle (engine + extensions, incl. cross-index JOIN)"
    } else {
        Write-Host "  Install type:           plain engine (pure Apache-2.0, no extensions)"
    }
    Write-Host ""
    Write-Host "  Directory structure:"
    Write-Host "    $Target\"
    Write-Host "    +-- bin\"
    Write-Host "    |   +-- softclient4es.bat"
    Write-Host "    |   \-- softclient4es.ps1"
    Write-Host "    +-- conf\"
    Write-Host "    |   +-- application.conf"
    Write-Host "    |   \-- logback.xml"
    Write-Host "    +-- lib\"
    Write-Host "    |   \-- $JAR_NAME"
    Write-Host "    +-- logs\"
    Write-Host "    |   \-- (runtime logs)"
    if ($script:EmbeddedJdkHome) {
        Write-Host "    +-- jdk\"
        Write-Host "    |   \-- (bundled Temurin $BOOTSTRAP_JAVA_VERSION - the launcher prefers it)"
    }
    if ($USE_BUNDLE) {
        Write-Host "    +-- licenses\"
        Write-Host "    |   \-- (per-component licences)"
        Write-Host "    +-- NOTICE"
    }
    Write-Host "    +-- LICENSE"
    Write-Host "    +-- README.md"
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
    Write-Host "  Documentation:"
    Write-Host "    Get-Content $Target\README.md" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Configuration:"
    Write-Host "    Application: $Target\conf\application.conf"
    Write-Host "    Logging:     $Target\conf\logback.xml"
    Write-Host ""
    Write-Host "  Log files:"
    Write-Host "    $Target\logs\softclient4es.log" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  To uninstall:"
    Write-Host "    $Target\uninstall.ps1" -ForegroundColor Cyan
    Write-Host ""
    # Repeat the licence status here: the mid-install notice has scrolled well
    # off screen by now (install.sh states it in its summary for the same reason).
    Show-LicenseNotice
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

# The finally runs on `exit` too, so it covers every failure path below - which is
# the point: whatever went wrong, the user is told about the JDK sitting in $Target
# rather than discovering ~300 MB of it later (issue #236).
$script:InstallCompleted = $false
try {
    Check-Prerequisites
    Create-Directories
    Download-Jar
    Extract-BundleLicenses
    Show-LicenseNotice
    Download-Docs
    Create-Config
    Create-LogbackConfig    # <-- Création du fichier logback.xml
    Create-Launcher
    Create-Uninstaller
    Create-VersionInfo
    Print-Summary
    $script:InstallCompleted = $true
}
finally {
    if (-not $script:InstallCompleted -and $script:EmbeddedJdkHome) {
        Write-Host ""
        Write-Warn "The install did not complete. A portable Temurin $BOOTSTRAP_JAVA_VERSION JDK (~300 MB) is at:"
        Write-Warn "  $($script:EmbeddedJdkHome)"
        Write-Warn "It is kept on purpose - a re-run reuses it instead of downloading it again."
        Write-Warn "Delete that directory if you are not going to retry."
    }
}
