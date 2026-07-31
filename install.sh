#!/usr/bin/env bash
#
# SoftClient4ES Installation Script
# For Linux and macOS
#

set -e

# =============================================================================
# Default Configuration
# =============================================================================

DEFAULT_TARGET_DIR="$HOME/softclient4es"
DEFAULT_ES_VERSION="8"
DEFAULT_SOFT_VERSION="latest"
DEFAULT_SCALA_VERSION="2.13"

JFROG_REPO_URL="https://softnetwork.jfrog.io/artifactory/releases/app/softnetwork/elastic"
JFROG_API_URL="https://softnetwork.jfrog.io/artifactory/api/storage/releases/app/softnetwork/elastic"

GITHUB_RAW_URL="https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/refs/heads/main"
README_URL="${GITHUB_RAW_URL}/documentation/client/repl.md"
LICENSE_URL="${GITHUB_RAW_URL}/LICENSE"

# =============================================================================
# Colors and Output
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# IMPORTANT: All logging goes to stderr so it doesn't pollute function return values
info()    { echo -e "${BLUE}[INFO]${NC} $1" >&2; }
success() { echo -e "${GREEN}[OK]${NC} $1" >&2; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
error()   { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# =============================================================================
# Detect OS
# =============================================================================

detect_os() {
    case "$OSTYPE" in
        linux*)   echo "linux" ;;
        darwin*)  echo "macos" ;;
        msys*)    echo "windows" ;;
        cygwin*)  echo "windows" ;;
        *)        echo "unknown" ;;
    esac
}

OS_TYPE=$(detect_os)

# =============================================================================
# Portable Version Sort
# =============================================================================

# Sort versions portably (works on both GNU and BSD)
sort_versions() {
    # Try sort -V first (GNU sort)
    if echo "1.0.0" | sort -V &>/dev/null; then
        sort -V
    else
        # Fallback: numeric sort by version components
        sort -t. -k1,1n -k2,2n -k3,3n -k4,4n
    fi
}

# =============================================================================
# Portable JSON URI Extraction
# =============================================================================

# Extract URIs from JFrog API JSON response
# Works on both GNU grep (Linux) and BSD grep (macOS)
# Works on both compact (single-line) and pretty-printed JSON
# Filters out hidden files (starting with .) and maven-metadata.xml
extract_uris_from_json() {
    # grep -oE prints each match on its own line, handles any JSON format
    # Step 1: extract "uri" : "/..." substrings (path URIs starting with /)
    # Step 2: extract the /value part
    # Step 3: strip the leading /
    grep -oE '"uri"[[:space:]]*:[[:space:]]*"/[^"]+"' \
        | grep -oE '/[^"]+' \
        | cut -c2- \
        | grep -v '^\.' \
        | grep -v '^maven-metadata' \
        | grep -v '\.xml$' \
        | grep -v '\.md5$' \
        | grep -v '\.sha'
}

# =============================================================================
# Portable Java Version Detection
# =============================================================================

# Get Java major version (works on Linux and macOS)
get_java_version() {
    local java_version_output
    java_version_output=$(java -version 2>&1 | head -n 1)

    local java_version=""

    # Extract the version string between quotes
    # Examples: "1.8.0_292", "11.0.11", "17.0.1", "21"
    local version_string
    version_string=$(echo "$java_version_output" | sed 's/.*"\(.*\)".*/\1/')

    if [[ -z "$version_string" ]]; then
        echo ""
        return
    fi

    # Check if it starts with "1." (old format like 1.8)
    if [[ "$version_string" == 1.* ]]; then
        # Old format: 1.8.0_xxx -> extract 8
        java_version=$(echo "$version_string" | cut -d'.' -f2)
    else
        # New format: 11.0.11, 17.0.1, 21 -> extract first number
        java_version=$(echo "$version_string" | cut -d'.' -f1)
    fi

    echo "$java_version"
}

# =============================================================================
# Help
# =============================================================================

show_help() {
    cat << EOF
SoftClient4ES Installation Script

Usage: $0 [OPTIONS]

Options:
  -t, --target <dir>       Installation directory (default: $DEFAULT_TARGET_DIR)
  -e, --es-version <ver>   Elasticsearch major version: 6, 7, 8, 9 (default: $DEFAULT_ES_VERSION)
  -v, --version <ver>      Version to install (default: latest). On the default
                           path this selects a BUNDLE version (the -all artifact
                           has its own version line); with --no-extensions (or
                           when no bundle matches) it selects an ENGINE version.
  -s, --scala <ver>        Scala version (default: $DEFAULT_SCALA_VERSION)
  -l, --list-versions      List available versions for the specified ES version
                           (bundle versions by default, engine versions with
                           --no-extensions)
      --no-extensions      Install the plain, pure Apache-2.0 engine only
                           (no cross-index JOINs, no materialized views)
  -h, --help               Show this help message

Default install = ONE self-contained -all bundle:
  engine + community extensions + arrow JOIN extension + all dependencies in a
  single jar (no install-time dependency resolution). The bundle contains
  components under the Elastic License 2.0 plus the proprietary JOIN engine
  (free to use) — it is NOT a pure Apache-2.0 artifact.
  Fallback (no bundle published / engine-version -v / Java < 11): the plain
  Apache-2.0 engine assembly + extensions resolved via coursier as before.
  --no-extensions always yields a pure Apache-2.0 install.
  Bundle bugs: https://github.com/SOFTNETWORK-APP/SoftClient4ES/issues

Extensions on the FALLBACK path (resolved with all their dependencies):
  - community extensions (materialized views): always — any engine version, Java 8+
  - arrow extensions (cross-index JOINs): engine >= 0.20; requires Java 11+
    (Apache Arrow constraint)

Java Requirements:
  ES 6, 7, 8  →  Java 11 or higher (the 0.20+ CLI bundles logback 1.5.x = Java-11 bytecode)
  ES 9        →  Java 17 or higher

Examples:
  $0
  $0 --list-versions --es-version 8
  $0 --target /opt/softclient4es --es-version 8 --no-extensions
  $0 -t ~/tools/softclient4es -e 7 -v 0.20.2 --no-extensions

Detected OS: $OS_TYPE

EOF
    exit 0
}

# =============================================================================
# Parse Arguments
# =============================================================================

TARGET_DIR="$DEFAULT_TARGET_DIR"
ES_VERSION="$DEFAULT_ES_VERSION"
SOFT_VERSION="$DEFAULT_SOFT_VERSION"
SCALA_VERSION="$DEFAULT_SCALA_VERSION"
LIST_VERSIONS=false
WITH_EXTENSIONS=true

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)
            TARGET_DIR="$2"
            shift 2
            ;;
        -e|--es-version)
            ES_VERSION="$2"
            shift 2
            ;;
        -v|--version)
            SOFT_VERSION="$2"
            shift 2
            ;;
        -s|--scala)
            SCALA_VERSION="$2"
            shift 2
            ;;
        -l|--list-versions)
            LIST_VERSIONS=true
            shift
            ;;
        --no-extensions)
            WITH_EXTENSIONS=false
            shift
            ;;
        --extensions)
            WITH_EXTENSIONS=true
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            error "Unknown option: $1"
            show_help
            ;;
    esac
done

# =============================================================================
# Validate Inputs
# =============================================================================

if [[ ! "$ES_VERSION" =~ ^[6-9]$ ]]; then
    error "Invalid Elasticsearch version: $ES_VERSION (must be 6, 7, 8, or 9)"
    exit 1
fi

# =============================================================================
# Derived Variables
# =============================================================================

# Plain artifact: the pure Apache-2.0 engine assembly (published by elasticsql).
PLAIN_ARTIFACT_NAME="softclient4es${ES_VERSION}-cli_${SCALA_VERSION}"
# Bundle artifact: the self-contained -all assembly (engine + community
# extensions + arrow JOIN extension + all dependencies), published by the
# softclient4es-repl packaging repo on its OWN version line.
BUNDLE_ARTIFACT_NAME="softclient4es${ES_VERSION}-cli-all_${SCALA_VERSION}"
# ARTIFACT_NAME is finalized by the bundle-selection block below; until then it
# refers to the plain artifact (fallback/legacy paths).
ARTIFACT_NAME="$PLAIN_ARTIFACT_NAME"

# =============================================================================
# Get Required Java Version
# =============================================================================

get_required_java_version() {
    local es_ver="$1"
    if [[ "$es_ver" == "9" ]]; then
        echo 17
    else
        # The 0.20+ CLI bundles logback 1.5.x (Java-11 bytecode): the REPL
        # does not start on Java 8 — verified empirically (--help crashes
        # with UnsupportedClassVersionError on Zulu 8).
        echo 11
    fi
}

REQUIRED_JAVA_VERSION=$(get_required_java_version "$ES_VERSION")

# =============================================================================
# Fetch Versions from Repository
# =============================================================================

fetch_versions() {
    local artifact="${1:-$PLAIN_ARTIFACT_NAME}"
    local api_url="${JFROG_API_URL}/${artifact}"
    local response=""

    if command -v curl &> /dev/null; then
        response=$(curl -fsSL "$api_url" 2>/dev/null)
    elif command -v wget &> /dev/null; then
        response=$(wget -qO- "$api_url" 2>/dev/null)
    else
        error "curl or wget is required"
        return 1
    fi

    if [[ -z "$response" ]]; then
        error "Failed to fetch versions from repository"
        error "URL: $api_url"
        error "Artifact: $artifact"
        return 1
    fi

    # Parse JSON response and extract clean version list
    echo "$response" | extract_uris_from_json | sort_versions
}

# =============================================================================
# List Available Versions
# =============================================================================

list_available_versions() {
    info "Fetching available versions for ES$ES_VERSION..."

    # List the versions of the artifact the install would actually download:
    # the -all bundle by default (its OWN version line), the plain artifact
    # under --no-extensions or when no bundle is published.
    local listed_artifact versions
    if [[ "$WITH_EXTENSIONS" == true ]]; then
        listed_artifact="$BUNDLE_ARTIFACT_NAME"
        # `|| true`: an empty bundle listing must fall through (set -e is active)
        versions=$(fetch_versions "$listed_artifact" 2>/dev/null || true)
        if [[ -z "$versions" ]]; then
            warn "No -all bundle versions found for $listed_artifact — listing the plain artifact instead"
            listed_artifact="$PLAIN_ARTIFACT_NAME"
            versions=$(fetch_versions "$listed_artifact")
        fi
    else
        listed_artifact="$PLAIN_ARTIFACT_NAME"
        versions=$(fetch_versions "$listed_artifact")
    fi

    if [[ -z "$versions" ]]; then
        error "No versions found for $listed_artifact"
        exit 1
    fi

    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Available SoftClient4ES Versions for Elasticsearch $ES_VERSION${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${YELLOW}Artifact:${NC} $listed_artifact"
    echo -e "  ${YELLOW}Java required:${NC} $REQUIRED_JAVA_VERSION+"
    echo ""
    echo -e "  ${GREEN}Versions:${NC}"
    echo ""

    # Display versions
    local count=0
    while IFS= read -r version; do
        if [[ -n "$version" ]]; then
            echo "    • $version"
            (( ++count ))
        fi
    done <<< "$versions"

    echo ""
    echo -e "  ${BLUE}Total: $count version(s)${NC}"
    echo ""
    echo "  To install a specific version:"
    echo -e "    ${CYAN}$0 --es-version $ES_VERSION --version <version>${NC}"
    echo ""

    exit 0
}

# Run list versions if requested
if [[ "$LIST_VERSIONS" == true ]]; then
    list_available_versions
fi

# =============================================================================
# Resolve Latest Version
# =============================================================================

resolve_latest_version() {
    info "Resolving latest version..."

    # Latest of the PLAIN artifact (engine version line) — used on the
    # fallback/--no-extensions paths; the bundle path resolves its latest
    # from the bundle listing in the bundle-selection block below.
    local versions
    versions=$(fetch_versions "$PLAIN_ARTIFACT_NAME")

    if [[ -z "$versions" ]]; then
        error "Could not fetch versions"
        echo ""
        return 1
    fi

    # Prefer non-snapshot versions
    local latest
    latest=$(echo "$versions" | grep -v 'SNAPSHOT' | tail -1)

    # Fallback to any version if no release found
    if [[ -z "$latest" ]]; then
        latest=$(echo "$versions" | tail -1)
    fi

    if [[ -z "$latest" ]]; then
        error "Could not determine latest version"
        echo ""
        return 1
    fi

    # Return ONLY the version string (no other output)
    echo "$latest"
}

# =============================================================================
# Bundle Selection: default install = ONE self-contained -all assembly
# =============================================================================
# (This block owns latest-resolution for BOTH paths: the bundle listing first,
#  the plain listing on fallback. The version list consulted is always the list
#  of the artifact actually downloaded — the bundle has its OWN version line.)

# Remember whether the user asked for "latest" (needed if the bundle path is
# abandoned later by the existence probe — the plain latest must be re-resolved).
REQUESTED_VERSION="$SOFT_VERSION"

USE_BUNDLE=false
if [[ "$WITH_EXTENSIONS" == true ]]; then
    # NOTE: the trailing `|| true` matters — with `set -e` an empty listing
    # (no bundles published yet → grep exits 1) must fall back, not abort.
    bundle_versions=$(fetch_versions "$BUNDLE_ARTIFACT_NAME" 2>/dev/null | grep -v 'SNAPSHOT' || true)
    if [[ -n "$bundle_versions" ]]; then
        if [[ "$SOFT_VERSION" == "latest" ]]; then
            SOFT_VERSION=$(echo "$bundle_versions" | tail -1)   # bundle-version line (0.20.1, 0.20.2, ...)
            USE_BUNDLE=true
            success "Resolved latest -all bundle version: $SOFT_VERSION"
        elif echo "$bundle_versions" | grep -qx "$SOFT_VERSION"; then
            USE_BUNDLE=true                                     # -v selects a BUNDLE version on the default path
        else
            warn "No -all bundle for version $SOFT_VERSION — falling back to the plain artifact + extensions resolution"
        fi
    else
        warn "No -all bundles published for $BUNDLE_ARTIFACT_NAME — falling back to the plain artifact + extensions resolution"
    fi
    # Belt-and-braces: the bundle needs Java 11+ (Arrow/logback bytecode);
    # check_prerequisites aborts below the ES-version floor anyway.
    java_version=$(get_java_version)
    if [[ -n "$java_version" ]] && [[ "$java_version" -lt 11 ]]; then
        if [[ "$USE_BUNDLE" == true ]]; then
            warn "Java $java_version found — the -all bundle requires Java 11+; falling back to the plain artifact"
            USE_BUNDLE=false
            SOFT_VERSION="$REQUESTED_VERSION"
        fi
    fi
fi

if [[ "$USE_BUNDLE" != true ]] && [[ "$SOFT_VERSION" == "latest" ]]; then
    # Plain/fallback path: resolve latest from the PLAIN artifact's listing
    # (`|| true` keeps set -e from aborting before the empty-check below)
    SOFT_VERSION=$(resolve_latest_version || true)
    if [[ -z "$SOFT_VERSION" ]]; then
        error "Failed to resolve latest version"
        error "Try specifying a version manually with --version"
        error "Or run with --list-versions to see available versions"
        exit 1
    fi
    success "Resolved latest version: $SOFT_VERSION"
fi

if [[ "$USE_BUNDLE" == true ]]; then
    ARTIFACT_NAME="$BUNDLE_ARTIFACT_NAME"
else
    ARTIFACT_NAME="$PLAIN_ARTIFACT_NAME"
fi
JAR_NAME="${ARTIFACT_NAME}-${SOFT_VERSION}-assembly.jar"
DOWNLOAD_URL="${JFROG_REPO_URL}/${ARTIFACT_NAME}/${SOFT_VERSION}/${JAR_NAME}"

# ── Existence probe (belt-and-braces over the listing check: covers a pruned
#    scala variant or a listing/artifact race). Self-contained — never rely on
#    $DOWNLOADER, which is only set later inside check_prerequisites().
probe_url() {
    local url="$1"
    if command -v curl &> /dev/null; then
        curl -fsI "$url" > /dev/null 2>&1
    elif command -v wget &> /dev/null; then
        wget -q --spider "$url" 2>/dev/null
    else
        return 1
    fi
}

if [[ "$USE_BUNDLE" == true ]]; then
    if ! probe_url "$DOWNLOAD_URL"; then
        warn "-all bundle not reachable at $DOWNLOAD_URL"
        warn "Falling back to the plain artifact + extensions resolution"
        USE_BUNDLE=false
        SOFT_VERSION="$REQUESTED_VERSION"
        if [[ "$SOFT_VERSION" == "latest" ]]; then
            SOFT_VERSION=$(resolve_latest_version || true)
            if [[ -z "$SOFT_VERSION" ]]; then
                error "Failed to resolve latest version"
                exit 1
            fi
            success "Resolved latest version: $SOFT_VERSION"
        fi
        ARTIFACT_NAME="$PLAIN_ARTIFACT_NAME"
        JAR_NAME="${ARTIFACT_NAME}-${SOFT_VERSION}-assembly.jar"
        DOWNLOAD_URL="${JFROG_REPO_URL}/${ARTIFACT_NAME}/${SOFT_VERSION}/${JAR_NAME}"
    fi
fi

# =============================================================================
# Check Prerequisites
# =============================================================================

check_prerequisites() {
    info "Checking prerequisites..."
    info "Detected OS: $OS_TYPE"

    # Check Java
    if ! command -v java &> /dev/null; then
        error "Java is not installed."
        error "ES$ES_VERSION requires Java $REQUIRED_JAVA_VERSION or higher."
        case "$OS_TYPE" in
            macos)
                error "Install with: brew install openjdk@$REQUIRED_JAVA_VERSION"
                ;;
            linux)
                error "Install with: sudo apt install openjdk-$REQUIRED_JAVA_VERSION-jdk"
                error "         or: sudo yum install java-$REQUIRED_JAVA_VERSION-openjdk"
                ;;
        esac
        exit 1
    fi

    # Get Java version (portable)
    local java_version
    java_version=$(get_java_version)

    if [[ -z "$java_version" ]]; then
        warn "Could not determine Java version. Proceeding anyway..."
    else
        if [[ "$java_version" -lt "$REQUIRED_JAVA_VERSION" ]]; then
            error "Java $REQUIRED_JAVA_VERSION or higher is required for ES$ES_VERSION."
            error "Found: Java $java_version"
            exit 1
        fi
        success "Java $java_version found (required: $REQUIRED_JAVA_VERSION+)"
    fi

    # Check curl or wget
    if command -v curl &> /dev/null; then
        DOWNLOADER="curl"
    elif command -v wget &> /dev/null; then
        DOWNLOADER="wget"
    else
        error "curl or wget is required for downloading artifacts."
        exit 1
    fi
    success "$DOWNLOADER found"
}

# =============================================================================
# Create Directory Structure
# =============================================================================

create_directories() {
    info "Creating directory structure..."

    mkdir -p "$TARGET_DIR/bin"
    mkdir -p "$TARGET_DIR/conf"
    mkdir -p "$TARGET_DIR/lib"
    mkdir -p "$TARGET_DIR/logs"

    success "Created $TARGET_DIR/{bin,conf,lib,logs}"
}

# =============================================================================
# Download File Helper
# =============================================================================

download_file() {
    local url="$1"
    local dest="$2"
    local description="$3"

    info "Downloading $description..."

    if [[ "$DOWNLOADER" == "curl" ]]; then
        if ! curl -fsSL -o "$dest" "$url" 2>/dev/null; then
            warn "Failed to download $description from $url"
            return 1
        fi
    else
        if ! wget -q -O "$dest" "$url" 2>/dev/null; then
            warn "Failed to download $description from $url"
            return 1
        fi
    fi

    success "Downloaded $description"
    return 0
}

# =============================================================================
# Download JAR
# =============================================================================

download_jar() {
    info "Downloading $JAR_NAME..."
    info "URL: $DOWNLOAD_URL"

    local dest="$TARGET_DIR/lib/$JAR_NAME"

    if [[ "$DOWNLOADER" == "curl" ]]; then
        if ! curl -fSL --progress-bar -o "$dest" "$DOWNLOAD_URL"; then
            error "Failed to download JAR from $DOWNLOAD_URL"
            error "Please check that version '$SOFT_VERSION' exists."
            error "Run with --list-versions to see available versions."
            exit 1
        fi
    else
        if ! wget -q --show-progress -O "$dest" "$DOWNLOAD_URL"; then
            error "Failed to download JAR from $DOWNLOAD_URL"
            error "Please check that version '$SOFT_VERSION' exists."
            error "Run with --list-versions to see available versions."
            exit 1
        fi
    fi

    success "Downloaded to $dest"
}

# =============================================================================
# Extract the licence bundle (bundle installs only)
# The -all jar mixes Apache-2.0 + ELv2 + proprietary: materialize licenses/
# and NOTICE into the install root so the visible tree is not just the
# Apache-2.0 LICENSE. Failure-tolerant (set -e): the jar keeps the canonical
# copies either way.
# =============================================================================

extract_bundle_licenses() {
    [[ "$USE_BUNDLE" == true ]] || return 0

    local jar="$TARGET_DIR/lib/$JAR_NAME"
    info "Extracting licence bundle (licenses/ + NOTICE) from $JAR_NAME..."

    if command -v unzip >/dev/null 2>&1; then
        if unzip -o -q "$jar" 'licenses/*' NOTICE -d "$TARGET_DIR" 2>/dev/null; then
            success "Extracted licenses/ and NOTICE to $TARGET_DIR"
            return 0
        fi
    elif command -v jar >/dev/null 2>&1; then
        if (cd "$TARGET_DIR" && jar -xf "$jar" licenses NOTICE) 2>/dev/null; then
            success "Extracted licenses/ and NOTICE to $TARGET_DIR"
            return 0
        fi
    fi

    warn "Could not extract the licence bundle (unzip/jar unavailable or failed)."
    warn "The canonical copies remain inside the jar: licenses/ and NOTICE."
}

# =============================================================================
# Install Extensions (cross-index JOINs, materialized views)
# =============================================================================

# The engine assembly is launched via classpath (bin/softclient4es uses -cp
# "lib/*"), so extension jars dropped into lib/ are discovered through the
# ServiceLoader SPI. The extensions are thin jars: their full dependency
# closure (Apache Arrow, DuckDB, ...) must be resolved too — a couple of jars
# is NOT enough. We bootstrap the coursier launcher (a single portable jar
# that runs with the already-required java) to resolve everything.

COURSIER_URL="https://github.com/coursier/launchers/raw/master/coursier"
JFROG_ROOT_URL="https://softnetwork.jfrog.io/artifactory/releases"
EXTENSIONS_INSTALLED="none"

# version_ge A B — true if version A >= version B
version_ge() {
    [[ "$(printf '%s\n%s\n' "$2" "$1" | sort_versions | head -1)" == "$2" ]]
}

# Resolve one extension (with its full dependency closure) into lib/.
# $1 = artifact base name (without scala suffix), $2 = version
install_one_extension() {
    local ext="$1"
    local ver="$2"
    local artifact="${ext}_${SCALA_VERSION}"
    local cs="$TARGET_DIR/bin/.coursier"
    local jars jar base_jar count

    info "Resolving ${artifact}:${ver} with all dependencies..."
    if ! jars=$("$cs" fetch --repository "$JFROG_ROOT_URL" "app.softnetwork.elastic:${artifact}:${ver}" 2>/dev/null); then
        warn "Dependency resolution failed for $artifact — skipping"
        return 1
    fi

    count=0
    while IFS= read -r jar; do
        [[ -f "$jar" ]] || continue
        base_jar=$(basename "$jar")
        if [[ ! -f "$TARGET_DIR/lib/$base_jar" ]]; then
            cp "$jar" "$TARGET_DIR/lib/$base_jar"
            (( ++count )) || true
        fi
    done <<< "$jars"

    success "Installed ${artifact}:${ver} ($count jar(s) added to lib/)"
    if [[ "$EXTENSIONS_INSTALLED" == "none" ]]; then
        EXTENSIONS_INSTALLED="${ext}:${ver}"
    else
        EXTENSIONS_INSTALLED="$EXTENSIONS_INSTALLED ${ext}:${ver}"
    fi
    return 0
}

install_extensions() {
    if [[ "$USE_BUNDLE" == true ]]; then
        info "Extensions are bundled inside $JAR_NAME — no dependency resolution needed"
        EXTENSIONS_INSTALLED="bundled (community + arrow JOIN)"
        extract_bundle_licenses
        return 0
    fi

    if [[ "$WITH_EXTENSIONS" != true ]]; then
        info "Skipping extensions (--no-extensions)"
        return 0
    fi

    info "Installing extensions (materialized views + cross-index JOINs)..."

    local cs="$TARGET_DIR/bin/.coursier"
    if [[ ! -x "$cs" ]]; then
        if ! download_file "$COURSIER_URL" "$cs" "coursier resolver"; then
            warn "Could not download the coursier resolver — extensions skipped."
            warn "Manual install: https://github.com/SOFTNETWORK-APP/SoftClient4ES/blob/main/documentation/client/repl.md"
            return 0
        fi
        chmod +x "$cs"
    fi

    local java_version
    java_version=$(get_java_version)

    # ── Community extensions (materialized views) — ALWAYS installed by default.
    #    No Arrow dependency, runs on Java 8+. For engines < 0.20 the matching
    #    0.1.x line is selected; for >= 0.20 the latest release.
    local community_ver
    if version_ge "$SOFT_VERSION" "0.20"; then
        community_ver=$(fetch_versions "softclient4es-community-extensions_${SCALA_VERSION}" | grep -v 'SNAPSHOT' | tail -1)
    else
        community_ver=$(fetch_versions "softclient4es-community-extensions_${SCALA_VERSION}" | grep -v 'SNAPSHOT' | grep '^0\.1\.' | tail -1)
    fi
    if [[ -n "$community_ver" ]]; then
        install_one_extension "softclient4es-community-extensions" "$community_ver" || true
    else
        warn "Could not resolve a community-extensions release for engine $SOFT_VERSION — skipping"
    fi

    # ── Arrow extensions (cross-index JOINs) — default for engines >= 0.20.
    #    Hard runtime constraint: Apache Arrow 18.x ships Java-11 bytecode
    #    (class file major 55) — on Java 8 the JOIN engine cannot load.
    if ! version_ge "$SOFT_VERSION" "0.20"; then
        info "Cross-index JOIN extension requires SoftClient4ES >= 0.20 (installing $SOFT_VERSION) — skipping"
        return 0
    fi
    if [[ -n "$java_version" ]] && [[ "$java_version" -lt 11 ]]; then
        warn "Cross-index JOINs require Java 11+ (Apache Arrow constraint) — found Java $java_version."
        warn "Materialized views remain available; re-run the installer on Java 11+ to enable JOINs."
        return 0
    fi

    local arrow_ver
    arrow_ver=$(fetch_versions "softclient4es-arrow-extensions_${SCALA_VERSION}" | grep -v 'SNAPSHOT' | tail -1)
    if [[ -n "$arrow_ver" ]]; then
        install_one_extension "softclient4es-arrow-extensions" "$arrow_ver" || true
    else
        warn "Could not resolve an arrow-extensions release — skipping"
    fi
}

# =============================================================================
# License Notice (AC 4b) — wording rules: the bundle is "free to use", NEVER
# "open source" / "source-available" / "Apache-2.0" as a whole.
# =============================================================================

print_license_notice() {
    if [[ "$USE_BUNDLE" == true ]]; then
        echo "  License: this bundle contains the Apache-2.0 SoftClient4ES engine PLUS"
        echo "  SoftClient4ES extensions under the Elastic License 2.0 and the proprietary"
        echo "  cross-index JOIN engine (free to use; see the licenses/ directory and NOTICE"
        echo "  in the install root — canonical copies ship inside the jar)."
        echo "  Quota enforcement is active. For a pure Apache-2.0 install re-run with --no-extensions."
    elif [[ "$WITH_EXTENSIONS" != true ]] || [[ "$EXTENSIONS_INSTALLED" == "none" ]]; then
        echo "  License: pure Apache-2.0 engine (no extensions)."
    elif [[ "$EXTENSIONS_INSTALLED" == *arrow-extensions* ]]; then
        echo "  License: this installation contains the Apache-2.0 SoftClient4ES engine PLUS"
        echo "  SoftClient4ES extensions under the Elastic License 2.0 and the proprietary"
        echo "  cross-index JOIN engine (free to use). Quota enforcement is active."
        echo "  For a pure Apache-2.0 install re-run with --no-extensions."
    else
        echo "  License: this installation contains the Apache-2.0 SoftClient4ES engine PLUS"
        echo "  SoftClient4ES extensions under the Elastic License 2.0 (free to use)."
        echo "  Quota enforcement is active. For a pure Apache-2.0 install re-run with --no-extensions."
    fi
}

# =============================================================================
# Download Documentation and License
# =============================================================================

download_docs() {
    info "Downloading documentation and license..."

    # Download README.md
    if download_file "$README_URL" "$TARGET_DIR/README.md" "README.md"; then
        : # success already printed
    else
        warn "README.md download failed, creating minimal version"
        create_minimal_readme
    fi

    # Download LICENSE
    if download_file "$LICENSE_URL" "$TARGET_DIR/LICENSE" "LICENSE"; then
        : # success already printed
    else
        warn "LICENSE download failed, skipping"
    fi
}

# =============================================================================
# Create Minimal README (Fallback)
# =============================================================================

create_minimal_readme() {
    cat > "$TARGET_DIR/README.md" << 'EOF'
# SoftClient4ES

SQL Gateway for Elasticsearch

## Quick Start

```bash
# Start the REPL
./bin/softclient4es

# Execute a single command
./bin/softclient4es -c "SHOW TABLES"

# Get help
./bin/softclient4es --help
```

## Configuration

Edit `conf/application.conf` to configure default connection settings.

## Documentation

Full documentation available at:
https://github.com/SOFTNETWORK-APP/SoftClient4ES

## License

See LICENSE file for details.
EOF

    success "Created minimal README.md"
}

# =============================================================================
# Create Configuration File
# =============================================================================

create_config() {
    info "Creating configuration file..."

    cat > "$TARGET_DIR/conf/application.conf" << 'EOF'
# SoftClient4ES Configuration
# Override these settings or use command-line options
# Precedence: CLI flag > ELASTIC_* env var > this file > built-in defaults

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
EOF

    success "Created $TARGET_DIR/conf/application.conf"
}

# =============================================================================
# Create Launcher Script
# =============================================================================

create_launcher() {
    info "Creating launcher script..."

    cat > "$TARGET_DIR/bin/softclient4es" << LAUNCHER_EOF
#!/usr/bin/env bash
#
# SoftClient4ES Launcher
# Elasticsearch version: $ES_VERSION
# Required Java: $REQUIRED_JAVA_VERSION+
#

SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="\$(dirname "\$SCRIPT_DIR")"

JAR_FILE="\$BASE_DIR/lib/$JAR_NAME"
CONFIG_FILE="\$BASE_DIR/conf/application.conf"
LOGBACK_FILE="\$BASE_DIR/conf/logback.xml"
LOG_DIR="\$BASE_DIR/logs"

REQUIRED_JAVA=$REQUIRED_JAVA_VERSION

if [[ ! -f "\$JAR_FILE" ]]; then
    echo "Error: JAR file not found: \$JAR_FILE" >&2
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p "\$LOG_DIR"

# Check Java version (portable - works on Linux and macOS)
check_java() {
    if ! command -v java &> /dev/null; then
        echo "Error: Java is not installed. Java \$REQUIRED_JAVA+ is required." >&2
        exit 1
    fi

    local java_version_output
    java_version_output=\$(java -version 2>&1 | head -n 1)

    # Extract version string between quotes
    local version_string
    version_string=\$(echo "\$java_version_output" | sed 's/.*"\(.*\)".*/\1/')

    local java_version=""

    if [[ "\$version_string" == 1.* ]]; then
        # Old format: 1.8.0_xxx -> extract 8
        java_version=\$(echo "\$version_string" | cut -d'.' -f2)
    else
        # New format: 11.0.11, 17.0.1, 21 -> extract first number
        java_version=\$(echo "\$version_string" | cut -d'.' -f1)
    fi

    if [[ -n "\$java_version" ]] && [[ "\$java_version" -lt "\$REQUIRED_JAVA" ]]; then
        echo "Error: Java \$REQUIRED_JAVA+ is required. Found: Java \$java_version" >&2
        exit 1
    fi

    JAVA_MAJOR="\$java_version"
}

check_java

# HOCON \${?VAR} substitution treats an env var set to "" as PRESENT and lets it
# override file/default values (empty host => http://:9200). Treat empty as unset.
# The ELASTIC_WATCHER_* family is included because the builtin conf carries the
# same \${?VAR} substitution lines for the watcher block.
for var in ELASTIC_SCHEME ELASTIC_HOST ELASTIC_IP ELASTIC_PORT \\
           ELASTIC_USERNAME ELASTIC_PASSWORD ELASTIC_API_KEY ELASTIC_BEARER_TOKEN \\
           ELASTIC_AUTH_METHOD \\
           ELASTIC_CREDENTIALS_USERNAME ELASTIC_CREDENTIALS_PASSWORD \\
           ELASTIC_CREDENTIALS_API_KEY ELASTIC_CREDENTIALS_BEARER_TOKEN \\
           ELASTIC_WATCHER_SCHEME ELASTIC_WATCHER_HOST ELASTIC_WATCHER_PORT \\
           ELASTIC_WATCHER_AUTH_METHOD ELASTIC_WATCHER_USERNAME ELASTIC_WATCHER_PASSWORD \\
           ELASTIC_WATCHER_API_KEY ELASTIC_WATCHER_BEARER_TOKEN; do
    if [[ -z "\${!var:-}" ]]; then
        unset "\$var"
    fi
done

# Default JVM options
JAVA_OPTS="\${JAVA_OPTS:--Xmx512m}"

# The extensions (Apache Arrow / DuckDB) need reflective access on Java 9+
EXTRA_OPTS=""
if [[ -n "\$JAVA_MAJOR" ]] && [[ "\$JAVA_MAJOR" -ge 9 ]]; then
    EXTRA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
fi

# Class-Data Sharing (AppCDS) — cuts JVM class-load time on every start (#163 fix 3).
# BUNDLE-ONLY: CDS_BUNDLE below is baked in AT INSTALL TIME from REPL.4's USE_BUNDLE
# (intentionally NOT \$-escaped) — plain/--no-extensions installs get NO CDS behaviour
# at all (no flags, no cache/ dir), on every JDK. Without this gate the 19+ branch
# would fire on plain installs too and attempt runtime dumps over the multi-jar lib/*.
# - JDK 19+: the JVM creates/refreshes the archive itself (AutoCreateSharedArchive).
# - JDK 13-18: use the archive generated at install time, if present and not stale.
# - JDK 11/12 or no archive: no CDS flags — default behaviour, zero regression.
# -Xshare:auto (the default) silently ignores an invalid/mismatched archive.
CDS_BUNDLE=${USE_BUNDLE:-false}
CDS_ARCHIVE="\$BASE_DIR/cache/softclient4es.jsa"
CDS_OPTS=""
if [[ "\$CDS_BUNDLE" == true ]] && [[ -n "\$JAVA_MAJOR" ]]; then
    if [[ "\$JAVA_MAJOR" -ge 19 ]]; then
        mkdir -p "\$BASE_DIR/cache"
        CDS_OPTS="-XX:+AutoCreateSharedArchive -XX:SharedArchiveFile=\$CDS_ARCHIVE"
    elif [[ "\$JAVA_MAJOR" -ge 13 ]] && [[ -f "\$CDS_ARCHIVE" ]] && [[ ! "\$JAR_FILE" -nt "\$CDS_ARCHIVE" ]]; then
        CDS_OPTS="-XX:SharedArchiveFile=\$CDS_ARCHIVE"
    fi
fi

# Logback configuration
LOGBACK_OPTS=""
if [[ -f "\$LOGBACK_FILE" ]]; then
    LOGBACK_OPTS="-Dlogback.configurationFile=\$LOGBACK_FILE"
fi

# The engine assembly comes first on the classpath so it wins any conflict;
# lib/* brings the extension jars, discovered via the ServiceLoader SPI.
# (java -jar would ignore the classpath entirely — do not switch back.)
exec java \$JAVA_OPTS \$EXTRA_OPTS \$CDS_OPTS \\
    -Dconfig.file="\$CONFIG_FILE" \\
    -Dlog.dir="\$LOG_DIR" \\
    \$LOGBACK_OPTS \\
    -cp "\$JAR_FILE:\$BASE_DIR/lib/*" \\
    app.softnetwork.elastic.client.Cli \\
    "\$@"
LAUNCHER_EOF

    chmod +x "$TARGET_DIR/bin/softclient4es"

    success "Created $TARGET_DIR/bin/softclient4es"
}

# =============================================================================
# Generate AppCDS archive (REPL.5 / #163 fix 3) — bundle installs, JDK 13-18.
# JDK 19+ needs nothing here (the launcher passes -XX:+AutoCreateSharedArchive).
# =============================================================================

CDS_STATUS="disabled"

generate_cds_archive() {
    [[ "$USE_BUNDLE" == true ]] || return 0        # single-jar classpath only (CDS forbids lib/*)
    local jv
    jv=$(get_java_version)
    [[ -n "$jv" ]] || return 0
    if [[ "$jv" -ge 19 ]]; then
        # The launcher's -XX:+AutoCreateSharedArchive branch dumps/refreshes at runtime.
        CDS_STATUS="enabled (runtime auto-create, JDK 19+)"
        return 0
    fi
    [[ "$jv" -ge 13 ]] || return 0                 # 11/12: no dynamic archive support

    info "Generating AppCDS archive (one-off; speeds up every REPL/batch start)..."
    # The installer runs under `set -e`: every may-fail step below is guarded so a
    # CDS failure can NEVER fail the install (warn-and-continue only).
    if ! mkdir -p "$TARGET_DIR/cache" 2>/dev/null; then
        warn "AppCDS archive generation skipped — could not create cache/ (no runtime impact)"
        CDS_STATUS="disabled (generation failed)"
        return 0
    fi
    # Re-install into an existing dir: never dump over a stale archive.
    rm -f "$TARGET_DIR/cache/softclient4es.jsa" 2>/dev/null || true
    # Dump workload: a short-lived batch run. It needs NO Elasticsearch — whatever the
    # statement resolves to (local help or a connection error printed by the executor),
    # the process exits normally with code 0 (Repl.executeCommand always returns 0) and
    # the dynamic archive is written at exit. -cp is the bare jar: wildcards are
    # forbidden at dump time, and [jar] is a prefix of the runtime [jar, lib/*] path.
    # The exit code alone is not proof the archive was written (an unwritable path
    # still exits 0) — require the .jsa file to actually exist.
    if java -XX:ArchiveClassesAtExit="$TARGET_DIR/cache/softclient4es.jsa" \
            -cp "$TARGET_DIR/lib/$JAR_NAME" \
            app.softnetwork.elastic.client.Cli -c "help" > /dev/null 2>&1 \
            && [[ -f "$TARGET_DIR/cache/softclient4es.jsa" ]]; then
        success "AppCDS archive created: cache/softclient4es.jsa"
        CDS_STATUS="enabled (cache/softclient4es.jsa)"
    else
        warn "AppCDS archive generation failed — continuing without it (no runtime impact)"
        rm -f "$TARGET_DIR/cache/softclient4es.jsa" 2>/dev/null || true
        CDS_STATUS="disabled (generation failed)"
    fi
}

# =============================================================================
# Create Logback Configuration
# =============================================================================

create_logback_config() {
    info "Creating logback configuration..."

    cat > "$TARGET_DIR/conf/logback.xml" << 'EOF'
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
EOF

    success "Created $TARGET_DIR/conf/logback.xml"
}

# =============================================================================
# Create Uninstall Script
# =============================================================================

create_uninstaller() {
    info "Creating uninstall script..."

    cat > "$TARGET_DIR/uninstall.sh" << UNINSTALL_EOF
#!/usr/bin/env bash
#
# SoftClient4ES Uninstaller
#

TARGET_DIR="$TARGET_DIR"

echo "This will remove SoftClient4ES from: \$TARGET_DIR"
read -p "Continue? [y/N] " -n 1 -r
echo
if [[ \$REPLY =~ ^[Yy]$ ]]; then
    rm -rf "\$TARGET_DIR"
    echo "SoftClient4ES has been uninstalled."
    echo ""
    echo "Don't forget to remove the PATH entry from your shell config if you added one."
else
    echo "Uninstall cancelled."
fi
UNINSTALL_EOF

    chmod +x "$TARGET_DIR/uninstall.sh"

    success "Created $TARGET_DIR/uninstall.sh"
}

# =============================================================================
# Create Version Info File
# =============================================================================

create_version_info() {
    if [[ "$USE_BUNDLE" == true ]]; then
        # Bundle path: $SOFT_VERSION is the BUNDLE version (its own line);
        # the exact engine/extension versions are disclosed by the jar
        # MANIFEST and the REPL banner / 'version' command.
        cat > "$TARGET_DIR/VERSION" << EOF
SoftClient4ES Installation Info
================================
Installed:          $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Elasticsearch:      $ES_VERSION
Version:            $SOFT_VERSION (bundle version line)
Note:               engine and extension versions are disclosed by the jar
                    MANIFEST and the REPL banner ('version' command)
Scala:              $SCALA_VERSION
Java Required:      $REQUIRED_JAVA_VERSION+
Artifact:           $ARTIFACT_NAME
Extensions:         $EXTENSIONS_INSTALLED
OS:                 $OS_TYPE
$(print_license_notice)
EOF
    else
        cat > "$TARGET_DIR/VERSION" << EOF
SoftClient4ES Installation Info
================================
Installed:          $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Elasticsearch:      $ES_VERSION
Version:            $SOFT_VERSION
Scala:              $SCALA_VERSION
Java Required:      $REQUIRED_JAVA_VERSION+
Artifact:           $ARTIFACT_NAME
Extensions:         $EXTENSIONS_INSTALLED
AppCDS:             $CDS_STATUS
OS:                 $OS_TYPE
$(print_license_notice)
EOF
    fi

    success "Created $TARGET_DIR/VERSION"
}

# =============================================================================
# Print Summary
# =============================================================================

print_summary() {
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  SoftClient4ES Installation Complete!${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  Installation directory: $TARGET_DIR"
    echo "  Elasticsearch version:  $ES_VERSION"
    echo "  SoftClient4ES version:  $SOFT_VERSION"
    echo "  Java required:          $REQUIRED_JAVA_VERSION+"
    echo "  OS detected:            $OS_TYPE"
    if [[ "$CDS_STATUS" == enabled* ]]; then
        echo "  AppCDS:                 $CDS_STATUS"
        if [[ "$CDS_STATUS" == *cache/* ]]; then
            echo "                          (regenerate by re-running the installer after a Java upgrade)"
        fi
    fi
    echo ""
    echo "  Directory structure:"
    echo "    $TARGET_DIR/"
    echo "    ├── bin/"
    echo "    │   └── softclient4es"
    echo "    ├── conf/"
    echo "    │   ├── application.conf"
    echo "    │   └── logback.xml"
    echo "    ├── lib/"
    echo "    │   ├── $JAR_NAME"
    if [[ "$USE_BUNDLE" == true ]]; then
        echo "    │   └── (self-contained -all bundle — no additional jars)"
    elif [[ "$EXTENSIONS_INSTALLED" != "none" ]]; then
        echo "    │   └── (+ extension jars: $EXTENSIONS_INSTALLED)"
    else
        echo "    │   └── (no extensions installed)"
    fi
    echo "    ├── logs/"
    echo "    │   └── softclient4es.log  (created at runtime)"
    if [[ "$USE_BUNDLE" == true ]]; then
        echo "    ├── licenses/"
        echo "    │   ├── LICENSE-Apache-2.0.txt"
        echo "    │   ├── LICENSE-Elastic-2.0.txt"
        echo "    │   └── NOTICE-arrow-extensions.txt"
        echo "    ├── LICENSE"
        echo "    ├── NOTICE"
    else
        echo "    ├── LICENSE"
    fi
    echo "    ├── README.md"
    echo "    ├── VERSION"
    echo "    └── uninstall.sh"
    echo ""
    print_license_notice
    echo ""
    echo -e "  ${CYAN}Quick Start:${NC}"
    echo ""
    echo "    # Start the REPL (interactive mode)"
    echo -e "    ${BLUE}$TARGET_DIR/bin/softclient4es${NC}"
    echo ""
    echo "    # Or add to your PATH first:"
    case "$OS_TYPE" in
        macos)
            echo -e "    ${BLUE}echo 'export PATH=\"\$PATH:$TARGET_DIR/bin\"' >> ~/.zshrc${NC}"
            echo -e "    ${BLUE}source ~/.zshrc${NC}"
            ;;
        linux)
            echo -e "    ${BLUE}echo 'export PATH=\"\$PATH:$TARGET_DIR/bin\"' >> ~/.bashrc${NC}"
            echo -e "    ${BLUE}source ~/.bashrc${NC}"
            ;;
        *)
            echo -e "    ${BLUE}export PATH=\"\$PATH:$TARGET_DIR/bin\"${NC}"
            ;;
    esac
    echo ""
    echo "    # Then simply run:"
    echo -e "    ${BLUE}softclient4es${NC}"
    echo ""
    echo -e "  ${CYAN}Connection Examples:${NC}"
    echo ""
    echo "    # Connect to local Elasticsearch"
    echo -e "    ${BLUE}softclient4es${NC}"
    echo ""
    echo "    # Connect to remote Elasticsearch"
    echo -e "    ${BLUE}softclient4es --host es.example.com --port 9200${NC}"
    echo ""
    echo "    # Connect with authentication"
    echo -e "    ${BLUE}softclient4es --host es.example.com --username admin --password secret${NC}"
    echo ""
    echo "    # Connect with SSL"
    echo -e "    ${BLUE}softclient4es --host es.example.com --scheme https${NC}"
    echo ""
    echo "    # Execute a single command"
    echo -e "    ${BLUE}softclient4es -c \"SHOW TABLES\"${NC}"
    echo ""
    echo -e "  ${CYAN}Configuration:${NC}"
    echo "    Edit $TARGET_DIR/conf/application.conf"
    echo "    Or use environment variables (ELASTIC_HOST, ELASTIC_PORT, etc.)"
    echo ""
    echo -e "  ${CYAN}Documentation:${NC}"
    echo -e "    ${BLUE}cat $TARGET_DIR/README.md${NC}"
    echo "    https://github.com/SOFTNETWORK-APP/SoftClient4ES"
    echo ""
    echo -e "  ${CYAN}To uninstall:${NC}"
    echo -e "    ${BLUE}$TARGET_DIR/uninstall.sh${NC}"
    echo ""
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  SoftClient4ES Installer${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""

    check_prerequisites
    echo ""
    create_directories
    download_jar
    install_extensions
    # AC 4b: state the licence terms right after the download (repeated in the summary)
    print_license_notice >&2
    download_docs
    create_config
    create_logback_config
    create_launcher
    generate_cds_archive
    create_uninstaller
    create_version_info
    print_summary
}

main
