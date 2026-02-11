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

info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1" >&2; }

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
  -v, --version <ver>      SoftClient4ES version (default: latest)
  -s, --scala <ver>        Scala version (default: $DEFAULT_SCALA_VERSION)
  -l, --list-versions      List available versions for the specified ES version
  -h, --help               Show this help message

Java Requirements:
  ES 6, 7, 8  →  Java 8 or higher
  ES 9        →  Java 17 or higher

Examples:
  $0
  $0 --list-versions --es-version 8
  $0 --target /opt/softclient4es --es-version 8 --version 0.16-SNAPSHOT
  $0 -t ~/tools/softclient4es -e 7 -v 0.2.0

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

ARTIFACT_NAME="softclient4es${ES_VERSION}-cli_${SCALA_VERSION}"

# =============================================================================
# Get Required Java Version
# =============================================================================

get_required_java_version() {
    local es_ver="$1"
    if [[ "$es_ver" == "9" ]]; then
        echo 17
    else
        echo 8
    fi
}

REQUIRED_JAVA_VERSION=$(get_required_java_version "$ES_VERSION")

# =============================================================================
# List Available Versions
# =============================================================================

list_available_versions() {
    info "Fetching available versions for ES$ES_VERSION..."

    local api_url="${JFROG_API_URL}/${ARTIFACT_NAME}"

    if command -v curl &> /dev/null; then
        local response=$(curl -fsSL "$api_url" 2>/dev/null)
    elif command -v wget &> /dev/null; then
        local response=$(wget -qO- "$api_url" 2>/dev/null)
    else
        error "curl or wget is required"
        exit 1
    fi

    if [[ -z "$response" ]]; then
        error "Failed to fetch versions from repository"
        error "Artifact: $ARTIFACT_NAME"
        exit 1
    fi

    # Parse JSON response to extract version folders
    local versions=$(echo "$response" | grep -oP '"uri"\s*:\s*"/\K[^"]+' | grep -v '^\.' | sort -V)

    if [[ -z "$versions" ]]; then
        error "No versions found for $ARTIFACT_NAME"
        exit 1
    fi

    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Available SoftClient4ES Versions for Elasticsearch $ES_VERSION${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${YELLOW}Artifact:${NC} $ARTIFACT_NAME"
    echo -e "  ${YELLOW}Java required:${NC} $REQUIRED_JAVA_VERSION+"
    echo ""
    echo -e "  ${GREEN}Versions:${NC}"
    echo ""

    # Display versions
    local count=0
    while IFS= read -r version; do
        if [[ -n "$version" ]]; then
            echo "    • $version"
            ((count++))
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

    local api_url="${JFROG_API_URL}/${ARTIFACT_NAME}"

    if command -v curl &> /dev/null; then
        local response=$(curl -fsSL "$api_url" 2>/dev/null)
    elif command -v wget &> /dev/null; then
        local response=$(wget -qO- "$api_url" 2>/dev/null)
    else
        error "curl or wget is required"
        exit 1
    fi

    if [[ -z "$response" ]]; then
        error "Failed to fetch versions from repository"
        exit 1
    fi

    # Parse and get latest non-snapshot version, fallback to any latest
    local versions=$(echo "$response" | grep -oP '"uri"\s*:\s*"/\K[^"]+' | grep -v '^\.' | sort -V)

    # Prefer non-snapshot versions
    local latest=$(echo "$versions" | grep -v 'SNAPSHOT' | tail -1)

    # Fallback to any version if no release found
    if [[ -z "$latest" ]]; then
        latest=$(echo "$versions" | tail -1)
    fi

    if [[ -z "$latest" ]]; then
        error "Could not determine latest version"
        exit 1
    fi

    echo "$latest"
}

if [[ "$SOFT_VERSION" == "latest" ]]; then
    SOFT_VERSION=$(resolve_latest_version)
    success "Resolved latest version: $SOFT_VERSION"
fi

JAR_NAME="${ARTIFACT_NAME}-${SOFT_VERSION}-assembly.jar"
DOWNLOAD_URL="${JFROG_REPO_URL}/${ARTIFACT_NAME}/${SOFT_VERSION}/${JAR_NAME}"

# =============================================================================
# Check Prerequisites
# =============================================================================

check_prerequisites() {
    info "Checking prerequisites..."

    # Check Java
    if ! command -v java &> /dev/null; then
        error "Java is not installed."
        error "ES$ES_VERSION requires Java $REQUIRED_JAVA_VERSION or higher."
        exit 1
    fi

    # Get Java version
    local java_version_output=$(java -version 2>&1 | head -n 1)
    local java_version

    # Handle both old (1.8.x) and new (11.x, 17.x) version formats
    if echo "$java_version_output" | grep -q '"1\.[0-9]'; then
        # Old format: "1.8.0_xxx"
        java_version=$(echo "$java_version_output" | grep -oP '1\.(\d+)' | cut -d'.' -f2)
    else
        # New format: "11.x.x", "17.x.x", etc.
        java_version=$(echo "$java_version_output" | grep -oP '"(\d+)' | tr -d '"')
    fi

    if [[ -z "$java_version" ]]; then
        warn "Could not determine Java version"
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

    success "Created $TARGET_DIR/{bin,conf,lib}"
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

    cat > "$TARGET_DIR/bin/softclient4es" << EOF
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

if [[ ! -f "\$JAR_FILE" ]]; then
    echo "Error: JAR file not found: \$JAR_FILE" >&2
    exit 1
fi

# Check Java version
check_java() {
    if ! command -v java &> /dev/null; then
        echo "Error: Java is not installed. Java $REQUIRED_JAVA_VERSION+ is required." >&2
        exit 1
    fi

    local java_version_output=\$(java -version 2>&1 | head -n 1)
    local java_version

    if echo "\$java_version_output" | grep -q '"1\.[0-9]'; then
        java_version=\$(echo "\$java_version_output" | grep -oP '1\.(\d+)' | cut -d'.' -f2)
    else
        java_version=\$(echo "\$java_version_output" | grep -oP '"\d+' | tr -d '"')
    fi

    if [[ -n "\$java_version" ]] && [[ "\$java_version" -lt $REQUIRED_JAVA_VERSION ]]; then
        echo "Error: Java $REQUIRED_JAVA_VERSION+ is required. Found: Java \$java_version" >&2
        exit 1
    fi
}

check_java

# Default JVM options
JAVA_OPTS="\${JAVA_OPTS:--Xmx512m}"

exec java \$JAVA_OPTS \\
    -Dconfig.file="\$CONFIG_FILE" \\
    -jar "\$JAR_FILE" \\
    "\$@"
EOF

    chmod +x "$TARGET_DIR/bin/softclient4es"

    success "Created $TARGET_DIR/bin/softclient4es"
}

# =============================================================================
# Create Uninstall Script
# =============================================================================

create_uninstaller() {
    info "Creating uninstall script..."

    cat > "$TARGET_DIR/uninstall.sh" << EOF
#!/usr/bin/env bash
#
# SoftClient4ES Uninstaller
#

TARGET_DIR="$TARGET_DIR"

read -p "This will remove \$TARGET_DIR. Continue? [y/N] " -n 1 -r
echo
if [[ \$REPLY =~ ^[Yy]$ ]]; then
    rm -rf "\$TARGET_DIR"
    echo "SoftClient4ES has been uninstalled."
else
    echo "Uninstall cancelled."
fi
EOF

    chmod +x "$TARGET_DIR/uninstall.sh"

    success "Created $TARGET_DIR/uninstall.sh"
}

# =============================================================================
# Create Version Info File
# =============================================================================

create_version_info() {
    cat > "$TARGET_DIR/VERSION" << EOF
SoftClient4ES Installation Info
================================
Installed:          $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Elasticsearch:      $ES_VERSION
Version:            $SOFT_VERSION
Scala:              $SCALA_VERSION
Java Required:      $REQUIRED_JAVA_VERSION+
Artifact:           $ARTIFACT_NAME
EOF

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
    echo ""
    echo "  Directory structure:"
    echo "    $TARGET_DIR/"
    echo "    ├── bin/"
    echo "    │   └── softclient4es"
    echo "    ├── conf/"
    echo "    │   └── application.conf"
    echo "    ├── lib/"
    echo "    │   └── $JAR_NAME"
    echo "    ├── LICENSE"
    echo "    ├── README.md"
    echo "    ├── VERSION"
    echo "    └── uninstall.sh"
    echo ""
    echo "  To start the REPL:"
    echo -e "    ${BLUE}$TARGET_DIR/bin/softclient4es${NC}"
    echo ""
    echo "  Or add to your PATH:"
    echo -e "    ${BLUE}export PATH=\"\$PATH:$TARGET_DIR/bin\"${NC}"
    echo ""
    echo "  Documentation:"
    echo -e "    ${BLUE}cat $TARGET_DIR/README.md${NC}"
    echo ""
    echo "  Configuration:"
    echo "    Edit $TARGET_DIR/conf/application.conf"
    echo "    Or use command-line options (run with --help)"
    echo ""
    echo "  To uninstall:"
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
    create_directories
    download_jar
    download_docs
    create_config
    create_launcher
    create_uninstaller
    create_version_info
    print_summary
}

main
