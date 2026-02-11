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
DEFAULT_SOFT_VERSION="0.16-SNAPSHOT"
DEFAULT_SCALA_VERSION="2.13"

JFROG_REPO_URL="https://softnetwork.jfrog.io/artifactory/releases/app/softnetwork/elastic"

# =============================================================================
# Colors and Output
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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
  -v, --version <ver>      SoftClient4ES version (default: $DEFAULT_SOFT_VERSION)
  -s, --scala <ver>        Scala version (default: $DEFAULT_SCALA_VERSION)
  -h, --help               Show this help message

Examples:
  $0
  $0 --target /opt/softclient4es --es-version 8 --version 1.0.0
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
JAR_NAME="${ARTIFACT_NAME}-${SOFT_VERSION}-assembly.jar"
DOWNLOAD_URL="${JFROG_REPO_URL}/${ARTIFACT_NAME}/${SOFT_VERSION}/${JAR_NAME}"

# =============================================================================
# Check Prerequisites
# =============================================================================

check_prerequisites() {
    info "Checking prerequisites..."

    # Check Java
    if ! command -v java &> /dev/null; then
        error "Java is not installed. Please install Java 11 or higher."
        exit 1
    fi

    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [[ "$JAVA_VERSION" -lt 11 ]]; then
        error "Java 11 or higher is required. Found: Java $JAVA_VERSION"
        exit 1
    fi
    success "Java $JAVA_VERSION found"

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
# Download JAR
# =============================================================================

download_jar() {
    info "Downloading $JAR_NAME..."
    info "URL: $DOWNLOAD_URL"

    local dest="$TARGET_DIR/lib/$JAR_NAME"

    if [[ "$DOWNLOADER" == "curl" ]]; then
        if ! curl -fSL --progress-bar -o "$dest" "$DOWNLOAD_URL"; then
            error "Failed to download JAR from $DOWNLOAD_URL"
            exit 1
        fi
    else
        if ! wget -q --show-progress -O "$dest" "$DOWNLOAD_URL"; then
            error "Failed to download JAR from $DOWNLOAD_URL"
            exit 1
        fi
    fi

    success "Downloaded to $dest"
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
#

SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="\$(dirname "\$SCRIPT_DIR")"

JAR_FILE="\$BASE_DIR/lib/$JAR_NAME"
CONFIG_FILE="\$BASE_DIR/conf/application.conf"

if [[ ! -f "\$JAR_FILE" ]]; then
    echo "Error: JAR file not found: \$JAR_FILE" >&2
    exit 1
fi

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
    echo ""
    echo "  Directory structure:"
    echo "    $TARGET_DIR/"
    echo "    ├── bin/"
    echo "    │   └── softclient4es"
    echo "    ├── conf/"
    echo "    │   └── application.conf"
    echo "    ├── lib/"
    echo "    │   └── $JAR_NAME"
    echo "    └── uninstall.sh"
    echo ""
    echo "  To start the REPL:"
    echo -e "    ${BLUE}$TARGET_DIR/bin/softclient4es${NC}"
    echo ""
    echo "  Or add to your PATH:"
    echo -e "    ${BLUE}export PATH=\"\$PATH:$TARGET_DIR/bin\"${NC}"
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
    create_config
    create_launcher
    create_uninstaller
    print_summary
}

main
