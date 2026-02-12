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
# Filters out hidden files (starting with .) and maven-metadata.xml
extract_uris_from_json() {
    # Use sed which is portable across Linux and macOS
    # Pattern: "uri" : "/version" -> extract version
    sed -n 's/.*"uri"[[:space:]]*:[[:space:]]*"\/\([^"]*\)".*/\1/p' \
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
# Fetch Versions from Repository
# =============================================================================

fetch_versions() {
    local api_url="${JFROG_API_URL}/${ARTIFACT_NAME}"
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
        error "Artifact: $ARTIFACT_NAME"
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

    local versions
    versions=$(fetch_versions)

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

    local versions
    versions=$(fetch_versions)

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

if [[ "$SOFT_VERSION" == "latest" ]]; then
    SOFT_VERSION=$(resolve_latest_version)
    if [[ -z "$SOFT_VERSION" ]]; then
        error "Failed to resolve latest version"
        error "Try specifying a version manually with --version"
        error "Or run with --list-versions to see available versions"
        exit 1
    fi
    success "Resolved latest version: $SOFT_VERSION"
fi

JAR_NAME="${ARTIFACT_NAME}-${SOFT_VERSION}-assembly.jar"
DOWNLOAD_URL="${JFROG_REPO_URL}/${ARTIFACT_NAME}/${SOFT_VERSION}/${JAR_NAME}"

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
}

check_java

# Default JVM options
JAVA_OPTS="\${JAVA_OPTS:--Xmx512m}"

# Logback configuration
LOGBACK_OPTS=""
if [[ -f "\$LOGBACK_FILE" ]]; then
    LOGBACK_OPTS="-Dlogback.configurationFile=\$LOGBACK_FILE"
fi

exec java \$JAVA_OPTS \\
    -Dconfig.file="\$CONFIG_FILE" \\
    -Dlog.dir="\$LOG_DIR" \\
    \$LOGBACK_OPTS \\
    -jar "\$JAR_FILE" \\
    "\$@"
LAUNCHER_EOF

    chmod +x "$TARGET_DIR/bin/softclient4es"

    success "Created $TARGET_DIR/bin/softclient4es"
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

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%date{HH:mm:ss.SSS} %-5level %logger{20} - %msg%n</pattern>
        </encoder>
    </appender>

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
    cat > "$TARGET_DIR/VERSION" << EOF
SoftClient4ES Installation Info
================================
Installed:          $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Elasticsearch:      $ES_VERSION
Version:            $SOFT_VERSION
Scala:              $SCALA_VERSION
Java Required:      $REQUIRED_JAVA_VERSION+
Artifact:           $ARTIFACT_NAME
OS:                 $OS_TYPE
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
    echo "  OS detected:            $OS_TYPE"
    echo ""
    echo "  Directory structure:"
    echo "    $TARGET_DIR/"
    echo "    ├── bin/"
    echo "    │   └── softclient4es"
    echo "    ├── conf/"
    echo "    │   ├── application.conf"
    echo "    │   └── logback.xml"
    echo "    ├── lib/"
    echo "    │   └── $JAR_NAME"
    echo "    ├── logs/"
    echo "    │   └── softclient4es.log  (created at runtime)"
    echo "    ├── LICENSE"
    echo "    ├── README.md"
    echo "    ├── VERSION"
    echo "    └── uninstall.sh"
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
    download_docs
    create_config
    create_logback_config
    create_launcher
    create_uninstaller
    create_version_info
    print_summary
}

main
