[Back to index](README.md)

# 📘 REPL Client — SQL Gateway for Elasticsearch

---

## Introduction

The **SoftClient4ES REPL** (Read-Eval-Print Loop) is an interactive command-line interface for executing SQL statements against Elasticsearch clusters.

It provides:

- **Interactive SQL execution** with immediate feedback
- **Full DDL, DML, and DQL support**
- **Formatted table output** with emojis and execution timing
- **Multi-line statement support**
- **Multiple output formats** (ASCII, JSON, CSV)
- **Stream consumption** for real-time data
- **Version-aware** compatibility (ES6 → ES9)

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Connection](#connection)
- [Basic Usage](#basic-usage)
- [Statement Execution](#statement-execution)
- [Multi-line Statements](#multi-line-statements)
- [Output Formatting](#output-formatting)
- [Available Commands](#available-commands)
- [SQL Statement Reference](#sql-statement-reference)
- [Examples](#examples)
- [Version Compatibility](#version-compatibility)

---

## Prerequisites

### Java Requirements

| Elasticsearch Version | Minimum Java Version |
|-----------------------|----------------------|
| ES 6                  | Java 8+              |
| ES 7                  | Java 8+              |
| ES 8                  | Java 8+              |
| ES 9                  | Java 17+             |

### Network Requirements

- Network access to JFrog repository (`softnetwork.jfrog.io`)

---

## Installation

### Quick Install

#### Linux / macOS

```bash
curl -fsSL https://raw.githubusercontent.com/SOFTNETWORK-APP/softclient4es/main/install.sh | bash
```

Or download and run manually:

```bash
curl -O https://raw.githubusercontent.com/SOFTNETWORK-APP/softclient4es/main/install.sh
chmod +x install.sh
./install.sh
```

#### Windows (PowerShell)

```powershell
irm https://raw.githubusercontent.com/SOFTNETWORK-APP/softclient4es/main/install.ps1 | iex
```

Or download and run manually:

```powershell
Invoke-WebRequest -Uri https://raw.githubusercontent.com/SOFTNETWORK-APP/softclient4es/main/install.ps1 -OutFile install.ps1
.\install.ps1
```

### List Available Versions

Before installing, you can list all available versions for a specific Elasticsearch version:

#### Linux / macOS

```bash
./install.sh --list-versions --es-version 8
```

#### Windows

```powershell
.\install.ps1 -ListVersions -EsVersion 8
```

**Example output:**

```
═══════════════════════════════════════════════════════════════
  Available SoftClient4ES Versions for Elasticsearch 8
═══════════════════════════════════════════════════════════════

  Artifact: softclient4es8-cli_2.13
  Java required: 8+

  Versions:

    • 0.16.0-SNAPSHOT
    • 0.16.0

  Total: 2 version(s)

  To install a specific version:
    ./install.sh --es-version 8 --version <version>
```

### Installation Options

| Option            | Linux/Mac                | Windows               | Default                       |
|-------------------|--------------------------|-----------------------|-------------------------------|
| Target directory  | `-t, --target <dir>`     | `-Target <dir>`       | `$HOME/softclient4es`         |
| ES version        | `-e, --es-version <n>`   | `-EsVersion <n>`      | `8`                           |
| Software version  | `-v, --version <ver>`    | `-Version <ver>`      | `latest`                      |
| Scala version     | `-s, --scala <ver>`      | `-ScalaVersion <ver>` | `2.13`                        |
| List versions     | `-l, --list-versions`    | `-ListVersions`       | —                             |

### Installation Examples

#### Linux / macOS

```bash
# Default installation (latest version for ES8)
./install.sh

# List available versions first
./install.sh --list-versions --es-version 8

# Install specific version
./install.sh --es-version 8 --version 0.16.0

# Install for Elasticsearch 9 (requires Java 17+)
./install.sh --es-version 9

# Custom installation directory
./install.sh --target /opt/softclient4es

# Full custom installation
./install.sh --target ~/tools/softclient4es --es-version 7 --version 0.16.0
```

#### Windows

```powershell
# Default installation (latest version for ES8)
.\install.ps1

# List available versions first
.\install.ps1 -ListVersions -EsVersion 8

# Install specific version
.\install.ps1 -EsVersion 8 -Version 0.16.0

# Install for Elasticsearch 9 (requires Java 17+)
.\install.ps1 -EsVersion 9

# Custom installation directory
.\install.ps1 -Target "C:\tools\softclient4es"

# Full custom installation
.\install.ps1 -Target "C:\tools\softclient4es" -EsVersion 7 -Version 0.16.0
```

---

### Directory Structure

After installation:

```
softclient4es/
├── bin/
│   ├── softclient4es       # Linux/Mac launcher
│   ├── softclient4es.bat   # Windows batch launcher
│   └── softclient4es.ps1   # Windows PowerShell launcher
├── conf/
│   └── application.conf    # Configuration file
├── lib/
│   └── softclient4es8-cli_2.13-x.y.z-assembly.jar
└── uninstall.sh            # or uninstall.ps1 on Windows
```

### Add to PATH

#### Linux / macOS

Add to `~/.bashrc` or `~/.zshrc`:

```bash
export PATH="$PATH:$HOME/softclient4es/bin"
```

#### Windows

```powershell
# Temporary (current session)
$env:PATH += ";$env:USERPROFILE\softclient4es\bin"

# Permanent (requires admin)
[Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";$env:USERPROFILE\softclient4es\bin", "User")
```

### Uninstall

#### Linux / macOS

```bash
~/softclient4es/uninstall.sh
```

#### Windows

```powershell
~\softclient4es\uninstall.ps1
```

---

## Connection

### Configuration File

The REPL reads default connection settings from `conf/application.conf`:

```hocon
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
```

### Environment Variables

The configuration file supports environment variable overrides:

| Variable              | Description                  |
|-----------------------|------------------------------|
| `ELASTIC_SCHEME`      | Connection scheme            |
| `ELASTIC_HOST`        | Elasticsearch host           |
| `ELASTIC_PORT`        | Elasticsearch port           |
| `ELASTIC_USERNAME`    | Username for authentication  |
| `ELASTIC_PASSWORD`    | Password for authentication  |
| `ELASTIC_API_KEY`     | API key for authentication   |
| `ELASTIC_BEARER_TOKEN`| Bearer token for auth        |
| `JAVA_OPTS`           | JVM options (default: -Xmx512m)|

### Command-Line Options

Command-line options **override** the configuration file settings:

| Option                       | Short | Description                          | Default     |
|------------------------------|-------|--------------------------------------|-------------|
| `--scheme <scheme>`          | `-s`  | Connection scheme (`http` or `https`)| `http`      |
| `--host <host>`              | `-h`  | Elasticsearch host                   | `localhost` |
| `--port <port>`              | `-p`  | Elasticsearch port                   | `9200`      |
| `--username <user>`          | `-u`  | Username for authentication          | —           |
| `--password <pass>`          | `-P`  | Password for authentication          | —           |
| `--api-key <key>`            | `-k`  | API key for authentication           | —           |
| `--bearer-token <token>`     | `-b`  | Bearer token for authentication      | —           |
| `--file <path>`              | `-f`  | Execute SQL from file and exit       | —           |
| `--command <sql>`            | `-c`  | Execute SQL command and exit         | —           |
| `--help`                     |       | Show help message                    | —           |

### Authentication Methods

The REPL supports multiple authentication methods:

| Method           | Options                      | Use Case                    |
|------------------|------------------------------|-----------------------------|
| Basic Auth       | `-u` / `-P`                  | Username/password           |
| API Key          | `-k`                         | Elasticsearch API key       |
| Bearer Token     | `-b`                         | OAuth/JWT token             |

### Connection Examples

```bash
# Local connection (uses defaults from application.conf)
softclient4es

# Override host and port
softclient4es -h es.example.com -p 9200

# HTTPS with basic authentication
softclient4es -s https -h es.example.com -p 9243 -u admin -P secret

# Using API key
softclient4es -s https -h es.example.com -k "your-api-key"

# Using bearer token
softclient4es -s https -h es.example.com -b "your-bearer-token"

# Execute a single command and exit
softclient4es -c "SHOW TABLES"

# Execute SQL from a file and exit
softclient4es -f /path/to/script.sql

# Combine options
softclient4es -h es.example.com -u admin -P secret -c "SELECT * FROM users LIMIT 10"
```

### Non-Interactive Mode

The REPL can run in non-interactive mode using `-c` or `-f`:

#### Execute a single command

```bash
softclient4es -c "SELECT COUNT(*) FROM users"
```

#### Execute SQL from a file

```bash
softclient4es -f setup.sql
```

The file can contain multiple statements separated by semicolons:

```sql
-- setup.sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL,
  name VARCHAR,
  PRIMARY KEY (id)
);

INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');

SELECT * FROM users;
```

---

## Basic Usage

### Prompt

Once connected, you will see the REPL prompt:

```
sql>
```

### Executing a Statement

Type your SQL statement and press **Enter**:

```
sql> SHOW TABLES;
```

### Exiting

To exit the REPL:

```
sql> exit
```

Or use `Ctrl+D` or `\q`.

---

## Statement Execution

### Single-line Statements

Simple statements can be entered on a single line:

```
sql> SELECT * FROM users LIMIT 10;
```

### Statement Terminator

Statements must end with a **semicolon** (`;`):

```
sql> SHOW TABLES;
```

---

## Multi-line Statements

For complex statements, the REPL supports multi-line input.

Continue typing on the next line until you enter the semicolon:

```
sql> CREATE TABLE users (
   |   id INT NOT NULL,
   |   name VARCHAR,
   |   birthdate DATE,
   |   PRIMARY KEY (id)
   | );
```

The prompt changes to `|` to indicate continuation mode.

---

## Output Formatting

### Table Output (ASCII)

Query results are displayed as formatted tables by default:

```
sql> SHOW TABLES LIKE 'show_%';

| name       | type    | pk | partitioned |
|------------|---------|----|-------------|
| show_users | REGULAR | id |             |
📊 1 row(s) (7ms)
```

### Output Formats

The REPL supports multiple output formats:

| Format  | Description                    |
|---------|--------------------------------|
| `ascii` | Formatted ASCII table (default)|
| `json`  | JSON output                    |
| `csv`   | Comma-separated values         |

Change format using the `format` command:

```
sql> format json
Current format: Json

sql> SELECT * FROM users LIMIT 1;
{"id":1,"name":"Alice","age":30}
```

### DML Results

DML statements return operation counts:

```
sql> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
📊 inserted: 2, updated: 0, deleted: 0, rejected: 0 (15ms)
```

### Timing Information

All statements display execution time by default:

```
📊 6 row(s) (1ms)
```

Toggle timing display:

```
sql> timing
Timing: OFF

sql> timing
Timing: ON
```

---

## Available Commands

### General Commands

| Command       | Shortcut | Description                          |
|---------------|----------|--------------------------------------|
| `help`        | `\h`     | Display help information             |
| `quit`        | `\q`     | Exit the REPL                        |
| `exit`        | `\q`     | Exit the REPL                        |
| `history`     |          | Display command history              |
| `clear`       |          | Clear the screen                     |
| `timing`      |          | Toggle timing display ON/OFF         |
| `format`      |          | Set or show output format            |
| `timeout`     |          | Set or show query timeout            |

### Table Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `tables`           | `\t`     | List all tables (`SHOW TABLES`)      |
| `\st <table>`      |          | Show table details                   |
| `\ct <table>`      |          | Show CREATE TABLE statement          |
| `\dt <table>`      |          | Describe table schema                |

### Pipeline Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `pipelines`        | `\p`     | List all pipelines (`SHOW PIPELINES`)|
| `\sp <pipeline>`   |          | Show pipeline details                |
| `\cp <pipeline>`   |          | Show CREATE PIPELINE statement       |
| `\dp <pipeline>`   |          | Describe pipeline processors         |

### Watcher Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `watchers`         | `\w`     | List all watchers (`SHOW WATCHERS`)  |
| `\sw <watcher>`    |          | Show watcher status                  |

### Enrich Policy Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `policies`         | `\pol`   | List all enrich policies             |
| `\spol <policy>`   |          | Show enrich policy details           |

### Stream Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `consume`          | `\c`     | Start consuming a stream             |
| `stream`           | `\s`     | Show stream status                   |
| `cancel`           | `\x`     | Cancel active stream                 |

---

### Command Details

#### `format [ascii|json|csv]`

Set the output format or display current format:

```
sql> format
Current format: Ascii

sql> format json
Current format: Json

sql> format csv
Current format: Csv
```

#### `timeout [seconds]`

Set the query timeout or display current timeout:

```
sql> timeout
Current timeout: 30s

sql> timeout 60
Timeout set to 60s
```

#### `\st <table_name>`

Show detailed table information:

```
sql> \st users

📋 Table: users [Regular]
...
```

#### `\ct <table_name>`

Show the CREATE TABLE DDL:

```
sql> \ct users

CREATE OR REPLACE TABLE users (
  id INT NOT NULL,
  ...
)
```

#### `\dt <table_name>`

Describe the table schema:

```
sql> \dt users

| Field | Type    | Null | Key | Default | Comment | Script | Extra |
|-------|---------|------|-----|---------|---------|--------|-------|
| id    | INT     | no   | PRI | NULL    |         |        | ()    |
...
```

---

## SQL Statement Reference

The REPL supports the full SQL Gateway syntax.

### DDL Statements

| Statement                  | Description                    |
|----------------------------|--------------------------------|
| `CREATE TABLE`             | Create a new table             |
| `CREATE OR REPLACE TABLE`  | Create or replace a table      |
| `CREATE TABLE AS SELECT`   | Create table from query        |
| `ALTER TABLE`              | Modify table structure         |
| `DROP TABLE`               | Delete a table                 |
| `TRUNCATE TABLE`           | Remove all documents           |
| `CREATE PIPELINE`          | Create an ingest pipeline      |
| `ALTER PIPELINE`           | Modify a pipeline              |
| `DROP PIPELINE`            | Delete a pipeline              |
| `CREATE WATCHER`           | Create a watcher               |
| `DROP WATCHER`             | Delete a watcher               |
| `CREATE ENRICH POLICY`     | Create an enrich policy        |
| `EXECUTE ENRICH POLICY`    | Execute an enrich policy       |
| `DROP ENRICH POLICY`       | Delete an enrich policy        |

### DML Statements

| Statement                       | Description              |
|---------------------------------|--------------------------|
| `INSERT INTO ... VALUES`        | Insert documents         |
| `INSERT INTO ... AS SELECT`     | Insert from query        |
| `UPDATE ... SET ... WHERE`      | Update documents         |
| `DELETE FROM ... WHERE`         | Delete documents         |
| `COPY INTO ... FROM`            | Bulk load from file      |

### DQL Statements

| Statement              | Description                     |
|------------------------|---------------------------------|
| `SELECT`               | Query documents                 |
| `SHOW TABLES`          | List all tables                 |
| `SHOW TABLE`           | Show table details              |
| `SHOW CREATE TABLE`    | Show table DDL                  |
| `DESCRIBE TABLE`       | Describe table schema           |
| `SHOW PIPELINES`       | List all pipelines              |
| `SHOW PIPELINE`        | Show pipeline details           |
| `SHOW CREATE PIPELINE` | Show pipeline DDL               |
| `DESCRIBE PIPELINE`    | Describe pipeline processors    |
| `SHOW WATCHERS`        | List all watchers               |
| `SHOW WATCHER STATUS`  | Show watcher status             |
| `SHOW ENRICH POLICIES` | List all enrich policies        |
| `SHOW ENRICH POLICY`   | Show enrich policy details      |

---

## Examples

### Create and Query a Table

```
sql> CREATE TABLE IF NOT EXISTS demo_users (
   |   id INT NOT NULL,
   |   name VARCHAR,
   |   age INT,
   |   PRIMARY KEY (id)
   | );
✔ Table created (120ms)

sql> INSERT INTO demo_users (id, name, age) VALUES
   |   (1, 'Alice', 30),
   |   (2, 'Bob', 25),
   |   (3, 'Chloe', 35);
📊 inserted: 3, updated: 0, deleted: 0, rejected: 0 (45ms)

sql> SELECT * FROM demo_users ORDER BY age DESC;

| id | name  | age |
|----|-------|-----|
| 3  | Chloe | 35  |
| 1  | Alice | 30  |
| 2  | Bob   | 25  |
📊 3 row(s) (8ms)
```

### Using Shortcut Commands

```
sql> tables

| name       | type    | pk | partitioned |
|------------|---------|----|-------------|
| demo_users | REGULAR | id |             |
📊 1 row(s) (5ms)

sql> \dt demo_users

| Field | Type    | Null | Key | Default | Comment | Script | Extra |
|-------|---------|------|-----|---------|---------|--------|-------|
| id    | INT     | no   | PRI | NULL    |         |        | ()    |
| name  | VARCHAR | yes  |     | NULL    |         |        | ()    |
| age   | INT     | yes  |     | NULL    |         |        | ()    |
📊 3 row(s) (5ms)

sql> \ct demo_users

CREATE OR REPLACE TABLE demo_users (
	id INT NOT NULL,
	name VARCHAR,
	age INT,
	PRIMARY KEY (id)
)
OPTIONS = (...)
```

### Change Output Format

```
sql> format json

sql> SELECT * FROM demo_users WHERE id = 1;
{"id":1,"name":"Alice","age":30}
📊 1 row(s) (3ms)

sql> format csv

sql> SELECT * FROM demo_users;
id,name,age
1,Alice,30
2,Bob,25
3,Chloe,35
📊 3 row(s) (4ms)

sql> format ascii
```

### Pipeline Inspection

```
sql> pipelines

| name                              | processors_count |
|-----------------------------------|------------------|
| demo_users_ddl_default_pipeline   | 1                |
📊 1 row(s) (3ms)

sql> \dp demo_users_ddl_default_pipeline

| processor_type | description        | field | ignore_failure | options                                        |
|----------------|--------------------|-------|----------------|------------------------------------------------|
| set            | PRIMARY KEY (id)   | _id   | no             | (value = "{{id}}", ignore_empty_value = false) |
📊 1 row(s) (2ms)
```

### Watcher and Policy Commands

```
sql> watchers

| id          | active | status  | ... |
|-------------|--------|---------|-----|
| my_watcher  | true   | Healthy | ... |
📊 1 row(s) (9ms)

sql> \sw my_watcher

| id          | active | status  | execution_status | ... |
|-------------|--------|---------|------------------|-----|
| my_watcher  | true   | Healthy | Executed         | ... |
📊 1 row(s) (5ms)

sql> policies

| name      | type  | indices   | match_field | enrich_fields |
|-----------|-------|-----------|-------------|---------------|
| my_policy | match | dql_users | id          | name,email    |
📊 1 row(s) (4ms)
```

### Cleanup

```
sql> DROP TABLE IF EXISTS demo_users;
✔ Table dropped (35ms)

sql> exit
Goodbye!
```

---

## Version Compatibility

| Feature           | ES6  | ES7  | ES8  | ES9  |
|-------------------|------|------|------|------|
| REPL Client       | ✔    | ✔    | ✔    | ✔    |
| DDL Statements    | ✔    | ✔    | ✔    | ✔    |
| DML Statements    | ✔    | ✔    | ✔    | ✔    |
| DQL Statements    | ✔    | ✔    | ✔    | ✔    |
| Watchers          | ✔    | ✔    | ✔    | ✔    |
| Enrich Policies   | ✖    | ✔*   | ✔    | ✔    |

\* Enrich policies require ES 7.5+

---

[Back to index](README.md)