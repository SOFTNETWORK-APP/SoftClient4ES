# Elasticsearch Scroll & Search After Implementation

## Supported Clients
- Jest (ES 5-7) - Legacy
- RestHighLevelClient (ES 6-7) - Deprecated
- Java Client (ES 8-9) - Current

## Features
- ✅ Retry with exponential backoff
- ✅ Automatic tie-breaker for search_after
- ✅ Shard failure detection
- ✅ Resource cleanup
- ✅ Detailed error messages

## Usage
See trait `ScrollApi` for the common interface.