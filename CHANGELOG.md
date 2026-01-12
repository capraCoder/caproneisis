# Changelog

All notable changes to CaproneISIS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-12

### Added

- **Core module** (`caproneisis.core`): `CaproneIndex` class for Elasticsearch-based indices
  - Full-text search with Elasticsearch query DSL
  - Bulk ingestion via Elasticsearch bulk API
  - Configurable shards and replicas
  - API-compatible with capraISIS (little sibling)

- **Builder module** (`caproneisis.builder`): `IndexBuilder` for large-scale construction
  - Parallel bulk ingestion using `helpers.parallel_bulk`
  - Resumable builds with progress tracking
  - DataCite JSONL extractor included

- **Cluster module** (`caproneisis.cluster`): `ClusterManager` for cluster operations
  - Cluster health monitoring
  - Index lifecycle management
  - Force merge / optimization
  - Reindexing support

- **CLI** (`python -m caproneisis`): Command-line interface
  - `cluster health` — Show cluster status
  - `cluster indices` — List all indices
  - `create` — Create new index
  - `delete` — Delete index
  - `build` — Build index from JSONL files
  - `search` — Search index
  - `interactive` — Interactive search session
  - `stats` — Show index statistics
  - `benchmark` — Run performance benchmarks

- **Search module** (`caproneisis.search`): Interactive search interface
  - Real-time query execution
  - Year filtering
  - Content preview toggle
  - Benchmark utilities

### Technical Details

- Requires Elasticsearch 8.x
- Uses official `elasticsearch` Python client
- Supports API key and basic authentication
- Configurable connection pooling
- Optimal batch size: 5000 (Elasticsearch recommendation)

### Family Architecture

CaproneISIS is the "big brother" of capraISIS:

| Package | Backend | Scale | Use Case |
|---------|---------|-------|----------|
| capraISIS | SQLite FTS5 | <100M | Research, portable |
| caproneISIS | Elasticsearch | 100M–10B | Enterprise, distributed |

Both share the same API surface for easy migration.

---

*"Scale the mountain, honour the heritage."*
