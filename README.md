# CaproneISIS

**Industrial-Scale CDS/ISIS Implementation Using Elasticsearch**

CaproneISIS is the "big brother" of [capraISIS](https://github.com/capraCoder/capraisis), designed for massive datasets (100M–10B records) using Elasticsearch as the backend.

Both packages implement UNESCO's CDS/ISIS (**C**omputerised **D**ocumentation **S**ystem / **I**ntegrated **S**et of **I**nformation **S**ystems, 1985–2005) inverted file indexing principles — capraISIS for portable single-file use, caproneISIS for distributed enterprise scale.

## Family Architecture

| Package | Backend | Scale | Use Case |
|---------|---------|-------|----------|
| **capraISIS** | SQLite FTS5 | <100M records | Research, portable, zero dependencies |
| **caproneISIS** | Elasticsearch | 100M–10B records | Enterprise, distributed, horizontally scalable |

**Same API, different scale.** Code written for capraISIS migrates to caproneISIS by changing one import.

## Installation

```bash
pip install caproneisis
```

**Requires:** Elasticsearch 8.x cluster (local or remote)

### Quick Start with Docker

```bash
# Start local Elasticsearch (for testing)
docker run -d --name es-test \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  elasticsearch:8.11.0
```

## Quick Start

### Python API

```python
from caproneisis import CaproneIndex

# Connect to local Elasticsearch
index = CaproneIndex("my_corpus")

# Add records (same API as capraISIS)
index.add(
    id="10.5281/zenodo.12345",
    title="Quantum Mechanics and Consciousness",
    content="A paper exploring the relationship between...",
    year=2024,
    prefix="10.5281"
)

# Bulk add
records = [
    ("10.1234/a", "Title A", "Content A", 2023, "10.1234"),
    ("10.1234/b", "Title B", "Content B", 2024, "10.1234"),
]
index.bulk_add(records)

# Search
results = index.search("quantum consciousness")
for r in results:
    print(f"{r['year']} | {r['id']} | {r['title']}")

# Statistics
stats = index.stats()
print(f"Total records: {stats['total_records']:,}")
print(f"Size: {stats['size_gb']:.2f} GB")
print(f"Shards: {stats['shards']}")
```

### Production Cluster

```python
from caproneisis import CaproneIndex

# Connect to production cluster with authentication
index = CaproneIndex(
    "production_corpus",
    hosts=["https://es1.example.com:9200", "https://es2.example.com:9200"],
    api_key="your-api-key",
    verify_certs=True,
    shards=10,      # More shards for larger clusters
    replicas=2      # Higher redundancy for production
)
```

### Command Line

```bash
# Cluster management
python -m caproneisis cluster health
python -m caproneisis cluster indices

# Create index
python -m caproneisis create myindex --shards 5 --replicas 1

# Build index from JSONL files
python -m caproneisis build "data/*.jsonl" --index myindex

# Search
python -m caproneisis search myindex "quantum mechanics"
python -m caproneisis search myindex "neural network" --year 2024

# Interactive search
python -m caproneisis interactive myindex

# Statistics
python -m caproneisis stats myindex

# Benchmark
python -m caproneisis benchmark myindex
```

## Large-Scale Indexing

For massive datasets (millions of records), use the `IndexBuilder`:

```python
from caproneisis import IndexBuilder

builder = IndexBuilder(
    "datacite",
    hosts=["http://localhost:9200"],
    batch_size=5000,         # Optimal for Elasticsearch
    thread_count=4,          # Parallel ingestion threads
    shards=10,               # Scale horizontally
    replicas=1
)

# Build from DataCite JSONL files
stats = builder.add_jsonl_files(
    "/path/to/DataCite/**/*.jsonl",
    resume=True              # Skip already processed files
)

print(f"Indexed {stats['total_records']:,} records")
print(f"Rate: {stats['rate_per_second']:,.0f} records/second")
```

## Cluster Management

```python
from caproneisis import ClusterManager

manager = ClusterManager(hosts=["http://localhost:9200"])

# Check cluster health
health = manager.health()
print(f"Status: {health['status']}")

# List indices
for idx in manager.indices():
    print(f"{idx['name']}: {idx['docs_count']:,} docs, {idx['size']}")

# Optimize index for queries
manager.optimize("myindex", max_segments=1)

# Create alias for zero-downtime reindexing
manager.alias("myindex_v2", "myindex")
```

## Search Syntax

CaproneISIS supports Elasticsearch query string syntax:

| Query | Meaning |
|-------|---------|
| `quantum mechanics` | Both terms (AND) |
| `quantum OR mechanics` | Either term |
| `quantum -classical` | Exclude term |
| `"quantum mechanics"` | Exact phrase |
| `quant*` | Prefix match |
| `title:quantum` | Field-specific |
| `year:[2020 TO 2024]` | Range query |

## Performance

Designed for enterprise scale:

| Metric | Capability |
|--------|------------|
| **Records** | 100M–10B |
| **Ingestion** | 10,000+ records/second |
| **Search** | <100ms at billion scale |
| **Shards** | Configurable (5 default) |
| **Replicas** | Configurable (1 default) |

### Scaling Guidelines

| Records | Recommended Shards | Notes |
|--------:|-------------------:|-------|
| <10M | 1–3 | Single node sufficient |
| 10M–100M | 5–10 | Small cluster |
| 100M–1B | 10–50 | Multi-node cluster |
| >1B | 50–100+ | Large cluster, consider aliases |

## Why Not capraISIS?

Use **capraISIS** when:
- You need zero dependencies (stdlib only)
- Portability matters (single `.db` file)
- Records <100M
- Local/research use

Use **caproneISIS** when:
- You need horizontal scaling
- Records >100M
- Production/enterprise deployment
- You already have Elasticsearch infrastructure

## Migration from capraISIS

The APIs are compatible. Migration is straightforward:

```python
# Before (capraISIS)
from capraisis import CapraIndex
index = CapraIndex("corpus.db")

# After (caproneISIS)
from caproneisis import CaproneIndex
index = CaproneIndex("corpus")  # Now an ES index name
```

Most code works unchanged. Key differences:
- File path → Index name
- Single file → Cluster connection
- Automatic refresh → Configurable refresh

## License

MIT License. Use at your own risk.

## Citation

If you use CaproneISIS in research, please cite:

```bibtex
@software{caproneisis2026,
  author = {Caprazli, Kafkas M.},
  title = {CaproneISIS: Industrial-Scale CDS/ISIS Implementation},
  year = {2026},
  url = {https://github.com/capraCoder/caproneisis}
}
```

**Author ORCID:** [0000-0002-5744-8944](https://orcid.org/0000-0002-5744-8944)

## See Also

- [capraISIS](https://github.com/capraCoder/capraisis) — Little sibling (SQLite FTS5)
- [Elasticsearch](https://www.elastic.co/elasticsearch/) — Backend engine
- [UNESCO CDS/ISIS](https://wayback.archive-it.org/all/20110128100935/http://portal.unesco.org/ci/en/ev.php-URL_ID=2071&URL_DO=DO_TOPIC&URL_SECTION=201.html) — Original system (archived)

---

*"Scale the mountain, honour the heritage."*
