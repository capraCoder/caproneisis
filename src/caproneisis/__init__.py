"""
CaproneISIS — Industrial-Scale CDS/ISIS Implementation
=======================================================

The "big brother" of capraISIS, designed for massive datasets (100M–10B records)
using Elasticsearch as the backend.

CaproneISIS implements the same CDS/ISIS inverted file indexing principles
as capraISIS, but scales horizontally across Elasticsearch clusters for
enterprise and production workloads.

Key Features:
- Elasticsearch 8.x backend
- Distributed indexing across shards
- Horizontal scaling for billions of records
- API-compatible with capraISIS
- Production-ready authentication

Family Architecture:
    capraISIS (little sibling)  →  SQLite FTS5, portable, <100M records
    caproneISIS (big brother)   →  Elasticsearch, distributed, 100M–10B records

Usage:
    from caproneisis import CaproneIndex

    # Connect to local Elasticsearch
    index = CaproneIndex("my_corpus")

    # Add records (same API as capraISIS)
    index.add("10.1234/example", "Title", "Content", 2024, "10.1234")

    # Search
    results = index.search("quantum mechanics")

Reference:
    - capraISIS: https://github.com/capraCoder/capraisis
    - Elasticsearch: https://www.elastic.co/elasticsearch/

License: MIT
"""

__version__ = "0.1.0"
__author__ = "Caprazli"

from .core import CaproneIndex
from .builder import IndexBuilder
from .cluster import ClusterManager

__all__ = ["CaproneIndex", "IndexBuilder", "ClusterManager"]
