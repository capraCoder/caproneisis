"""
CaproneISIS Core — Elasticsearch-Based Inverted File Index
==========================================================

This module implements CDS/ISIS principles using Elasticsearch:

CDS/ISIS Architecture (1985):
    Master File (.MST) → Inverted File (.IFX) → Cross-Reference (.XRF)

CaproneISIS Architecture (2026):
    Elasticsearch Cluster → Inverted Index → Shard Distribution

The fundamental insight: Elasticsearch IS an inverted file index with
distributed B-tree organization. We're scaling CDS/ISIS to billions of records.

Complexity guarantees:
    - INSERT: O(1) amortized with bulk API
    - SEARCH: O(log k + m) where m=matching documents
    - SCALE: Linear with shard count (horizontal scaling)
"""

from typing import Optional, List, Dict, Any, Union
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class CaproneIndex:
    """
    Elasticsearch-based inverted file index for massive datasets.

    The index stores:
        - id: Unique identifier (DOI, ISBN, URI, or any string)
        - title: Searchable title field
        - content: Searchable content/description field
        - year: Publication year (keyword for filtering)
        - prefix: DOI prefix or category (keyword for filtering)

    Example:
        # Local Elasticsearch
        index = CaproneIndex("corpus")
        index.add("10.5281/zenodo.123", "Quantum Mechanics", "A paper about...", 2024)
        results = index.search("quantum")

        # Production cluster
        index = CaproneIndex(
            "corpus",
            hosts=["https://es1:9200", "https://es2:9200"],
            api_key="your-api-key"
        )
    """

    # Index mapping — mirrors CDS/ISIS field structure
    INDEX_MAPPING = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "english",
                    "fields": {"raw": {"type": "keyword"}}
                },
                "content": {
                    "type": "text",
                    "analyzer": "english"
                },
                "year": {"type": "keyword"},
                "prefix": {"type": "keyword"}
            }
        }
    }

    # Default settings for bulk ingestion optimization
    DEFAULT_SETTINGS = {
        "number_of_shards": 5,
        "number_of_replicas": 1,
        "refresh_interval": "30s"
    }

    def __init__(
        self,
        index_name: str,
        hosts: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        basic_auth: Optional[tuple] = None,
        verify_certs: bool = True,
        shards: int = 5,
        replicas: int = 1,
        create_if_missing: bool = True
    ):
        """
        Open or create a CaproneISIS index.

        Args:
            index_name: Name of the Elasticsearch index
            hosts: List of ES node URLs (default: ["http://localhost:9200"])
            api_key: API key for authentication
            basic_auth: Tuple of (username, password)
            verify_certs: Verify SSL certificates
            shards: Number of primary shards (for scaling)
            replicas: Number of replica shards (for redundancy)
            create_if_missing: Create index if it doesn't exist
        """
        self.index_name = index_name
        self.shards = shards
        self.replicas = replicas

        # Build connection kwargs
        conn_kwargs: Dict[str, Any] = {
            "hosts": hosts or ["http://localhost:9200"],
            "verify_certs": verify_certs
        }

        if api_key:
            conn_kwargs["api_key"] = api_key
        elif basic_auth:
            conn_kwargs["basic_auth"] = basic_auth

        self._client = Elasticsearch(**conn_kwargs)

        # Create index if needed
        if create_if_missing and not self._client.indices.exists(index=index_name):
            self._create_index()

    def _create_index(self):
        """Create the index with optimized settings."""
        settings = {
            "settings": {
                "number_of_shards": self.shards,
                "number_of_replicas": self.replicas,
                "refresh_interval": "30s"
            },
            **self.INDEX_MAPPING
        }
        self._client.indices.create(index=self.index_name, body=settings)

    def add(
        self,
        id: str,
        title: str,
        content: str = "",
        year: Optional[int] = None,
        prefix: str = ""
    ) -> None:
        """
        Add a single record to the index.

        Args:
            id: Unique identifier (DOI, ISBN, etc.)
            title: Title of the work
            content: Description or abstract
            year: Publication year
            prefix: DOI prefix or category
        """
        doc = {
            "id": id,
            "title": title,
            "content": content,
            "year": str(year) if year else "",
            "prefix": prefix
        }
        self._client.index(index=self.index_name, id=id, document=doc)

    def bulk_add(
        self,
        records: List[tuple],
        batch_size: int = 5000,
        refresh: bool = False
    ) -> int:
        """
        Add multiple records efficiently using Elasticsearch bulk API.

        Args:
            records: List of tuples (id, title, content, year, prefix)
            batch_size: Records per bulk request
            refresh: Force refresh after bulk (slower but immediate search)

        Returns:
            Number of records added
        """
        def generate_actions():
            for rec in records:
                doc_id = rec[0]
                yield {
                    "_index": self.index_name,
                    "_id": doc_id,
                    "_source": {
                        "id": doc_id,
                        "title": rec[1] if len(rec) > 1 else "",
                        "content": rec[2] if len(rec) > 2 else "",
                        "year": str(rec[3]) if len(rec) > 3 and rec[3] else "",
                        "prefix": rec[4] if len(rec) > 4 else ""
                    }
                }

        success, _ = bulk(
            self._client,
            generate_actions(),
            chunk_size=batch_size,
            refresh=refresh
        )
        return success

    def search(
        self,
        query: str,
        limit: int = 20,
        year: Optional[int] = None,
        prefix: Optional[str] = None
    ) -> List[dict]:
        """
        Full-text search using Elasticsearch query DSL.

        Args:
            query: Search query (supports Elasticsearch query string syntax)
            limit: Maximum results to return
            year: Optional year filter
            prefix: Optional prefix filter

        Returns:
            List of matching records as dicts
        """
        # Build query
        must_clauses = [
            {
                "query_string": {
                    "query": query,
                    "fields": ["title^2", "content"],
                    "default_operator": "AND"
                }
            }
        ]

        filter_clauses = []
        if year:
            filter_clauses.append({"term": {"year": str(year)}})
        if prefix:
            filter_clauses.append({"term": {"prefix": prefix}})

        body = {
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            },
            "size": limit
        }

        response = self._client.search(index=self.index_name, body=body)

        return [
            {
                "id": hit["_source"].get("id", ""),
                "title": hit["_source"].get("title", ""),
                "content": hit["_source"].get("content", ""),
                "year": hit["_source"].get("year", ""),
                "prefix": hit["_source"].get("prefix", ""),
                "score": hit["_score"]
            }
            for hit in response["hits"]["hits"]
        ]

    def search_id(self, id_pattern: str, limit: int = 20) -> List[dict]:
        """
        Search by identifier pattern (wildcard query).

        Args:
            id_pattern: Pattern to match (e.g., "10.5281*" for Zenodo DOIs)
            limit: Maximum results

        Returns:
            List of matching records
        """
        body = {
            "query": {
                "wildcard": {
                    "id": f"*{id_pattern}*"
                }
            },
            "size": limit
        }

        response = self._client.search(index=self.index_name, body=body)

        return [
            {
                "id": hit["_source"].get("id", ""),
                "title": hit["_source"].get("title", ""),
                "content": hit["_source"].get("content", ""),
                "year": hit["_source"].get("year", ""),
                "prefix": hit["_source"].get("prefix", "")
            }
            for hit in response["hits"]["hits"]
        ]

    def count(self, query: Optional[str] = None) -> int:
        """
        Count records (total or matching query).

        Args:
            query: Optional search query

        Returns:
            Record count
        """
        if query:
            body = {
                "query": {
                    "query_string": {
                        "query": query,
                        "fields": ["title", "content"]
                    }
                }
            }
            response = self._client.count(index=self.index_name, body=body)
        else:
            response = self._client.count(index=self.index_name)

        return response["count"]

    def stats(self) -> dict:
        """
        Get index statistics.

        Returns:
            Dict with count, size, shard info, and aggregations
        """
        # Basic count
        count = self.count()

        # Index stats
        idx_stats = self._client.indices.stats(index=self.index_name)
        size_bytes = idx_stats["_all"]["primaries"]["store"]["size_in_bytes"]

        # Year distribution (aggregation)
        year_agg = self._client.search(
            index=self.index_name,
            body={
                "size": 0,
                "aggs": {
                    "years": {
                        "terms": {"field": "year", "size": 10}
                    }
                }
            }
        )
        top_years = {
            b["key"]: b["doc_count"]
            for b in year_agg["aggregations"]["years"]["buckets"]
        }

        # Prefix distribution
        prefix_agg = self._client.search(
            index=self.index_name,
            body={
                "size": 0,
                "aggs": {
                    "prefixes": {
                        "terms": {"field": "prefix", "size": 10}
                    }
                }
            }
        )
        top_prefixes = {
            b["key"]: b["doc_count"]
            for b in prefix_agg["aggregations"]["prefixes"]["buckets"]
        }

        # Shard info
        idx_settings = self._client.indices.get_settings(index=self.index_name)
        settings = idx_settings[self.index_name]["settings"]["index"]

        return {
            "total_records": count,
            "size_bytes": size_bytes,
            "size_gb": size_bytes / 1e9,
            "shards": int(settings.get("number_of_shards", 1)),
            "replicas": int(settings.get("number_of_replicas", 0)),
            "top_years": top_years,
            "top_prefixes": top_prefixes
        }

    def refresh(self):
        """Force index refresh (makes recent changes searchable)."""
        self._client.indices.refresh(index=self.index_name)

    def optimize(self, max_segments: int = 1):
        """
        Force merge to optimize query performance.

        Args:
            max_segments: Target number of segments per shard
        """
        self._client.indices.forcemerge(
            index=self.index_name,
            max_num_segments=max_segments
        )

    def delete_index(self):
        """Delete the entire index. Use with caution!"""
        self._client.indices.delete(index=self.index_name)

    def close(self):
        """Close the Elasticsearch client connection."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
