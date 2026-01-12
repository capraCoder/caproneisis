"""
CaproneISIS Cluster — Elasticsearch Cluster Management
======================================================

Utilities for managing Elasticsearch clusters, indices, and operations.

This module provides cluster-level operations that go beyond single-index
management, useful for production deployments.
"""

from typing import Optional, List, Dict, Any
from elasticsearch import Elasticsearch


class ClusterManager:
    """
    Elasticsearch cluster management utilities.

    Example:
        manager = ClusterManager(hosts=["http://localhost:9200"])
        print(manager.health())
        print(manager.indices())
    """

    def __init__(
        self,
        hosts: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        basic_auth: Optional[tuple] = None,
        verify_certs: bool = True
    ):
        """
        Initialize cluster manager.

        Args:
            hosts: List of ES node URLs
            api_key: API key for authentication
            basic_auth: Tuple of (username, password)
            verify_certs: Verify SSL certificates
        """
        conn_kwargs: Dict[str, Any] = {
            "hosts": hosts or ["http://localhost:9200"],
            "verify_certs": verify_certs
        }

        if api_key:
            conn_kwargs["api_key"] = api_key
        elif basic_auth:
            conn_kwargs["basic_auth"] = basic_auth

        self._client = Elasticsearch(**conn_kwargs)

    def health(self) -> dict:
        """
        Get cluster health status.

        Returns:
            Dict with cluster health information
        """
        return self._client.cluster.health()

    def info(self) -> dict:
        """
        Get cluster information.

        Returns:
            Dict with cluster info (version, name, etc.)
        """
        return self._client.info()

    def indices(self) -> List[dict]:
        """
        List all indices with stats.

        Returns:
            List of index info dicts
        """
        cat_indices = self._client.cat.indices(format="json")
        return [
            {
                "name": idx["index"],
                "health": idx.get("health", "unknown"),
                "status": idx.get("status", "unknown"),
                "docs_count": int(idx.get("docs.count", 0)),
                "size": idx.get("store.size", "0b"),
                "pri_shards": int(idx.get("pri", 0)),
                "rep_shards": int(idx.get("rep", 0))
            }
            for idx in cat_indices
            if not idx["index"].startswith(".")  # Skip system indices
        ]

    def create_index(
        self,
        name: str,
        shards: int = 5,
        replicas: int = 1,
        mapping: Optional[dict] = None
    ) -> dict:
        """
        Create a new index with CaproneISIS schema.

        Args:
            name: Index name
            shards: Number of primary shards
            replicas: Number of replica shards
            mapping: Custom mapping (uses default if None)

        Returns:
            Creation response
        """
        from .core import CaproneIndex

        body = {
            "settings": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
                "refresh_interval": "30s"
            }
        }

        if mapping:
            body["mappings"] = mapping
        else:
            body["mappings"] = CaproneIndex.INDEX_MAPPING["mappings"]

        return self._client.indices.create(index=name, body=body)

    def delete_index(self, name: str) -> dict:
        """
        Delete an index.

        Args:
            name: Index name

        Returns:
            Deletion response
        """
        return self._client.indices.delete(index=name)

    def refresh(self, index: str) -> dict:
        """
        Force refresh an index (makes recent changes searchable).

        Args:
            index: Index name

        Returns:
            Refresh response
        """
        return self._client.indices.refresh(index=index)

    def optimize(self, index: str, max_segments: int = 1) -> dict:
        """
        Force merge to optimize query performance.

        Args:
            index: Index name
            max_segments: Target number of segments per shard

        Returns:
            Merge response
        """
        return self._client.indices.forcemerge(
            index=index,
            max_num_segments=max_segments
        )

    def reindex(
        self,
        source: str,
        dest: str,
        wait_for_completion: bool = True
    ) -> dict:
        """
        Reindex from source to destination index.

        Args:
            source: Source index name
            dest: Destination index name
            wait_for_completion: Wait for reindex to complete

        Returns:
            Reindex response
        """
        return self._client.reindex(
            body={
                "source": {"index": source},
                "dest": {"index": dest}
            },
            wait_for_completion=wait_for_completion
        )

    def alias(self, index: str, alias: str) -> dict:
        """
        Create an alias for an index.

        Args:
            index: Index name
            alias: Alias name

        Returns:
            Alias response
        """
        return self._client.indices.put_alias(index=index, name=alias)

    def get_stats(self, index: str) -> dict:
        """
        Get detailed index statistics.

        Args:
            index: Index name

        Returns:
            Index statistics
        """
        stats = self._client.indices.stats(index=index)
        primary = stats["_all"]["primaries"]

        return {
            "docs_count": primary["docs"]["count"],
            "docs_deleted": primary["docs"]["deleted"],
            "size_bytes": primary["store"]["size_in_bytes"],
            "size_gb": primary["store"]["size_in_bytes"] / 1e9,
            "indexing_total": primary["indexing"]["index_total"],
            "search_total": primary["search"]["query_total"],
            "search_time_ms": primary["search"]["query_time_in_millis"]
        }

    def close(self):
        """Close the Elasticsearch client connection."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
