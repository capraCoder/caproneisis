"""
CaproneISIS Builder — Large-Scale Index Construction
====================================================

Handles construction of inverted file indices from massive datasets
using Elasticsearch bulk API and parallel ingestion.

Design principles (inherited from CDS/ISIS):
    - Stream processing: Never load entire dataset into memory
    - Bulk API: Optimal batch sizes for Elasticsearch (5000 default)
    - Parallel ingestion: Multiple threads for maximum throughput
    - Progress reporting: Visibility into long-running builds
    - Resume capability: Track progress, recover from interruption

Typical usage:
    builder = IndexBuilder("corpus", hosts=["http://localhost:9200"])
    builder.add_jsonl_files("data/*.jsonl", extractor=my_extractor)
"""

import json
import time
from pathlib import Path
from datetime import datetime
from typing import Callable, Optional, Iterator, Tuple, List, Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk

from .core import CaproneIndex


# Type alias for record extractor function
RecordExtractor = Callable[[dict], Optional[Tuple[str, str, str, str, str]]]


def default_extractor(rec: dict) -> Optional[Tuple[str, str, str, str, str]]:
    """
    Default record extractor for DataCite JSONL format.

    Args:
        rec: Raw JSON record

    Returns:
        Tuple of (id, title, content, year, prefix) or None if invalid
    """
    try:
        doi = rec.get('id', '')
        attrs = rec.get('attributes', {})

        # Combine all titles
        titles = ' '.join(
            t.get('title', '') for t in attrs.get('titles', [])
        )

        # Combine all descriptions
        descs = ' '.join(
            d.get('description', '') for d in attrs.get('descriptions', [])
        )

        year = str(attrs.get('publicationYear', ''))
        prefix = attrs.get('prefix', '')

        return (doi, titles, descs, year, prefix)
    except Exception:
        return None


class IndexBuilder:
    """
    Builder for constructing CaproneISIS indices from large datasets.

    Features:
        - Elasticsearch bulk API for efficient ingestion
        - Parallel bulk for multi-threaded throughput
        - Resumable builds via progress tracking
        - Configurable batch sizes
        - Progress reporting
    """

    # Metadata index for tracking build progress
    META_INDEX = "_caproneisis_meta"

    def __init__(
        self,
        index_name: str,
        hosts: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        basic_auth: Optional[tuple] = None,
        batch_size: int = 5000,
        progress_interval: int = 100_000,
        thread_count: int = 4,
        shards: int = 5,
        replicas: int = 1,
        refresh_interval: str = "-1"
    ):
        """
        Initialize the builder.

        Args:
            index_name: Target index name
            hosts: List of ES node URLs
            api_key: API key for authentication
            basic_auth: Tuple of (username, password)
            batch_size: Records per bulk request (5000 optimal for ES)
            progress_interval: Records between progress reports
            thread_count: Threads for parallel_bulk
            shards: Number of primary shards
            replicas: Number of replica shards
            refresh_interval: Index refresh interval (default "-1" = disabled for bulk)
        """
        self.index_name = index_name
        self.batch_size = batch_size
        self.progress_interval = progress_interval
        self.thread_count = thread_count

        # Build connection kwargs
        conn_kwargs: Dict[str, Any] = {
            "hosts": hosts or ["http://localhost:9200"]
        }
        if api_key:
            conn_kwargs["api_key"] = api_key
        elif basic_auth:
            conn_kwargs["basic_auth"] = basic_auth

        self._client = Elasticsearch(**conn_kwargs)

        # Create main index via CaproneIndex
        self.index = CaproneIndex(
            index_name,
            hosts=hosts,
            api_key=api_key,
            basic_auth=basic_auth,
            shards=shards,
            replicas=replicas,
            refresh_interval=refresh_interval
        )

        # Initialize progress tracking
        self._processed_files: set = set()
        self._init_meta_index()
        self._load_progress()

    def _init_meta_index(self):
        """Create metadata index for progress tracking."""
        if not self._client.indices.exists(index=self.META_INDEX):
            self._client.indices.create(
                index=self.META_INDEX,
                body={
                    "mappings": {
                        "properties": {
                            "index_name": {"type": "keyword"},
                            "processed_files": {"type": "text"},
                            "last_updated": {"type": "date"}
                        }
                    }
                }
            )

    def _load_progress(self):
        """Load set of already processed files (for resume)."""
        try:
            doc = self._client.get(
                index=self.META_INDEX,
                id=f"progress_{self.index_name}"
            )
            files_json = doc["_source"].get("processed_files", "[]")
            self._processed_files = set(json.loads(files_json))
        except Exception:
            self._processed_files = set()

    def _save_progress(self):
        """Save processed files list."""
        self._client.index(
            index=self.META_INDEX,
            id=f"progress_{self.index_name}",
            document={
                "index_name": self.index_name,
                "processed_files": json.dumps(list(self._processed_files)),
                "last_updated": datetime.utcnow().isoformat()
            }
        )

    def add_jsonl_files(
        self,
        pattern: str,
        extractor: RecordExtractor = default_extractor,
        test_limit: Optional[int] = None,
        resume: bool = True,
        parallel: bool = True
    ) -> dict:
        """
        Add records from JSONL files matching pattern.

        Args:
            pattern: Glob pattern for files (e.g., "data/**/*.jsonl")
            extractor: Function to extract (id, title, content, year, prefix)
            test_limit: Stop after N records (for testing)
            resume: Skip already processed files
            parallel: Use parallel_bulk for multi-threaded ingestion

        Returns:
            Build statistics dict
        """
        # Find all matching files
        if '**' in pattern:
            base = pattern.split('**')[0]
            files = sorted(Path(base).rglob(pattern.split('**/')[-1]))
        else:
            base_path = Path(pattern).parent
            glob_pattern = Path(pattern).name
            files = sorted(base_path.glob(glob_pattern))

        print(f"Found {len(files)} files matching pattern")

        # Counters
        total_records = 0
        total_errors = 0
        start_time = time.time()

        def generate_actions():
            nonlocal total_records, total_errors

            for file_idx, filepath in enumerate(files):
                file_key = str(filepath)

                # Resume support
                if resume and file_key in self._processed_files:
                    continue

                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        for line in f:
                            try:
                                rec = json.loads(line)
                                extracted = extractor(rec)

                                if extracted:
                                    total_records += 1
                                    yield {
                                        "_index": self.index_name,
                                        "_id": extracted[0],
                                        "_source": {
                                            "id": extracted[0],
                                            "title": extracted[1],
                                            "content": extracted[2],
                                            "year": extracted[3],
                                            "prefix": extracted[4]
                                        }
                                    }

                                    # Progress report
                                    if total_records % self.progress_interval == 0:
                                        elapsed = time.time() - start_time
                                        rate = total_records / elapsed
                                        print(
                                            f"[{datetime.now().strftime('%H:%M:%S')}] "
                                            f"{total_records:,} records | "
                                            f"{rate:,.0f} rec/sec | "
                                            f"File {file_idx + 1}/{len(files)}"
                                        )

                                    # Test limit
                                    if test_limit and total_records >= test_limit:
                                        return

                            except json.JSONDecodeError:
                                total_errors += 1

                except Exception as e:
                    print(f"Error processing {filepath}: {e}")
                    total_errors += 1
                    continue

                # Mark file complete
                self._processed_files.add(file_key)

                # Periodic progress save
                if len(self._processed_files) % 100 == 0:
                    self._save_progress()

        # Execute bulk ingestion
        if parallel:
            for success, info in parallel_bulk(
                self._client,
                generate_actions(),
                chunk_size=self.batch_size,
                thread_count=self.thread_count,
                raise_on_error=False
            ):
                if not success:
                    total_errors += 1
        else:
            bulk(
                self._client,
                generate_actions(),
                chunk_size=self.batch_size,
                raise_on_error=False
            )

        # Save final progress
        self._save_progress()

        # Refresh index for immediate searchability
        self.index.refresh()

        # Build statistics
        elapsed = time.time() - start_time
        stats = {
            "total_records": total_records,
            "total_errors": total_errors,
            "elapsed_seconds": elapsed,
            "elapsed_hours": elapsed / 3600,
            "rate_per_second": total_records / elapsed if elapsed > 0 else 0,
            "files_processed": len(self._processed_files)
        }

        self._print_summary(stats)
        return stats

    def add_records(
        self,
        records: Iterator[Tuple],
        total_hint: Optional[int] = None
    ) -> dict:
        """
        Add records from any iterator.

        Args:
            records: Iterator yielding (id, title, content, year, prefix) tuples
            total_hint: Expected total count (for progress)

        Returns:
            Build statistics
        """
        total_records = 0
        start_time = time.time()

        def generate_actions():
            nonlocal total_records
            for rec in records:
                total_records += 1
                yield {
                    "_index": self.index_name,
                    "_id": rec[0],
                    "_source": {
                        "id": rec[0],
                        "title": rec[1] if len(rec) > 1 else "",
                        "content": rec[2] if len(rec) > 2 else "",
                        "year": str(rec[3]) if len(rec) > 3 and rec[3] else "",
                        "prefix": rec[4] if len(rec) > 4 else ""
                    }
                }

                if total_records % self.progress_interval == 0:
                    elapsed = time.time() - start_time
                    rate = total_records / elapsed
                    pct = f" ({100*total_records/total_hint:.1f}%)" if total_hint else ""
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        f"{total_records:,} records{pct} | "
                        f"{rate:,.0f} rec/sec"
                    )

        bulk(self._client, generate_actions(), chunk_size=self.batch_size)

        self.index.refresh()

        elapsed = time.time() - start_time
        return {
            "total_records": total_records,
            "elapsed_seconds": elapsed,
            "rate_per_second": total_records / elapsed if elapsed > 0 else 0
        }

    def _print_summary(self, stats: dict):
        """Print build summary."""
        print()
        print("=" * 60)
        print("BUILD COMPLETE")
        print("=" * 60)
        print(f"Total records: {stats['total_records']:,}")
        print(f"Total errors: {stats['total_errors']:,}")
        print(f"Time elapsed: {stats['elapsed_hours']:.2f} hours")
        print(f"Average rate: {stats['rate_per_second']:,.0f} records/second")
        print(f"Files processed: {stats['files_processed']}")
        print("=" * 60)

    def close(self):
        """Close connections."""
        self.index.close()
        self._client.close()
