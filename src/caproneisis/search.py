"""
CaproneISIS Search — Interactive Search Interface
=================================================

Provides an interactive search interface for exploring indices.
"""

import time
from typing import Optional, List
from .core import CaproneIndex


class SearchInterface:
    """
    Interactive search interface for CaproneISIS indices.

    Example:
        interface = SearchInterface("corpus", hosts=["http://localhost:9200"])
        interface.interactive()
    """

    def __init__(
        self,
        index_name: str,
        hosts: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        basic_auth: Optional[tuple] = None
    ):
        """
        Initialize search interface.

        Args:
            index_name: Name of the index to search
            hosts: List of ES node URLs
            api_key: API key for authentication
            basic_auth: Tuple of (username, password)
        """
        self.index = CaproneIndex(
            index_name,
            hosts=hosts,
            api_key=api_key,
            basic_auth=basic_auth,
            create_if_missing=False
        )

    def search(
        self,
        query: str,
        limit: int = 20,
        year: Optional[int] = None,
        show_content: bool = False
    ) -> None:
        """
        Execute search and print results.

        Args:
            query: Search query
            limit: Maximum results
            year: Optional year filter
            show_content: Show content field
        """
        start = time.time()
        results = self.index.search(query, limit=limit, year=year)
        elapsed_ms = (time.time() - start) * 1000

        print(f"\n{'='*70}")
        print(f"Query: {query}")
        if year:
            print(f"Year filter: {year}")
        print(f"Results: {len(results)} (in {elapsed_ms:.1f}ms)")
        print(f"{'='*70}\n")

        for i, r in enumerate(results, 1):
            score = r.get('score', 0)
            print(f"{i:3}. [{r['year']}] {r['id']}")
            print(f"     {r['title'][:80]}..." if len(r['title']) > 80 else f"     {r['title']}")
            if score:
                print(f"     Score: {score:.2f}")
            if show_content and r['content']:
                content_preview = r['content'][:200].replace('\n', ' ')
                print(f"     {content_preview}...")
            print()

    def interactive(self):
        """
        Start interactive search session.
        """
        print("\n" + "="*70)
        print("CaproneISIS Interactive Search")
        print("="*70)
        print("Commands:")
        print("  <query>        - Search for terms")
        print("  :year <YYYY>   - Set year filter")
        print("  :limit <N>     - Set result limit")
        print("  :content       - Toggle content display")
        print("  :stats         - Show index statistics")
        print("  :quit          - Exit")
        print("="*70 + "\n")

        year_filter = None
        limit = 20
        show_content = False

        while True:
            try:
                query = input("search> ").strip()

                if not query:
                    continue

                if query == ":quit" or query == ":q":
                    break

                if query.startswith(":year"):
                    parts = query.split()
                    if len(parts) == 2:
                        year_filter = int(parts[1])
                        print(f"Year filter set to: {year_filter}")
                    else:
                        year_filter = None
                        print("Year filter cleared")
                    continue

                if query.startswith(":limit"):
                    parts = query.split()
                    if len(parts) == 2:
                        limit = int(parts[1])
                        print(f"Limit set to: {limit}")
                    continue

                if query == ":content":
                    show_content = not show_content
                    print(f"Content display: {'ON' if show_content else 'OFF'}")
                    continue

                if query == ":stats":
                    stats = self.index.stats()
                    print("\nIndex Statistics:")
                    print(f"  Total records: {stats['total_records']:,}")
                    print(f"  Size: {stats['size_gb']:.2f} GB")
                    print(f"  Shards: {stats['shards']} primary, {stats['replicas']} replica")
                    print(f"  Top years: {stats['top_years']}")
                    print()
                    continue

                self.search(query, limit=limit, year=year_filter, show_content=show_content)

            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}")

        self.index.close()


def benchmark(
    index_name: str,
    hosts: Optional[List[str]] = None,
    api_key: Optional[str] = None,
    queries: Optional[List[str]] = None
) -> dict:
    """
    Run benchmark queries against an index.

    Args:
        index_name: Index to benchmark
        hosts: ES hosts
        api_key: API key
        queries: List of test queries (uses defaults if None)

    Returns:
        Benchmark results
    """
    if queries is None:
        queries = [
            "quantum",
            "neural network",
            "climate change",
            "machine learning",
            "polysemanticity"
        ]

    index = CaproneIndex(
        index_name,
        hosts=hosts,
        api_key=api_key,
        create_if_missing=False
    )

    results = []
    total_time = 0

    print(f"\n{'Query':<30} {'Results':>10} {'Time (ms)':>12}")
    print("-" * 55)

    for query in queries:
        start = time.time()
        hits = index.search(query, limit=100)
        elapsed_ms = (time.time() - start) * 1000
        total_time += elapsed_ms

        count = len(hits)
        print(f"{query:<30} {count:>10,} {elapsed_ms:>12.1f}")

        results.append({
            "query": query,
            "results": count,
            "time_ms": elapsed_ms
        })

    print("-" * 55)
    avg_time = total_time / len(queries)
    print(f"{'Average':<30} {'':<10} {avg_time:>12.1f}")

    index.close()

    return {
        "queries": results,
        "average_ms": avg_time,
        "total_ms": total_time
    }
