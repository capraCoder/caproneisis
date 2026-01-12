"""
CaproneISIS CLI — Command-Line Interface
========================================

Command-line interface for CaproneISIS operations.

Usage:
    python -m caproneisis cluster health
    python -m caproneisis cluster indices
    python -m caproneisis create myindex --shards 5
    python -m caproneisis build "data/*.jsonl" --index myindex
    python -m caproneisis search myindex "quantum mechanics"
    python -m caproneisis stats myindex
    python -m caproneisis benchmark myindex
"""

import argparse
from typing import List, Optional


def get_hosts(args) -> List[str]:
    """Extract hosts from args."""
    if args.hosts:
        return args.hosts.split(",")
    return ["http://localhost:9200"]


def cmd_cluster_health(args):
    """Show cluster health."""
    from .cluster import ClusterManager

    manager = ClusterManager(
        hosts=get_hosts(args),
        api_key=args.api_key
    )

    health = manager.health()
    print(f"\nCluster: {health['cluster_name']}")
    print(f"Status: {health['status']}")
    print(f"Nodes: {health['number_of_nodes']}")
    print(f"Data nodes: {health['number_of_data_nodes']}")
    print(f"Active shards: {health['active_shards']}")
    print(f"Relocating shards: {health['relocating_shards']}")
    print(f"Unassigned shards: {health['unassigned_shards']}")

    manager.close()


def cmd_cluster_indices(args):
    """List all indices."""
    from .cluster import ClusterManager

    manager = ClusterManager(
        hosts=get_hosts(args),
        api_key=args.api_key
    )

    indices = manager.indices()

    print(f"\n{'Index':<30} {'Health':<8} {'Docs':>12} {'Size':>10}")
    print("-" * 65)

    for idx in indices:
        print(
            f"{idx['name']:<30} "
            f"{idx['health']:<8} "
            f"{idx['docs_count']:>12,} "
            f"{idx['size']:>10}"
        )

    manager.close()


def cmd_create(args):
    """Create a new index."""
    from .cluster import ClusterManager

    manager = ClusterManager(
        hosts=get_hosts(args),
        api_key=args.api_key
    )

    manager.create_index(
        name=args.index,
        shards=args.shards,
        replicas=args.replicas
    )

    print(f"Created index: {args.index}")
    print(f"  Shards: {args.shards}")
    print(f"  Replicas: {args.replicas}")

    manager.close()


def cmd_delete(args):
    """Delete an index."""
    from .cluster import ClusterManager

    if not args.force:
        confirm = input(f"Delete index '{args.index}'? [y/N] ")
        if confirm.lower() != 'y':
            print("Aborted.")
            return

    manager = ClusterManager(
        hosts=get_hosts(args),
        api_key=args.api_key
    )

    manager.delete_index(args.index)
    print(f"Deleted index: {args.index}")

    manager.close()


def cmd_build(args):
    """Build index from JSONL files."""
    from .builder import IndexBuilder

    builder = IndexBuilder(
        index_name=args.index,
        hosts=get_hosts(args),
        api_key=args.api_key,
        batch_size=args.batch_size,
        shards=args.shards,
        replicas=args.replicas
    )

    builder.add_jsonl_files(
        pattern=args.pattern,
        test_limit=args.limit,
        resume=not args.no_resume
    )

    builder.close()


def cmd_search(args):
    """Search an index."""
    from .core import CaproneIndex
    import time

    index = CaproneIndex(
        args.index,
        hosts=get_hosts(args),
        api_key=args.api_key,
        create_if_missing=False
    )

    start = time.time()
    results = index.search(
        args.query,
        limit=args.limit,
        year=args.year
    )
    elapsed_ms = (time.time() - start) * 1000

    print(f"\nQuery: {args.query}")
    print(f"Results: {len(results)} (in {elapsed_ms:.1f}ms)\n")

    for r in results:
        print(f"[{r['year']}] {r['id']}")
        title = r['title'][:70] + "..." if len(r['title']) > 70 else r['title']
        print(f"  {title}\n")

    index.close()


def cmd_interactive(args):
    """Start interactive search session."""
    from .search import SearchInterface

    interface = SearchInterface(
        args.index,
        hosts=get_hosts(args),
        api_key=args.api_key
    )
    interface.interactive()


def cmd_stats(args):
    """Show index statistics."""
    from .core import CaproneIndex

    index = CaproneIndex(
        args.index,
        hosts=get_hosts(args),
        api_key=args.api_key,
        create_if_missing=False
    )

    stats = index.stats()

    print(f"\nIndex: {args.index}")
    print(f"{'='*50}")
    print(f"Total records: {stats['total_records']:,}")
    print(f"Size: {stats['size_gb']:.2f} GB")
    print(f"Shards: {stats['shards']} primary, {stats['replicas']} replica")
    print("\nTop years:")
    for year, count in list(stats['top_years'].items())[:5]:
        print(f"  {year}: {count:,}")
    print("\nTop prefixes:")
    for prefix, count in list(stats['top_prefixes'].items())[:5]:
        print(f"  {prefix}: {count:,}")

    index.close()


def cmd_benchmark(args):
    """Run benchmark queries."""
    from .search import benchmark

    queries = args.queries.split(",") if args.queries else None

    benchmark(
        index_name=args.index,
        hosts=get_hosts(args),
        api_key=args.api_key,
        queries=queries
    )


def main(argv: Optional[List[str]] = None):
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="caproneisis",
        description="CaproneISIS — Industrial-Scale CDS/ISIS Implementation"
    )

    # Global options
    parser.add_argument(
        "--hosts",
        help="Elasticsearch hosts (comma-separated)",
        default=None
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        help="Elasticsearch API key",
        default=None
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # cluster command
    cluster_parser = subparsers.add_parser("cluster", help="Cluster operations")
    cluster_sub = cluster_parser.add_subparsers(dest="cluster_cmd")

    cluster_sub.add_parser("health", help="Show cluster health")
    cluster_sub.add_parser("indices", help="List all indices")

    # create command
    create_parser = subparsers.add_parser("create", help="Create an index")
    create_parser.add_argument("index", help="Index name")
    create_parser.add_argument("--shards", type=int, default=5, help="Primary shards")
    create_parser.add_argument("--replicas", type=int, default=1, help="Replica shards")

    # delete command
    delete_parser = subparsers.add_parser("delete", help="Delete an index")
    delete_parser.add_argument("index", help="Index name")
    delete_parser.add_argument("-f", "--force", action="store_true", help="Skip confirmation")

    # build command
    build_parser = subparsers.add_parser("build", help="Build index from files")
    build_parser.add_argument("pattern", help="Glob pattern for JSONL files")
    build_parser.add_argument("--index", required=True, help="Target index name")
    build_parser.add_argument("--batch-size", type=int, default=5000, help="Batch size")
    build_parser.add_argument("--shards", type=int, default=5, help="Primary shards")
    build_parser.add_argument("--replicas", type=int, default=1, help="Replica shards")
    build_parser.add_argument("--limit", type=int, help="Limit records (for testing)")
    build_parser.add_argument("--no-resume", action="store_true", help="Don't resume previous build")

    # search command
    search_parser = subparsers.add_parser("search", help="Search an index")
    search_parser.add_argument("index", help="Index name")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", type=int, default=20, help="Max results")
    search_parser.add_argument("--year", type=int, help="Year filter")

    # interactive command
    interactive_parser = subparsers.add_parser("interactive", help="Interactive search")
    interactive_parser.add_argument("index", help="Index name")

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show index statistics")
    stats_parser.add_argument("index", help="Index name")

    # benchmark command
    bench_parser = subparsers.add_parser("benchmark", help="Run benchmarks")
    bench_parser.add_argument("index", help="Index name")
    bench_parser.add_argument("--queries", help="Comma-separated test queries")

    # Parse and dispatch
    args = parser.parse_args(argv)

    if args.command == "cluster":
        if args.cluster_cmd == "health":
            cmd_cluster_health(args)
        elif args.cluster_cmd == "indices":
            cmd_cluster_indices(args)
        else:
            cluster_parser.print_help()
    elif args.command == "create":
        cmd_create(args)
    elif args.command == "delete":
        cmd_delete(args)
    elif args.command == "build":
        cmd_build(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "interactive":
        cmd_interactive(args)
    elif args.command == "stats":
        cmd_stats(args)
    elif args.command == "benchmark":
        cmd_benchmark(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
