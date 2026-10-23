"""Demo fixtures, Silver and Gold; service extraction is owned by Airbyte."""
import argparse
import json
from .storage import ingest_sources


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("ingest", "build", "verify", "build-gold", "verify-gold", "demo"))
    parser.add_argument("--sources", default="all")
    parser.add_argument("--silver-snapshot", help="Silver snapshot to use when building Gold; default is Silver LATEST")
    args = parser.parse_args()
    if args.command == "demo":
        from .airbyte import verify_bronze
        print(json.dumps(verify_bronze(), indent=2))
    if args.command in ("ingest", "demo"):
        sources = None if args.sources == "all" else args.sources.split(",")
        print(json.dumps(ingest_sources(sources), indent=2))
    if args.command in ("build", "demo"):
        from .silver import build_silver
        silver_result = build_silver()
        print(json.dumps(silver_result, indent=2))
    if args.command in ("build-gold", "demo"):
        from .gold import build_gold
        source_id = silver_result["snapshot_id"] if args.command == "demo" else args.silver_snapshot
        print(json.dumps(build_gold(silver_snapshot_id=source_id), indent=2))
    if args.command == "verify":
        from .silver import validate_snapshot
        print(json.dumps(validate_snapshot(), indent=2))
    if args.command == "verify-gold":
        from .gold import validate_snapshot
        print(json.dumps(validate_snapshot(), indent=2))


if __name__ == "__main__":
    main()
