import argparse

from .ingest import Ingestor


def main() -> None:
    p = argparse.ArgumentParser(description="Load CSV files dropped into <root>/landing into DuckDB")
    p.add_argument("root")
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--interval", type=float, default=10.0, help="seconds between scans")
    p.add_argument("--once", action="store_true")
    args = p.parse_args()
    ing = Ingestor(args.root, args.db)
    if args.once:
        r = ing.run_once()
        print(f"loaded {r.loaded} ({r.rows} rows), duplicates {r.duplicates}, "
              f"quarantined {r.quarantined}, still writing {r.still_writing}")
    else:
        ing.watch(args.interval)


if __name__ == "__main__":
    main()
