from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipelines.retailpulse.config import PRESETS
from pipelines.retailpulse.generate import generate_dataset
from pipelines.retailpulse.pipeline import run_pipeline


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="RetailPulse AI data engineering commands.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate deterministic raw CSV data.")
    generate_parser.add_argument("--preset", choices=sorted(PRESETS), default="dev")
    generate_parser.add_argument("--raw-dir", type=Path, default=None)
    generate_parser.add_argument("--seed", type=int, default=None)
    generate_parser.add_argument("--rows", type=int, default=None)

    pipeline_parser = subparsers.add_parser("pipeline", help="Run bronze, silver and gold PySpark pipeline.")
    pipeline_parser.add_argument("--raw-dir", type=Path, default=None)
    pipeline_parser.add_argument("--warehouse-dir", type=Path, default=None)

    all_parser = subparsers.add_parser("all", help="Generate data and run the PySpark pipeline.")
    all_parser.add_argument("--preset", choices=sorted(PRESETS), default="dev")
    all_parser.add_argument("--raw-dir", type=Path, default=None)
    all_parser.add_argument("--warehouse-dir", type=Path, default=None)
    all_parser.add_argument("--seed", type=int, default=None)
    all_parser.add_argument("--rows", type=int, default=None)

    args = parser.parse_args(argv)

    if args.command == "generate":
        result = generate_dataset(
            preset_name=args.preset,
            raw_dir=args.raw_dir,
            seed=args.seed,
            rows=args.rows,
        )
    elif args.command == "pipeline":
        result = run_pipeline(raw_dir=args.raw_dir, warehouse_dir=args.warehouse_dir)
    else:
        generate_dataset(
            preset_name=args.preset,
            raw_dir=args.raw_dir,
            seed=args.seed,
            rows=args.rows,
        )
        result = run_pipeline(raw_dir=args.raw_dir, warehouse_dir=args.warehouse_dir)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
