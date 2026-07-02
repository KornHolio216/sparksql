from __future__ import annotations

import csv
import shutil
from pathlib import Path

from pipelines.retailpulse.fields import SALES_FIELDNAMES
from pipelines.retailpulse.generate import generate_dataset


ARTIFACTS_DIR = Path(".test_artifacts")


def fresh_dir(name: str) -> Path:
    path = ARTIFACTS_DIR / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def count_csv_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as file:
        return sum(1 for _ in csv.DictReader(file))


def test_generator_is_reproducible_for_same_seed() -> None:
    base_dir = fresh_dir("generator_reproducible")
    first_dir = base_dir / "first"
    second_dir = base_dir / "second"

    generate_dataset(raw_dir=first_dir, rows=20, seed=7)
    generate_dataset(raw_dir=second_dir, rows=20, seed=7)

    for file_name in ["products.csv", "stores.csv", "calendar.csv", "sales_transactions.csv"]:
        assert read_text(first_dir / file_name) == read_text(second_dir / file_name)


def test_generator_writes_expected_sales_columns_and_quality_rows() -> None:
    raw_dir = fresh_dir("generator_columns")
    manifest = generate_dataset(raw_dir=raw_dir, rows=20, seed=7)
    sales_path = raw_dir / "sales_transactions.csv"

    with sales_path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        assert reader.fieldnames == SALES_FIELDNAMES
        rows = list(reader)

    assert count_csv_rows(sales_path) == 23
    assert manifest["counts"]["sales_transactions"] == 23
    assert any(row["transaction_id"] == "T_INVALID_PRICE" for row in rows)
    assert any(row["product_id"] == "UNKNOWN_PRODUCT" for row in rows)
    assert not any(row["order_status"] == "refunded" for row in rows)
    assert manifest["paths"] == {
        "products": "products.csv",
        "stores": "stores.csv",
        "calendar": "calendar.csv",
        "sales_transactions": "sales_transactions.csv",
    }
    assert manifest["known_patterns"]
    assert manifest["injected_quality_issues"] == [
        "duplicate_transaction",
        "invalid_price",
        "unknown_product",
    ]


def test_order_lines_share_order_metadata() -> None:
    raw_dir = fresh_dir("generator_order_lines")
    generate_dataset(raw_dir=raw_dir, rows=80, seed=3)

    with (raw_dir / "sales_transactions.csv").open(newline="", encoding="utf-8") as file:
        rows = [
            row
            for row in csv.DictReader(file)
            if row["transaction_id"] not in {"T_INVALID_PRICE", "T_MISSING_PRODUCT"}
        ]

    metadata_by_order: dict[str, tuple[str, str, str, str]] = {}
    for row in rows:
        metadata = (
            row["transaction_date"],
            row["store_id"],
            row["channel"],
            row["order_status"],
        )
        previous = metadata_by_order.setdefault(row["order_id"], metadata)
        assert previous == metadata

    counts_by_order: dict[str, int] = {}
    for row in rows:
        counts_by_order[row["order_id"]] = counts_by_order.get(row["order_id"], 0) + 1
    multi_line_order_found = any(count > 1 for count in counts_by_order.values())
    assert multi_line_order_found
