from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from pipelines.retailpulse.generate import generate_dataset
from pipelines.retailpulse.pipeline import build_silver_sales, build_spark, run_pipeline
from pipelines.retailpulse.spark_schemas import (
    CALENDAR_SCHEMA,
    PRODUCT_SCHEMA,
    SALES_SCHEMA,
    STORE_SCHEMA,
)


ARTIFACTS_DIR = Path(".test_artifacts")


def fresh_dir(name: str) -> Path:
    path = ARTIFACTS_DIR / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def test_pipeline_quarantines_known_bad_rows() -> None:
    base_dir = fresh_dir("pipeline_quality")
    raw_dir = base_dir / "raw"
    warehouse_dir = base_dir / "warehouse"

    generate_dataset(raw_dir=raw_dir, rows=30, seed=11)
    report = run_pipeline(raw_dir=raw_dir, warehouse_dir=warehouse_dir)

    assert report["input_rows"] == 33
    assert report["accepted_rows"] == 30
    assert report["rejected_rows"] == 3
    assert report["duplicate_rows"] == 1
    assert report["missing_product_rows"] == 1
    assert report["invalid_price_rows"] == 1
    assert (warehouse_dir / "bronze" / "sales_transactions").exists()
    assert (warehouse_dir / "silver" / "sales_enriched").exists()
    assert (warehouse_dir / "gold" / "metric_overview_daily").exists()
    assert (warehouse_dir / "quarantine" / "sales_transactions").exists()


def test_deduplication_keeps_first_transaction_and_rejects_copy() -> None:
    spark = build_spark("RetailPulseDeduplicationTest")
    try:
        sales_row = (
            "T00000001",
            "O00000001",
            date(2025, 1, 1),
            "P001",
            "S001",
            "store",
            1,
            100.0,
            0.0,
            100.0,
            100.0,
            70.0,
            70.0,
            30.0,
            "paid",
        )
        raw_tables = {
            "sales_transactions": spark.createDataFrame([sales_row, sales_row], SALES_SCHEMA),
            "products": spark.createDataFrame(
                [("P001", "Product", "electronics", "Brand", 100.0, 70.0, date(2024, 1, 1), True)],
                PRODUCT_SCHEMA,
            ),
            "stores": spark.createDataFrame(
                [("S001", "Warsaw", "Mazowieckie", "flagship", date(2023, 1, 1), "large_city")],
                STORE_SCHEMA,
            ),
            "calendar": spark.createDataFrame(
                [(date(2025, 1, 1), 2025, 1, 1, "January", 1, "Wednesday", False, "", "winter")],
                CALENDAR_SCHEMA,
            ),
        }

        silver, quarantine = build_silver_sales(raw_tables)

        assert silver.count() == 1
        assert quarantine.count() == 1
        assert quarantine.select("quality_reason").first()["quality_reason"] == "duplicate_transaction"
    finally:
        spark.stop()
