from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.retailpulse.config import (
    PROJECT_TIMEZONE_NAME,
    PathConfig,
    configure_windows_spark_environment,
    now_project_time,
)
from pipelines.retailpulse.spark_schemas import (
    CALENDAR_SCHEMA,
    PRODUCT_SCHEMA,
    SALES_SCHEMA,
    STORE_SCHEMA,
)


def build_spark(app_name: str = "RetailPulseBatchPipeline") -> SparkSession:
    configure_windows_spark_environment()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv(spark: SparkSession, path: Path, schema) -> DataFrame:
    return spark.read.schema(schema).option("header", True).csv(str(path))


def write_parquet(df: DataFrame, path: Path) -> None:
    df.write.mode("overwrite").parquet(str(path))


def load_raw_tables(spark: SparkSession, paths: PathConfig) -> dict[str, DataFrame]:
    return {
        "sales_transactions": read_csv(spark, paths.sales_path, SALES_SCHEMA),
        "products": read_csv(spark, paths.products_path, PRODUCT_SCHEMA),
        "stores": read_csv(spark, paths.stores_path, STORE_SCHEMA),
        "calendar": read_csv(spark, paths.calendar_path, CALENDAR_SCHEMA),
    }


def write_bronze(raw_tables: dict[str, DataFrame], paths: PathConfig) -> None:
    for table_name, df in raw_tables.items():
        enriched = df.withColumn("ingested_at", F.current_timestamp())
        write_parquet(enriched, paths.bronze_dir / table_name)


def build_silver_sales(raw_tables: dict[str, DataFrame]) -> tuple[DataFrame, DataFrame]:
    sales = raw_tables["sales_transactions"]
    products = raw_tables["products"]
    stores = raw_tables["stores"]
    calendar = raw_tables["calendar"]

    duplicate_window = Window.partitionBy("transaction_id").orderBy("_source_row_id")
    normalized_sales = (
        sales.withColumn("_source_row_id", F.monotonically_increasing_id())
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("store_id", F.trim(F.col("store_id")))
        .withColumn("_duplicate_rank", F.row_number().over(duplicate_window))
    )

    enriched = (
        normalized_sales.join(products, on="product_id", how="left")
        .join(stores, on="store_id", how="left")
        .join(calendar, normalized_sales.transaction_date == calendar.date, how="left")
        .drop(calendar.date)
    )

    reason_columns = [
        F.when(F.col("_duplicate_rank") > 1, F.lit("duplicate_transaction")),
        F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), F.lit("missing_product_id")),
        F.when(F.col("product_name").isNull(), F.lit("unknown_product")),
        F.when(F.col("city").isNull(), F.lit("unknown_store")),
        F.when(F.col("year").isNull(), F.lit("missing_calendar_date")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), F.lit("invalid_unit_price")),
        F.when(F.col("unit_cost").isNull() | (F.col("unit_cost") < 0), F.lit("invalid_unit_cost")),
        F.when(
            ~F.col("channel").isin("store", "online", "marketplace"),
            F.lit("invalid_channel"),
        ),
        F.when(
            ~F.col("order_status").isin("paid", "cancelled"),
            F.lit("invalid_order_status"),
        ),
        F.when(
            F.col("net_revenue") < 0,
            F.lit("invalid_net_revenue"),
        ),
    ]

    checked = enriched.withColumn("quality_reason", F.concat_ws("|", *reason_columns))
    quarantine = checked.filter(F.col("quality_reason") != "")
    silver = (
        checked.filter(F.col("quality_reason") == "")
        .drop("_source_row_id", "_duplicate_rank", "quality_reason")
        .withColumn("realized_revenue", F.when(F.col("order_status") == "paid", F.col("net_revenue")).otherwise(F.lit(0.0)))
        .withColumn("realized_margin", F.when(F.col("order_status") == "paid", F.col("margin")).otherwise(F.lit(0.0)))
    )
    return silver, quarantine


def build_gold_tables(spark: SparkSession, silver_sales: DataFrame) -> dict[str, DataFrame]:
    silver_sales.createOrReplaceTempView("silver_sales")

    metric_overview_daily = spark.sql(
        """
        SELECT
            transaction_date,
            COUNT(DISTINCT order_id) AS order_count,
            COUNT(*) AS order_line_count,
            SUM(quantity) AS units_sold,
            ROUND(SUM(realized_revenue), 2) AS net_revenue,
            ROUND(SUM(gross_revenue), 2) AS gross_revenue,
            ROUND(SUM(total_cost), 2) AS total_cost,
            ROUND(SUM(realized_margin), 2) AS margin,
            CASE
                WHEN COUNT(DISTINCT order_id) = 0 THEN 0
                ELSE ROUND(SUM(realized_revenue) / COUNT(DISTINCT order_id), 2)
            END AS average_order_value
        FROM silver_sales
        WHERE order_status = 'paid'
        GROUP BY transaction_date
        """
    )

    sales_by_product_daily = spark.sql(
        """
        SELECT
            transaction_date,
            product_id,
            product_name,
            category,
            COUNT(*) AS order_line_count,
            SUM(quantity) AS units_sold,
            ROUND(SUM(realized_revenue), 2) AS net_revenue,
            ROUND(SUM(realized_margin), 2) AS margin
        FROM silver_sales
        WHERE order_status = 'paid'
        GROUP BY transaction_date, product_id, product_name, category
        """
    )

    sales_by_category_daily = spark.sql(
        """
        SELECT
            transaction_date,
            category,
            COUNT(*) AS order_line_count,
            SUM(quantity) AS units_sold,
            ROUND(SUM(realized_revenue), 2) AS net_revenue,
            ROUND(SUM(realized_margin), 2) AS margin
        FROM silver_sales
        WHERE order_status = 'paid'
        GROUP BY transaction_date, category
        """
    )

    sales_by_city_daily = spark.sql(
        """
        SELECT
            transaction_date,
            city,
            region,
            COUNT(*) AS order_line_count,
            SUM(quantity) AS units_sold,
            ROUND(SUM(realized_revenue), 2) AS net_revenue,
            ROUND(SUM(realized_margin), 2) AS margin
        FROM silver_sales
        WHERE order_status = 'paid'
        GROUP BY transaction_date, city, region
        """
    )

    sales_by_channel_daily = spark.sql(
        """
        SELECT
            transaction_date,
            channel,
            COUNT(*) AS order_line_count,
            SUM(quantity) AS units_sold,
            ROUND(SUM(realized_revenue), 2) AS net_revenue,
            ROUND(SUM(realized_margin), 2) AS margin
        FROM silver_sales
        WHERE order_status = 'paid'
        GROUP BY transaction_date, channel
        """
    )

    return {
        "metric_overview_daily": metric_overview_daily,
        "sales_by_product_daily": sales_by_product_daily,
        "sales_by_category_daily": sales_by_category_daily,
        "sales_by_city_daily": sales_by_city_daily,
        "sales_by_channel_daily": sales_by_channel_daily,
    }


def write_quality_report(
    paths: PathConfig,
    input_rows: int,
    silver_sales: DataFrame,
    quarantine: DataFrame,
) -> dict:
    accepted_rows = silver_sales.count()
    rejected_rows = quarantine.count()
    reason_counts = {
        row["quality_reason"]: row["count"]
        for row in quarantine.groupBy("quality_reason").count().collect()
    }
    duplicate_rows = quarantine.filter(F.col("quality_reason").contains("duplicate_transaction")).count()
    missing_product_rows = quarantine.filter(
        F.col("quality_reason").contains("missing_product_id")
        | F.col("quality_reason").contains("unknown_product")
    ).count()
    invalid_price_rows = quarantine.filter(F.col("quality_reason").contains("invalid_unit_price")).count()

    generated_at = now_project_time()
    report = {
        "run_id": generated_at.strftime("%Y%m%dT%H%M%S%z"),
        "generated_at": generated_at.isoformat(),
        "timezone": PROJECT_TIMEZONE_NAME,
        "input_rows": input_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "duplicate_rows": duplicate_rows,
        "missing_product_rows": missing_product_rows,
        "invalid_price_rows": invalid_price_rows,
        "rejection_reasons": reason_counts,
    }
    paths.quality_report_path.parent.mkdir(parents=True, exist_ok=True)
    paths.quality_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def run_pipeline(raw_dir: Path | None = None, warehouse_dir: Path | None = None) -> dict:
    paths = PathConfig(
        raw_dir=raw_dir if raw_dir is not None else PathConfig().raw_dir,
        warehouse_dir=warehouse_dir if warehouse_dir is not None else PathConfig().warehouse_dir,
    )
    spark = build_spark()
    try:
        raw_tables = load_raw_tables(spark, paths)
        input_rows = raw_tables["sales_transactions"].count()

        write_bronze(raw_tables, paths)
        silver_sales, quarantine = build_silver_sales(raw_tables)

        write_parquet(silver_sales, paths.silver_dir / "sales_enriched")
        write_parquet(quarantine, paths.quarantine_dir / "sales_transactions")

        gold_tables = build_gold_tables(spark, silver_sales)
        for table_name, df in gold_tables.items():
            write_parquet(df, paths.gold_dir / table_name)

        return write_quality_report(paths, input_rows, silver_sales, quarantine)
    finally:
        spark.stop()
