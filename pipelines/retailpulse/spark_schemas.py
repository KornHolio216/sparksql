from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SALES_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_percent", DoubleType(), False),
        StructField("gross_revenue", DoubleType(), False),
        StructField("net_revenue", DoubleType(), False),
        StructField("unit_cost", DoubleType(), False),
        StructField("total_cost", DoubleType(), False),
        StructField("margin", DoubleType(), False),
        StructField("order_status", StringType(), False),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("brand", StringType(), False),
        StructField("base_price", DoubleType(), False),
        StructField("base_cost", DoubleType(), False),
        StructField("launch_date", DateType(), False),
        StructField("is_active", BooleanType(), False),
    ]
)

STORE_SCHEMA = StructType(
    [
        StructField("store_id", StringType(), False),
        StructField("city", StringType(), False),
        StructField("region", StringType(), False),
        StructField("store_type", StringType(), False),
        StructField("opened_at", DateType(), False),
        StructField("population_segment", StringType(), False),
    ]
)

CALENDAR_SCHEMA = StructType(
    [
        StructField("date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("day_of_week", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("holiday_name", StringType(), True),
        StructField("season", StringType(), False),
    ]
)
