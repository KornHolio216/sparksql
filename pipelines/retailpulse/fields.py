from __future__ import annotations


SALES_FIELDNAMES = [
    "transaction_id",
    "order_id",
    "transaction_date",
    "product_id",
    "store_id",
    "channel",
    "quantity",
    "unit_price",
    "discount_percent",
    "gross_revenue",
    "net_revenue",
    "unit_cost",
    "total_cost",
    "margin",
    "order_status",
]

PRODUCT_FIELDNAMES = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "base_price",
    "base_cost",
    "launch_date",
    "is_active",
]

STORE_FIELDNAMES = [
    "store_id",
    "city",
    "region",
    "store_type",
    "opened_at",
    "population_segment",
]

CALENDAR_FIELDNAMES = [
    "date",
    "year",
    "quarter",
    "month",
    "month_name",
    "week_of_year",
    "day_of_week",
    "is_weekend",
    "holiday_name",
    "season",
]
