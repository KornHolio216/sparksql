from __future__ import annotations

import csv
import json
import random
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

from pipelines.retailpulse.config import (
    PRESETS,
    PROJECT_TIMEZONE_NAME,
    GenerationPreset,
    PathConfig,
    now_project_time,
)
from pipelines.retailpulse.fields import (
    CALENDAR_FIELDNAMES,
    PRODUCT_FIELDNAMES,
    SALES_FIELDNAMES,
    STORE_FIELDNAMES,
)


PRODUCTS = [
    ("P001", "Laptop Pro 14", "electronics", "Northbyte", 4_700.00, 3_760.00, "2024-02-15", True),
    ("P002", "Wireless Mouse", "electronics", "Northbyte", 89.00, 52.00, "2023-06-01", True),
    ("P003", "UltraWide Monitor", "electronics", "ScreenMax", 1_450.00, 1_015.00, "2023-11-20", True),
    ("P004", "Standing Desk", "furniture", "HomeOffice", 1_250.00, 760.00, "2024-01-10", True),
    ("P005", "Ergo Chair", "furniture", "HomeOffice", 790.00, 466.00, "2023-09-08", True),
    ("P006", "LED Desk Lamp", "furniture", "BrightDesk", 139.00, 72.00, "2024-03-19", True),
    ("P007", "Coffee Beans 1kg", "grocery", "DailyRoast", 58.00, 36.00, "2023-01-01", True),
    ("P008", "Protein Bar Box", "grocery", "ActiveBite", 72.00, 45.00, "2024-05-06", True),
    ("P009", "Vitamin Set", "beauty", "WellnessLab", 119.00, 64.00, "2024-08-12", True),
    ("P010", "Skin Care Kit", "beauty", "PureSkin", 155.00, 83.00, "2023-10-03", True),
    ("P011", "Running Shoes", "sports", "MoveUp", 329.00, 198.00, "2024-04-05", True),
    ("P012", "Yoga Mat", "sports", "MoveUp", 99.00, 44.00, "2023-05-12", True),
]

STORES = [
    ("S001", "Warsaw", "Mazowieckie", "flagship", "2021-03-01", "large_city"),
    ("S002", "Krakow", "Malopolskie", "mall", "2021-06-15", "large_city"),
    ("S003", "Gdansk", "Pomorskie", "mall", "2022-02-20", "large_city"),
    ("S004", "Wroclaw", "Dolnoslaskie", "mall", "2022-07-10", "large_city"),
    ("S005", "Poznan", "Wielkopolskie", "high_street", "2022-09-01", "large_city"),
    ("S006", "Bydgoszcz", "Kujawsko-Pomorskie", "high_street", "2023-01-18", "mid_city"),
    ("S007", "Lublin", "Lubelskie", "retail_park", "2023-04-11", "mid_city"),
    ("S008", "Rzeszow", "Podkarpackie", "retail_park", "2023-08-29", "mid_city"),
]

CATEGORY_WEIGHTS = {
    "electronics": 0.25,
    "furniture": 0.18,
    "grocery": 0.25,
    "beauty": 0.16,
    "sports": 0.16,
}

STORE_WEIGHTS = {
    "S001": 0.24,
    "S002": 0.16,
    "S003": 0.14,
    "S004": 0.14,
    "S005": 0.12,
    "S006": 0.08,
    "S007": 0.07,
    "S008": 0.05,
}


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def daterange(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def weighted_choice(randomizer: random.Random, weighted_values: dict[str, float]) -> str:
    total = sum(weighted_values.values())
    point = randomizer.uniform(0, total)
    current = 0.0
    for value, weight in weighted_values.items():
        current += weight
        if point <= current:
            return value
    return next(reversed(weighted_values))


def season_for_month(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def holiday_name(day: date) -> str:
    if day.month == 11 and day.weekday() == 4 and 23 <= day.day <= 29:
        return "Black Friday"
    if day.month == 12 and 10 <= day.day <= 24:
        return "Holiday shopping"
    if day.month == 1 and day.day <= 7:
        return "New Year slowdown"
    return ""


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def product_rows() -> list[dict]:
    return [
        {
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "brand": brand,
            "base_price": f"{base_price:.2f}",
            "base_cost": f"{base_cost:.2f}",
            "launch_date": launch_date,
            "is_active": str(is_active).lower(),
        }
        for product_id, product_name, category, brand, base_price, base_cost, launch_date, is_active in PRODUCTS
    ]


def store_rows() -> list[dict]:
    return [
        {
            "store_id": store_id,
            "city": city,
            "region": region,
            "store_type": store_type,
            "opened_at": opened_at,
            "population_segment": population_segment,
        }
        for store_id, city, region, store_type, opened_at, population_segment in STORES
    ]


def calendar_rows(start: date, end: date) -> list[dict]:
    rows = []
    for day in daterange(start, end):
        rows.append(
            {
                "date": day.isoformat(),
                "year": day.year,
                "quarter": (day.month - 1) // 3 + 1,
                "month": day.month,
                "month_name": day.strftime("%B"),
                "week_of_year": int(day.strftime("%V")),
                "day_of_week": day.strftime("%A"),
                "is_weekend": str(day.weekday() >= 5).lower(),
                "holiday_name": holiday_name(day),
                "season": season_for_month(day.month),
            }
        )
    return rows


def choose_product(randomizer: random.Random, transaction_date: date, used_product_ids: set[str]) -> tuple:
    category_weights = dict(CATEGORY_WEIGHTS)
    if transaction_date.month == 11 and transaction_date.day >= 23:
        category_weights["electronics"] *= 1.8
        category_weights["beauty"] *= 1.3
    if transaction_date.month == 12:
        category_weights["electronics"] *= 1.4
        category_weights["sports"] *= 1.2
    if transaction_date.month in (5, 6, 7, 8):
        category_weights["sports"] *= 1.35
    if transaction_date.weekday() >= 5:
        category_weights["grocery"] *= 1.25

    category = weighted_choice(randomizer, category_weights)
    candidates = [product for product in PRODUCTS if product[2] == category and product[0] not in used_product_ids]
    if not candidates:
        candidates = [product for product in PRODUCTS if product[0] not in used_product_ids] or list(PRODUCTS)
    return randomizer.choice(candidates)


def channel_for_date(randomizer: random.Random, transaction_date: date, start: date, end: date) -> str:
    progress = (transaction_date - start).days / max((end - start).days, 1)
    online_weight = 0.26 + 0.20 * progress
    marketplace_weight = 0.11 + 0.05 * progress
    store_weight = max(0.35, 1 - online_weight - marketplace_weight)
    return weighted_choice(randomizer, {"store": store_weight, "online": online_weight, "marketplace": marketplace_weight})


def quantity_for_category(randomizer: random.Random, category: str) -> int:
    if category == "grocery":
        return randomizer.choices([1, 2, 3, 4, 5, 6], weights=[10, 18, 22, 18, 10, 5])[0]
    if category in {"electronics", "furniture"}:
        return randomizer.choices([1, 2, 3], weights=[78, 18, 4])[0]
    return randomizer.choices([1, 2, 3, 4], weights=[45, 35, 15, 5])[0]


def discount_for(randomizer: random.Random, transaction_date: date, channel: str) -> float:
    discount = randomizer.choices([0, 5, 10, 15], weights=[58, 22, 15, 5])[0]
    if holiday_name(transaction_date) == "Black Friday":
        discount += randomizer.choice([10, 15, 20])
    if channel == "marketplace":
        discount += randomizer.choice([0, 3, 5])
    return min(float(discount), 45.0)


def status_for(randomizer: random.Random) -> str:
    return randomizer.choices(["paid", "cancelled"], weights=[96, 4])[0]


def build_sales_line(
    transaction_id: str,
    order_id: str,
    transaction_date: date,
    store_id: str,
    channel: str,
    status: str,
    product: tuple,
    randomizer: random.Random,
) -> dict:
    product_id, _, category, _, base_price, base_cost, _, _ = product
    quantity = quantity_for_category(randomizer, category)
    discount_percent = discount_for(randomizer, transaction_date, channel)
    unit_price = round(base_price * randomizer.uniform(0.96, 1.06), 2)
    unit_cost = round(base_cost * randomizer.uniform(0.98, 1.05), 2)
    gross_revenue = round(quantity * unit_price, 2)
    net_revenue = round(gross_revenue * (1 - discount_percent / 100), 2)
    total_cost = round(quantity * unit_cost, 2)
    margin = round(net_revenue - total_cost, 2)

    if status == "cancelled":
        net_revenue = 0.0
        margin = 0.0

    return {
        "transaction_id": transaction_id,
        "order_id": order_id,
        "transaction_date": transaction_date.isoformat(),
        "product_id": product_id,
        "store_id": store_id,
        "channel": channel,
        "quantity": quantity,
        "unit_price": f"{unit_price:.2f}",
        "discount_percent": f"{discount_percent:.2f}",
        "gross_revenue": f"{gross_revenue:.2f}",
        "net_revenue": f"{net_revenue:.2f}",
        "unit_cost": f"{unit_cost:.2f}",
        "total_cost": f"{total_cost:.2f}",
        "margin": f"{margin:.2f}",
        "order_status": status,
    }


def sales_rows(preset: GenerationPreset) -> tuple[list[dict], dict]:
    randomizer = random.Random(preset.seed)
    start = parse_date(preset.start_date)
    end = parse_date(preset.end_date)
    days = list(daterange(start, end))
    rows: list[dict] = []
    order_index = 1
    transaction_index = 1

    while len(rows) < preset.rows:
        transaction_date = randomizer.choice(days)
        order_id = f"O{order_index:08d}"
        store_id = weighted_choice(randomizer, STORE_WEIGHTS)
        channel = channel_for_date(randomizer, transaction_date, start, end)
        status = status_for(randomizer)
        remaining_rows = preset.rows - len(rows)
        line_count = min(randomizer.choices([1, 2, 3], weights=[70, 23, 7])[0], remaining_rows)
        used_product_ids: set[str] = set()

        for _ in range(line_count):
            product = choose_product(randomizer, transaction_date, used_product_ids)
            used_product_ids.add(product[0])
            rows.append(
                build_sales_line(
                    transaction_id=f"T{transaction_index:08d}",
                    order_id=order_id,
                    transaction_date=transaction_date,
                    store_id=store_id,
                    channel=channel,
                    status=status,
                    product=product,
                    randomizer=randomizer,
                )
            )
            transaction_index += 1

        order_index += 1

    quality_rows = build_quality_issue_rows(rows, start)
    rows.extend(quality_rows)
    manifest_details = {
        "known_patterns": [
            "black_friday_electronics_growth",
            "weekend_sales_effect",
            "online_channel_growth",
            "december_holiday_shopping",
        ],
        "injected_quality_issues": [
            "duplicate_transaction",
            "invalid_price",
            "unknown_product",
        ],
    }
    return rows, manifest_details


def build_quality_issue_rows(rows: list[dict], start: date) -> list[dict]:
    duplicate = dict(rows[0])

    invalid_price = dict(rows[1])
    invalid_price.update(
        {
            "transaction_id": "T_INVALID_PRICE",
            "order_id": "O_INVALID_PRICE",
            "transaction_date": (start + timedelta(days=10)).isoformat(),
            "unit_price": "-99.00",
            "gross_revenue": "-99.00",
            "net_revenue": "-99.00",
            "margin": "-150.00",
            "order_status": "paid",
        }
    )

    missing_product = dict(rows[2])
    missing_product.update(
        {
            "transaction_id": "T_MISSING_PRODUCT",
            "order_id": "O_MISSING_PRODUCT",
            "transaction_date": (start + timedelta(days=20)).isoformat(),
            "product_id": "UNKNOWN_PRODUCT",
            "order_status": "paid",
        }
    )
    return [duplicate, invalid_price, missing_product]


def generate_dataset(
    preset_name: str = "dev",
    raw_dir: Path | None = None,
    seed: int | None = None,
    rows: int | None = None,
) -> dict:
    preset = PRESETS[preset_name]
    if seed is not None or rows is not None:
        preset = GenerationPreset(
            name=preset.name,
            rows=rows if rows is not None else preset.rows,
            start_date=preset.start_date,
            end_date=preset.end_date,
            seed=seed if seed is not None else preset.seed,
        )

    paths = PathConfig(raw_dir=raw_dir) if raw_dir is not None else PathConfig()
    start = parse_date(preset.start_date)
    end = parse_date(preset.end_date)
    sales, manifest_details = sales_rows(preset)

    counts = {
        "products": write_csv(paths.products_path, PRODUCT_FIELDNAMES, product_rows()),
        "stores": write_csv(paths.stores_path, STORE_FIELDNAMES, store_rows()),
        "calendar": write_csv(paths.calendar_path, CALENDAR_FIELDNAMES, calendar_rows(start, end)),
        "sales_transactions": write_csv(paths.sales_path, SALES_FIELDNAMES, sales),
    }

    manifest = {
        "generated_at": now_project_time().isoformat(),
        "timezone": PROJECT_TIMEZONE_NAME,
        "preset": asdict(preset),
        "counts": counts,
        "paths": {
            "products": paths.products_path.name,
            "stores": paths.stores_path.name,
            "calendar": paths.calendar_path.name,
            "sales_transactions": paths.sales_path.name,
        },
        **manifest_details,
    }
    paths.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
