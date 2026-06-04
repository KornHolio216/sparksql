import argparse
import csv
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input_stream"
SALES_PATH = BASE_DIR / "data" / "sales.csv"

OUTPUT_FIELDNAMES = [
    "event_time",
    "user_id",
    "miasto",
    "produkt",
    "category",
    "ilosc",
    "cena",
    "amount",
    "status",
]

#większość rekordów ma status paid, ale są też pending/cancelled do pokazania filtrowania.
STATUS_BY_ID = {
    1: "paid",
    2: "paid",
    3: "paid",
    4: "pending",
    5: "paid",
    6: "paid",
    7: "paid",
    8: "paid",
    9: "cancelled",
    10: "paid",
}


def load_sales_rows() -> list[dict]:
    with SALES_PATH.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    prepared_rows = []
    base_datetime = datetime(2026, 5, 1, 10, 0, 0)

    for index, row in enumerate(rows):
        sale_id = int(row["id"])
        ilosc = int(row["ilosc"])
        cena = float(row["cena"])
        amount = ilosc * cena

        event_time = base_datetime + timedelta(minutes=index * 4)

        prepared_rows.append({
            "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": f"u{sale_id:03d}",
            "miasto": row["miasto"],
            "produkt": row["produkt"],
            "category": row["kategoria"],
            "ilosc": ilosc,
            "cena": cena,
            "amount": amount,
            "status": STATUS_BY_ID.get(sale_id, "paid"),
        })

    return prepared_rows


def build_batches(rows: list[dict]) -> list[list[dict]]:
    batches = [
        rows[0:3],
        rows[3:6],
        rows[6:8],
        rows[8:10],
    ]

    #rekord celowo opóźniony. po przetworzeniu późniejszych danych watermark przesunie się do przodu,
    # więc bardzo stary rekord może nie zostać doliczony do zamkniętego okna.
    delayed_row = dict(rows[1])
    delayed_row["event_time"] = "2026-05-01 10:02:00"
    delayed_row["user_id"] = "u999"
    delayed_row["amount"] = 999.0
    delayed_row["ilosc"] = 1
    delayed_row["cena"] = 999.0
    delayed_row["status"] = "paid"

    watermark_moving_row = dict(rows[0])
    watermark_moving_row["event_time"] = "2026-05-01 10:55:00"
    watermark_moving_row["user_id"] = "u998"
    watermark_moving_row["amount"] = 1200.0
    watermark_moving_row["ilosc"] = 1
    watermark_moving_row["cena"] = 1200.0
    watermark_moving_row["status"] = "paid"

    batches.append([delayed_row, watermark_moving_row])
    return batches


def write_batch(batch_number: int, rows: list[dict]) -> Path:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    target_path = INPUT_DIR / f"batch_{batch_number:02d}_{timestamp}.csv"
    tmp_path = INPUT_DIR / f".{target_path.name}.tmp"

    with tmp_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    os.replace(tmp_path, target_path)
    return target_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generator danych CSV")
    parser.add_argument("--delay", type=int, default=8, help="Liczba sekund przerwy między batchami")
    args = parser.parse_args()

    sales_rows = load_sales_rows()
    batches = build_batches(sales_rows)

    print("Źródło danych:", SALES_PATH)
    print("Folder wejściowy streamingu:", INPUT_DIR)
    print("Generator będzie dodawał kolejne pliki CSV bez restartu aplikacji\n")

    for index, rows in enumerate(batches, start=1):
        path = write_batch(index, rows)
        print(f"Dodano {path.name}: {len(rows)} rekordów")
        if index < len(batches):
            time.sleep(args.delay)

    print("\nGenerator zakończył pracę")


if __name__ == "__main__":
    main()
