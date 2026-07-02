from __future__ import annotations

import os
import platform
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_TIMEZONE_NAME = "Europe/Warsaw"
PROJECT_TIMEZONE = ZoneInfo(PROJECT_TIMEZONE_NAME)


def now_project_time() -> datetime:
    return datetime.now(PROJECT_TIMEZONE).replace(microsecond=0)


def configure_windows_spark_environment() -> None:
    if platform.system() != "Windows":
        return

    hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop"))
    winutils_path = hadoop_home / "bin" / "winutils.exe"
    if winutils_path.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        bin_path = str(hadoop_home / "bin")
        current_path = os.environ.get("PATH", "")
        if bin_path.lower() not in current_path.lower():
            os.environ["PATH"] = os.pathsep.join([bin_path, current_path])


@dataclass(frozen=True)
class GenerationPreset:
    name: str
    rows: int
    start_date: str
    end_date: str
    seed: int


PRESETS: dict[str, GenerationPreset] = {
    "dev": GenerationPreset(
        name="dev",
        rows=5_000,
        start_date="2025-01-01",
        end_date="2026-06-30",
        seed=42,
    ),
    "demo": GenerationPreset(
        name="demo",
        rows=75_000,
        start_date="2025-01-01",
        end_date="2026-06-30",
        seed=20260702,
    ),
    "large": GenerationPreset(
        name="large",
        rows=500_000,
        start_date="2024-01-01",
        end_date="2026-06-30",
        seed=20260702,
    ),
}


@dataclass(frozen=True)
class PathConfig:
    raw_dir: Path = PROJECT_ROOT / "data" / "retailpulse" / "generated"
    warehouse_dir: Path = PROJECT_ROOT / "data" / "retailpulse" / "warehouse"

    @property
    def sales_path(self) -> Path:
        return self.raw_dir / "sales_transactions.csv"

    @property
    def products_path(self) -> Path:
        return self.raw_dir / "products.csv"

    @property
    def stores_path(self) -> Path:
        return self.raw_dir / "stores.csv"

    @property
    def calendar_path(self) -> Path:
        return self.raw_dir / "calendar.csv"

    @property
    def manifest_path(self) -> Path:
        return self.raw_dir / "generation_manifest.json"

    @property
    def bronze_dir(self) -> Path:
        return self.warehouse_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.warehouse_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.warehouse_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.warehouse_dir / "quarantine"

    @property
    def quality_report_path(self) -> Path:
        return self.warehouse_dir / "quality_report.json"
