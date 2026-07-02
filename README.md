<div align="center">

# RetailPulse AI

A reproducible PySpark pipeline for generating, validating and transforming synthetic retail sales data into curated Parquet datasets.

![Python](https://img.shields.io/badge/PYTHON-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PYSPARK-DATA_PIPELINE-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Parquet](https://img.shields.io/badge/PARQUET-CURATED_DATA-50ABF1?style=for-the-badge)

</div>

## Overview

RetailPulse AI is the data-engineering foundation of a retail analytics project. It intentionally uses PySpark to show explicit schemas, Spark SQL joins and aggregations, data-quality checks and Parquet outputs.

```text
synthetic retail data
    -> bronze raw tables
    -> silver cleaned facts
    -> gold analytical summaries
    -> quality report
```

The generator creates `sales_transactions`, `products`, `stores` and `calendar` CSV files. Generated data and pipeline outputs are reproducible and ignored by Git.

## Install

Use Python 3.10, 3.11 or 3.12.

```bash
python -m venv .venv
pip install -e ".[dev]"
```

On Windows, Spark needs `winutils.exe`; the project auto-detects `C:\hadoop\bin\winutils.exe` when it exists.

## Commands

```bash
python -m pipelines.retailpulse.cli generate --preset dev
python -m pipelines.retailpulse.cli pipeline
python -m pipelines.retailpulse.cli all --preset dev
```

Presets: `dev`, `demo`, `large`.

## Outputs

```text
data/retailpulse/generated/
data/retailpulse/warehouse/bronze/
data/retailpulse/warehouse/silver/
data/retailpulse/warehouse/gold/
data/retailpulse/warehouse/quarantine/
data/retailpulse/warehouse/quality_report.json
```

## Tests

```bash
pytest
```
