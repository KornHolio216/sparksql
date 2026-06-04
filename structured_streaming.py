import os
import platform
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lower,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    trim,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input_stream"
PRODUCTS_PATH = BASE_DIR / "data" / "products.csv"
OUTPUT_DIR = BASE_DIR / "data" / "output_stream"
CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "lab11_window_parquet"


def configure_environment_from_lab10() -> None:
    if platform.system() != "Windows":
        return

    java_home = Path(r"C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot")
    hadoop_home = Path(r"C:\hadoop")

    if java_home.exists():
        os.environ["JAVA_HOME"] = str(java_home)
    if hadoop_home.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)

    path_parts = []
    if "JAVA_HOME" in os.environ:
        path_parts.append(str(Path(os.environ["JAVA_HOME"]) / "bin"))
    if "HADOOP_HOME" in os.environ:
        path_parts.append(str(Path(os.environ["HADOOP_HOME"]) / "bin"))

    if path_parts:
        os.environ["PATH"] = os.pathsep.join(path_parts + [os.environ.get("PATH", "")])


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("LAB11_StructuredStreaming")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    return spark


def main() -> None:
    configure_environment_from_lab10()

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark()

    print("Spark/PySpark version:", spark.version)
    print("Folder wejściowy streamingu:", INPUT_DIR)
    print("Plik products.csv z LAB10:", PRODUCTS_PATH)
    print("Folder wyników Parquet:", OUTPUT_DIR)
    print("Folder checkpointów:", CHECKPOINT_DIR)

    #dane strumieniowe są generowane na podstawie sales.csv.
    stream_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("miasto", StringType(), True),
        StructField("produkt", StringType(), True),
        StructField("category", StringType(), True),
        StructField("ilosc", IntegerType(), True),
        StructField("cena", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])

    raw_df = (
        spark.readStream
        .schema(stream_schema)
        .option("header", True)
        .option("maxFilesPerTrigger", 1)
        .csv(str(INPUT_DIR))
    )

    #Transformacje i czyszczenie danych:
    # 1) konwersja czasu tekstowego na timestamp,
    # 2) ujednolicenie kategorii i statusu,
    # 3) odrzucenie błędnych rekordów,
    # 4) dodanie kolumny wartosc_z_vat
    df = (
        raw_df
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("category", lower(trim(col("category"))))
        .withColumn("status", lower(trim(col("status"))))
        .filter(col("event_time").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("amount") >= 0)
        .withColumn("wartosc_z_vat", spark_round(col("amount") * 1.23, 2))
        .select(
            "event_time",
            "user_id",
            "miasto",
            "produkt",
            "category",
            "ilosc",
            "cena",
            "amount",
            "wartosc_z_vat",
            "status",
        )
    )

    print("Czy DataFrame jest strumieniowy?:", df.isStreaming)
    print("Schemat strumieniowego DataFrame:")
    df.printSchema()

    products_df = spark.read.csv(str(PRODUCTS_PATH), header=True, inferSchema=True)

    enriched_df = (
        df.join(products_df, on="produkt", how="left")
        .withColumn(
            "szacowana_marza",
            spark_round(col("amount") * col("marza_procent") / 100, 2),
        )
    )

    paid_df = enriched_df.filter(col("status") == "paid")

    #agregacja według kategorii.
    category_summary = (
        paid_df
        .groupBy("category")
        .agg(
            count("*").alias("events_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
            spark_round(spark_sum("szacowana_marza"), 2).alias("total_estimated_margin"),
        )
    )

    #Dodatkowe podsumowanie według miasta i produktu
    city_product_summary = (
        paid_df
        .groupBy("miasto", "produkt")
        .agg(
            spark_sum("ilosc").alias("suma_sztuk"),
            spark_round(spark_sum("amount"), 2).alias("suma_sprzedazy"),
        )
    )

    #okna stałe 10 minut + watermark 10 minut.
    fixed_window_summary = (
        paid_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "10 minutes"), col("category"))
        .agg(
            count("*").alias("events_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
        )
    )

    #okna przesuwające 10 minut co 5 minut.
    sliding_window_summary = (
        paid_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "10 minutes", "5 minutes"), col("category"))
        .agg(
            count("*").alias("events_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
        )
    )

    category_query = (
        category_summary.writeStream
        .queryName("category_summary_console")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    city_product_query = (
        city_product_summary.writeStream
        .queryName("city_product_summary_console")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    fixed_window_console_query = (
        fixed_window_summary.writeStream
        .queryName("fixed_window_console")
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    sliding_window_console_query = (
        sliding_window_summary.writeStream
        .queryName("sliding_window_console")
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    #zapis wyniku okien stałych do Parquet + checkpointing.
    file_output = fixed_window_summary.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("events_count"),
        col("total_amount"),
    )

    file_query = (
        file_output.writeStream
        .queryName("fixed_window_parquet")
        .format("parquet")
        .outputMode("append")
        .option("path", str(OUTPUT_DIR))
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("\nAplikacja działa. W drugim terminalu uruchom:")
    print("python generate_stream_files.py")
    print("\nZatrzymanie: Ctrl+C. Przy teście restartu NIE usuwaj folderu checkpoints/.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nZatrzymywanie zapytań strumieniowych...")
        for query in [
            category_query,
            city_product_query,
            fixed_window_console_query,
            sliding_window_console_query,
            file_query,
        ]:
            if query.isActive:
                query.stop()
    finally:
        spark.stop()
        print("Spark został zatrzymany.")


if __name__ == "__main__":
    main()
