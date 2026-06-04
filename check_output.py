import os
import platform
from pathlib import Path

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "output_stream"

# config zeby nie odpalac co chwile nowej konsoli
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


def main() -> None:
    configure_environment_from_lab10()

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("LAB11_CheckOutput")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("System:", platform.system())
    print("Spark/PySpark version:", spark.version)
    print("Odczyt wyników z:", OUTPUT_DIR)

    parquet_files = list(OUTPUT_DIR.rglob("*.parquet"))
    if not parquet_files:
        print("Brak plików Parquet. Najpierw uruchom structured_streaming.py oraz generate_stream_files.py.")
        spark.stop()
        return

    df = spark.read.parquet(str(OUTPUT_DIR))

    print("Schemat wyników zapisanych przez streaming:")
    df.printSchema()

    print("Wyniki batch odczytane z Parquet:")
    df.orderBy("window_start", "category").show(100, truncate=False)

    print("Porównanie: suma wartości zapisanych okien według kategorii:")
    df.groupBy("category").sum("total_amount").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
