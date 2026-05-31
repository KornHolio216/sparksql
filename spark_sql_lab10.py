import os

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" +
    os.environ["HADOOP_HOME"] + r"\bin;" +
    os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SparkSQL_Parquet") \
    .getOrCreate()


df_csv = spark.read.csv(
    "data/sales.csv",
    header=True,
    inferSchema=True
)

print("Dane CSV zostały wczytane do DataFrame:")
df_csv.show()

df_csv = df_csv.withColumn("wartosc", col("ilosc") * col("cena"))

df_csv.createOrReplaceTempView("sprzedaz")

print("DataFrame został zarejestrowany jako widok tymczasowy: sprzedaz")

print("Proste zapytanie SQL")

spark.sql("""
    SELECT *
    FROM sprzedaz
    LIMIT 10
""").show()

df_csv.write.mode("overwrite").parquet("output/sales_parquet")

print("Plik Parquet został przygotowany i zapisany w folderze output/sales_parquet")

df_parquet = spark.read.parquet("output/sales_parquet")

print("Dane zostały wczytane z pliku Parquet do DataFrame")

print("Kilka pierwszych wierszy danych z Parquet:")
df_parquet.show()

print("Schemat danych z Parquet:")
df_parquet.printSchema()

df_parquet.createOrReplaceTempView("sprzedaz_parquet")

print("DataFrame zostal zarejestrowany jako widok: sprzedaz_parquet")

print("ZAPYTANIA SQL")

print("1.Agregacje: COUNT, SUM, AVG")
wynik_agregacje = spark.sql("""
    SELECT
        COUNT(*) AS liczba_rekordow,
        SUM(wartosc) AS suma_sprzedazy,
        AVG(wartosc) AS srednia_wartosc_zamowienia
    FROM sprzedaz_parquet
""")
wynik_agregacje.show()

print("2.Grupowanie po mieście i produkcie")
wynik_grupowanie = spark.sql("""
    SELECT
        miasto,
        produkt,
        SUM(ilosc) AS suma_sztuk,
        SUM(wartosc) AS suma_sprzedazy
    FROM sprzedaz_parquet
    GROUP BY miasto, produkt
    ORDER BY miasto, suma_sprzedazy DESC
""")
wynik_grupowanie.show()

print("3.Filtrowanie warunkowe WHERE wartosc > 1000")
wynik_filtrowanie = spark.sql("""
    SELECT
        id,
        data,
        miasto,
        produkt,
        ilosc,
        cena,
        wartosc
    FROM sprzedaz_parquet
    WHERE wartosc > 1000
    ORDER BY wartosc DESC
""")
wynik_filtrowanie.show()

print("4.JOIN dwóch widoków")

df_products = spark.read.csv(
    "data/products.csv",
    header=True,
    inferSchema=True
)

df_products.createOrReplaceTempView("produkty")

wynik_join = spark.sql("""
    SELECT
        s.id,
        s.data,
        s.miasto,
        s.produkt,
        s.kategoria,
        s.ilosc,
        s.cena,
        s.wartosc,
        p.dostawca,
        p.marza_procent
    FROM sprzedaz_parquet s
    JOIN produkty p
        ON s.produkt = p.produkt
    ORDER BY s.id
""")
wynik_join.show()

print("Zapis wyniku JOIN do pliku CSV i Parquet")

wynik_join.write.mode("overwrite").option("header", True).csv("output/wynik_join_csv")
wynik_join.write.mode("overwrite").parquet("output/wynik_join_parquet")

print("Wynik zapytania został zapisany w folderach:")
print("output/wynik_join_csv")
print("output/wynik_join_parquet")

spark.stop()