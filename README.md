# Apache Spark Structured Streaming

## Cel ćwiczenia

Celem laboratorium było przygotowanie aplikacji PySpark wykorzystującej Apache Spark Structured Streaming. W projekcie wczytuję dane strumieniowo z plików CSV dodawanych do folderu wejściowego, wykonuję czyszczenie danych, transformacje, agregacje według kategorii, agregacje w oknach czasowych, watermarking oraz zapis wyników do plików Parquet z checkpointingiem.


## Uruchomienie

Najpierw instaluję zależności:

```bash
pip install -r requirements.txt
```

W pierwszym terminalu uruchamiam aplikację Spark Structured Streaming:

```bash
python structured_streaming.py
```

W drugim terminalu uruchamiam generator danych:

```bash
python generate_stream_files.py
```

Generator automatycznie dodaje kolejne pliki CSV do folderu `data/input_stream`. Aplikacja Spark działa cały czas i przetwarza nowe pliki bez restartu programu.

Po zatrzymaniu aplikacji mogę sprawdzić zapisane wyniki Parquet:

```bash
python check_output.py
```

Zatrzymanie aplikacji streamingowej wykonuję przez `Ctrl+C`.