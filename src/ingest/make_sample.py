"""Eșantion de o zi din silver, pentru dezvoltare rapidă (dashboard, notebook-uri).

Scrie aceleași scheme ca silver, fiecare într-un singur fișier:
    data/sample/events    <- silver/events,   o zi
    data/sample/sessions  <- silver/sessions, sesiunile începute în aceeași zi

Rulare (din container, după build_sessions):
    python -m src.ingest.make_sample                    # 2019-10-01
    python -m src.ingest.make_sample --date 2019-10-15
"""
import argparse

from pyspark.sql import functions as F

from src.common.spark import SAMPLE, SILVER, get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2019-10-01")
    args = parser.parse_args()

    spark = get_spark("make_sample")
    spark.sparkContext.setLogLevel("ERROR")

    for table, date_col in (("events", "event_date"), ("sessions", "session_date")):
        out = str(SAMPLE / table)
        (
            spark.read.parquet(str(SILVER / table))
            .where(F.col(date_col) == args.date)
            .coalesce(1)
            .write.mode("overwrite")
            .parquet(out)
        )
        print(f"sample/{table} ({args.date}): {spark.read.parquet(out).count():,} rânduri")
    spark.stop()


if __name__ == "__main__":
    main()
