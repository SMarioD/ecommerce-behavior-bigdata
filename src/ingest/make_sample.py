"""Eșantion de o zi din bronze, pentru dezvoltare rapidă (dashboard, notebook-uri).

Scrie aceeași schemă ca bronze, într-un singur folder: data/sample/events.

Rulare (din container, după csv_to_bronze):
    python -m src.ingest.make_sample                    # 2019-10-01
    python -m src.ingest.make_sample --date 2019-10-15
"""
import argparse

from pyspark.sql import functions as F

from src.common.spark import BRONZE, SAMPLE, get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2019-10-01")
    args = parser.parse_args()

    spark = get_spark("make_sample")
    out = str(SAMPLE / "events")

    df = spark.read.parquet(str(BRONZE / "events")).where(F.col("event_date") == args.date)
    df.coalesce(1).write.mode("overwrite").parquet(out)

    sample = spark.read.parquet(out)
    print(f"Eșantion {args.date}: {sample.count():,} rânduri în {out}")
    sample.groupBy("event_type").count().orderBy(F.desc("count")).show()
    spark.stop()


if __name__ == "__main__":
    main()
