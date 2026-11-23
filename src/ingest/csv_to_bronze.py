"""CSV REES46 -> bronze (Parquet partiționat pe zi).

Bronze păstrează datele așa cum vin, fără curățare. Se adaugă doar:
  - event_date   (cheia de partiționare, din primele 10 caractere ale event_time)
  - source_file  (din ce CSV vine rândul)
  - ingested_at  (când a fost încărcat)

Rulare (din container):
    python -m src.ingest.csv_to_bronze                  # 2019-Oct.csv
    python -m src.ingest.csv_to_bronze --files 2019-Oct.csv 2019-Nov.csv
"""
import argparse
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

from src.common.spark import BRONZE, RAW, get_spark

# Schemă explicită: fără inferSchema, care ar citi tot fișierul de două ori.
RAW_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="+", default=["2019-Oct.csv"])
    args = parser.parse_args()

    spark = get_spark("csv_to_bronze")
    paths = [str(RAW / f) for f in args.files]
    out = str(BRONZE / "events")

    start = time.time()
    df = (
        spark.read.csv(paths, header=True, schema=RAW_SCHEMA, mode="PERMISSIVE")
        .withColumn("event_date", F.substring("event_time", 1, 10))
        .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("ingested_at", F.current_timestamp())
    )

    (
        df.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(out)
    )

    bronze = spark.read.parquet(out)
    print(f"Scris în {out} în {time.time() - start:.0f}s")
    print(f"Rânduri în bronze: {bronze.count():,}")
    bronze.groupBy("source_file").count().orderBy("source_file").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
