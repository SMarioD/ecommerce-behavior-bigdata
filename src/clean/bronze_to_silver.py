"""bronze/events -> silver/events (date curățate, tipizate).

Reguli de curățare:
  1. event_time (text "YYYY-MM-DD HH:MM:SS UTC") -> event_ts (timestamp UTC)
  2. se elimină rândurile fără user_session
  3. se elimină sesiunile atribuite mai multor utilizatori (ambigue)
  4. se elimină evenimentele cu price <= 0 (produs fără preț)
  5. se elimină duplicatele pe cheia evenimentului
     (event_time, event_type, product_id, user_id, user_session)
  6. brand și category_code lipsă -> "unknown"; category_code se împarte în
     category_l1 / category_l2

La final se afișează câte rânduri a eliminat fiecare regulă.

Rulare (din container):
    python -m src.clean.bronze_to_silver
"""
import time

from pyspark.sql import functions as F

from src.common.spark import BRONZE, SILVER, get_spark

EVENT_KEY = ["event_time", "event_type", "product_id", "user_id", "user_session"]


def main() -> None:
    spark = get_spark("bronze_to_silver")
    spark.sparkContext.setLogLevel("ERROR")
    start = time.time()

    bronze = spark.read.parquet(str(BRONZE / "events"))
    report = [("bronze", bronze.count())]

    df = bronze.where(F.col("user_session").isNotNull())
    report.append(("fără user_session", df.count()))

    multi_user = (
        df.groupBy("user_session").agg(F.countDistinct("user_id").alias("n"))
        .where("n > 1").select("user_session")
    )
    df = df.join(F.broadcast(multi_user), "user_session", "left_anti")
    report.append(("sesiuni cu mai mulți utilizatori", df.count()))

    df = df.where(F.col("price") > 0)
    report.append(("price <= 0", df.count()))

    df = df.dropDuplicates(EVENT_KEY)
    report.append(("duplicate", df.count()))

    cat = F.split(F.col("category_code"), r"\.")
    silver = df.select(
        F.to_timestamp(F.substring("event_time", 1, 19)).alias("event_ts"),
        F.col("event_date").cast("date").alias("event_date"),
        "event_type",
        "product_id",
        "category_id",
        F.coalesce("category_code", F.lit("unknown")).alias("category_code"),
        F.coalesce(cat.getItem(0), F.lit("unknown")).alias("category_l1"),
        F.coalesce(cat.getItem(1), F.lit("unknown")).alias("category_l2"),
        F.coalesce("brand", F.lit("unknown")).alias("brand"),
        "price",
        "user_id",
        F.col("user_session").alias("session_id"),
    )

    out = str(SILVER / "events")
    (
        silver.repartition("event_date")
        .sortWithinPartitions("session_id", "event_ts")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(out)
    )

    final = spark.read.parquet(out).count()
    print(f"\nScris în {out} în {time.time() - start:.0f}s\n")
    print(f"{'Pas':<36}{'rânduri rămase':>16}{'eliminate':>12}")
    prev = None
    for step, n in report:
        removed = "" if prev is None else f"{prev - n:,}"
        print(f"{step:<36}{n:>16,}{removed:>12}")
        prev = n
    print(f"{'silver/events (verificare)':<36}{final:>16,}")
    spark.stop()


if __name__ == "__main__":
    main()
