"""silver/events -> silver/sessions (un rând pe sesiune).

Definiții:
  - reached_cart   = sesiunea are cart SAU purchase. În date, peste jumătate din
                     sesiunile cu cumpărare nu au eveniment cart, deci un funnel
                     pe evenimente brute ar ieși greșit.
  - is_outlier     = peste 200 de evenimente sau peste 24h durată. Sesiunile nu se
                     șterg, doar se marchează; analizele le exclud cu un filtru.
  - session_date   = data primului eveniment (cheia de partiționare).

Rulare (din container, după bronze_to_silver):
    python -m src.clean.build_sessions
"""
import time

from pyspark.sql import functions as F

from src.common.spark import SILVER, get_spark

MAX_EVENTS = 200
MAX_DURATION_SEC = 24 * 3600


def main() -> None:
    spark = get_spark("build_sessions")
    spark.sparkContext.setLogLevel("ERROR")
    start = time.time()

    ev = spark.read.parquet(str(SILVER / "events"))
    is_type = lambda t: (F.col("event_type") == t).cast("int")  # noqa: E731

    s = ev.groupBy("session_id").agg(
        F.first("user_id").alias("user_id"),
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum(is_type("view")).alias("n_views"),
        F.sum(is_type("cart")).alias("n_carts"),
        F.sum(is_type("purchase")).alias("n_purchases"),
        F.countDistinct("product_id").alias("n_products"),
        F.countDistinct("category_id").alias("n_categories"),
        F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)), 2)
         .alias("revenue"),
        F.round(F.avg(F.when(F.col("event_type") == "view", F.col("price"))), 2)
         .alias("avg_view_price"),
    )

    sessions = (
        s.withColumn("duration_sec",
                     F.col("session_end").cast("long") - F.col("session_start").cast("long"))
        .withColumn("session_date", F.to_date("session_start"))
        .withColumn("has_view", F.col("n_views") > 0)
        .withColumn("has_cart", F.col("n_carts") > 0)
        .withColumn("has_purchase", F.col("n_purchases") > 0)
        .withColumn("reached_cart", F.col("has_cart") | F.col("has_purchase"))
        .withColumn("is_outlier",
                    (F.col("n_events") > MAX_EVENTS) | (F.col("duration_sec") > MAX_DURATION_SEC))
        .select(
            "session_id", "user_id", "session_date", "session_start", "session_end",
            "duration_sec", "n_events", "n_views", "n_carts", "n_purchases",
            "n_products", "n_categories", "revenue", "avg_view_price",
            "has_view", "has_cart", "has_purchase", "reached_cart", "is_outlier",
        )
    )

    out = str(SILVER / "sessions")
    sessions.repartition("session_date").write.mode("overwrite").partitionBy("session_date").parquet(out)

    r = spark.read.parquet(out).agg(
        F.count("*").alias("sesiuni"),
        F.sum(F.col("is_outlier").cast("int")).alias("outlier"),
        F.sum(F.col("has_view").cast("int")).alias("cu_view"),
        F.sum(F.col("reached_cart").cast("int")).alias("ajung_la_cart"),
        F.sum(F.col("has_purchase").cast("int")).alias("cu_purchase"),
        F.round(F.sum("revenue"), 2).alias("venit_total"),
    ).first()
    print(f"\nScris în {out} în {time.time() - start:.0f}s")
    for k, v in r.asDict().items():
        print(f"  {k:<14}{v:>18,}")
    spark.stop()


if __name__ == "__main__":
    main()
