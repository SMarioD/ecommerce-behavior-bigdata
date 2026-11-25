"""silver/sessions -> gold/user_features (un rând pe utilizator).

Trăsături RFM:
  recency_days   zile de la ultima sesiune până la sfârșitul datelor
  n_purchases    frecvența (numărul de produse cumpărate)
  revenue        valoarea monetară (suma cumpărăturilor)
Trăsături de comportament:
  n_sessions, cart_session_rate (ponderea sesiunilor care ajung la coș),
  avg_events_per_session, avg_duration_sec, avg_categories, avg_view_price

Se pornește din silver/sessions (deja agregat pe sesiune), fără sesiunile outlier.

Rulare (din container, după build_sessions):
    python -m src.analysis.user_features
    python -m src.analysis.user_features --sample
"""
import argparse
import time

from pyspark.sql import functions as F

from src.analysis.paths import resolve, write_small
from src.common.spark import get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    spark = get_spark("user_features")
    spark.sparkContext.setLogLevel("ERROR")
    start = time.time()
    _, ses_path, out = resolve(args.sample)

    sessions = spark.read.parquet(str(ses_path)).where(~F.col("is_outlier"))
    end_of_data = sessions.agg(F.max("session_end").cast("long")).first()[0]

    users = (
        sessions.groupBy("user_id").agg(
            F.count("*").alias("n_sessions"),
            F.sum("n_events").alias("n_events"),
            F.sum("n_views").alias("n_views"),
            F.sum("n_carts").alias("n_carts"),
            F.sum("n_purchases").alias("n_purchases"),
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.sum(F.col("reached_cart").cast("int")).alias("cart_sessions"),
            F.round(F.avg("duration_sec"), 1).alias("avg_duration_sec"),
            F.round(F.avg("n_events"), 2).alias("avg_events_per_session"),
            F.round(F.avg("n_categories"), 2).alias("avg_categories"),
            F.round(F.avg("avg_view_price"), 2).alias("avg_view_price"),
            F.max("session_end").cast("long").alias("last_seen"),
        )
        .withColumn("recency_days", F.round((F.lit(end_of_data) - F.col("last_seen")) / 86400, 3))
        .withColumn("cart_session_rate", F.round(F.col("cart_sessions") / F.col("n_sessions"), 4))
        .na.fill({"avg_view_price": 0.0})
        .select("user_id", "recency_days", "n_sessions", "n_purchases", "revenue",
                "cart_session_rate", "avg_events_per_session", "avg_duration_sec",
                "avg_categories", "avg_view_price", "n_events", "n_views", "n_carts")
    )

    n = write_small(users, out / "user_features")
    buyers = spark.read.parquet(str(out / "user_features")).where("n_purchases > 0").count()
    print(f"\nuser_features: {n:,} utilizatori, din care {buyers:,} au cumpărat "
          f"({buyers / n:.1%}) — scris în {time.time() - start:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
