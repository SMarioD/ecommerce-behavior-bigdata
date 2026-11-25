"""silver -> gold: KPI de business și clasamente.

Tabele scrise în gold:
  kpi_daily       un rând pe zi: evenimente, utilizatori, sesiuni, venit, conversie, valoare medie
  funnel_daily    sesiuni pe zi: cu view -> ajung la cart -> cu purchase
  top_products    top 100 produse după venit
  top_brands      top 50 branduri după venit (fără "unknown")
  top_categories  venit și activitate pe category_l1 / category_l2
  hourly_activity evenimente pe zi a săptămânii, oră și tip de eveniment

Toate analizele exclud sesiunile marcate is_outlier în build_sessions.
Funnel-ul folosește reached_cart așa cum e definit acolo (cart SAU purchase).
Nu există id de comandă în date, deci "valoarea medie a comenzii" = venit / sesiuni cu cumpărare.

Rulare (din container, după build_sessions):
    python -m src.analysis.kpis
    python -m src.analysis.kpis --sample
"""
import argparse
import time

from pyspark.sql import functions as F

from src.analysis.paths import resolve, write_small
from src.common.spark import get_spark


def is_type(t: str):
    return (F.col("event_type") == t).cast("int")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true", help="folosește eșantionul de o zi")
    args = parser.parse_args()

    spark = get_spark("kpis")
    spark.sparkContext.setLogLevel("ERROR")
    start = time.time()
    ev_path, ses_path, out = resolve(args.sample)

    sessions = spark.read.parquet(str(ses_path)).where(~F.col("is_outlier"))
    events = (
        spark.read.parquet(str(ev_path))
        .join(sessions.select("session_id"), "session_id", "left_semi")
        .cache()
    )

    # --- KPI zilnic și funnel -------------------------------------------------------------
    daily_ev = events.groupBy("event_date").agg(
        F.count("*").alias("n_events"),
        F.countDistinct("user_id").alias("n_users"),
        F.sum(is_type("view")).alias("n_views"),
        F.sum(is_type("cart")).alias("n_carts"),
        F.sum(is_type("purchase")).alias("n_purchases"),
    )
    daily_ses = sessions.groupBy(F.col("session_date").alias("event_date")).agg(
        F.count("*").alias("n_sessions"),
        F.sum(F.col("has_view").cast("int")).alias("sessions_view"),
        F.sum(F.col("reached_cart").cast("int")).alias("sessions_cart"),
        F.sum(F.col("has_purchase").cast("int")).alias("sessions_purchase"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    )

    kpi_daily = (
        daily_ev.join(daily_ses, "event_date", "left")
        .withColumn("conversion_rate", F.round(F.col("sessions_purchase") / F.col("n_sessions"), 4))
        .withColumn("avg_order_value",
                    F.round(F.col("revenue") / F.greatest(F.col("sessions_purchase"), F.lit(1)), 2))
        .select("event_date", "n_events", "n_users", "n_sessions", "n_views", "n_carts",
                "n_purchases", "sessions_purchase", "revenue", "conversion_rate", "avg_order_value")
        .orderBy("event_date")
    )
    funnel_daily = (
        daily_ses.select("event_date", "n_sessions", "sessions_view", "sessions_cart", "sessions_purchase")
        .orderBy("event_date")
    )

    # --- clasamente -----------------------------------------------------------------------
    def rank_table(keys, top, exclude_unknown=None):
        df = events
        if exclude_unknown:
            df = df.where(F.col(exclude_unknown) != "unknown")
        t = df.groupBy(*keys).agg(
            F.sum(is_type("view")).alias("n_views"),
            F.sum(is_type("cart")).alias("n_carts"),
            F.sum(is_type("purchase")).alias("n_purchases"),
            F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)), 2)
             .alias("revenue"),
        ).withColumn("view_to_purchase", F.round(F.col("n_purchases") / F.greatest(F.col("n_views"), F.lit(1)), 4))
        t = t.orderBy(F.desc("revenue"))
        return t.limit(top) if top else t

    top_products = (
        events.groupBy("product_id").agg(
            F.first("category_code").alias("category_code"),
            F.first("brand").alias("brand"),
            F.sum(is_type("view")).alias("n_views"),
            F.sum(is_type("cart")).alias("n_carts"),
            F.sum(is_type("purchase")).alias("n_purchases"),
            F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)), 2)
             .alias("revenue"),
        )
        .withColumn("view_to_purchase", F.round(F.col("n_purchases") / F.greatest(F.col("n_views"), F.lit(1)), 4))
        .orderBy(F.desc("revenue")).limit(100)
    )
    top_brands = rank_table(["brand"], 50, exclude_unknown="brand")
    top_categories = rank_table(["category_l1", "category_l2"], None)

    hourly = (
        events.groupBy(
            F.dayofweek("event_ts").alias("weekday"),  # 1 = duminică ... 7 = sâmbătă
            F.hour("event_ts").alias("hour"),
            "event_type",
        ).count().withColumnRenamed("count", "n_events")
        .orderBy("weekday", "hour", "event_type")
    )

    print(f"\nScriu în {out}")
    for name, df in [("kpi_daily", kpi_daily), ("funnel_daily", funnel_daily),
                     ("top_products", top_products), ("top_brands", top_brands),
                     ("top_categories", top_categories), ("hourly_activity", hourly)]:
        print(f"  {name:<18}{write_small(df, out / name):>8,} rânduri")
    print(f"Gata în {time.time() - start:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
