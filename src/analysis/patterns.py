"""silver/events -> reguli de asociere cu FP-Growth (Spark MLlib).

Un "coș" = mulțimea elementelor distincte pe care utilizatorul le-a adăugat în coș sau le-a
cumpărat în aceeași sesiune. Elementul poate fi categoria (implicit) sau brandul.
Suportul e raportat la toate coșurile (inclusiv cele cu un singur element).

Tabele scrise în gold (X = category sau brand):
  assoc_rules_X     antecedent, consequent, support, confidence, lift (doar lift > 1)
  freq_itemsets_X   seturi frecvente cu cel puțin 2 elemente

Rulare (din container, după build_sessions):
    python -m src.analysis.patterns
    python -m src.analysis.patterns --level brand --min-support 0.001
    python -m src.analysis.patterns --sample --min-support 0.005
"""
import argparse
import time

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

from src.analysis.paths import resolve, write_small
from src.common.spark import get_spark

ITEM_COL = {"category": "category_code", "brand": "brand"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--level", choices=list(ITEM_COL), default="category")
    parser.add_argument("--min-support", type=float, default=0.002)
    parser.add_argument("--min-confidence", type=float, default=0.2)
    args = parser.parse_args()

    spark = get_spark("patterns")
    spark.sparkContext.setLogLevel("ERROR")
    start = time.time()
    ev_path, ses_path, out = resolve(args.sample)
    item = ITEM_COL[args.level]

    ok_sessions = spark.read.parquet(str(ses_path)).where(~F.col("is_outlier")).select("session_id")
    baskets = (
        spark.read.parquet(str(ev_path))
        .where(F.col("event_type").isin("cart", "purchase") & (F.col(item) != "unknown"))
        .join(ok_sessions, "session_id", "left_semi")
        .groupBy("session_id").agg(F.collect_set(item).alias("items"))
        .cache()
    )
    n_baskets = baskets.count()
    multi = baskets.where(F.size("items") >= 2).count()
    print(f"\nCoșuri: {n_baskets:,}, din care {multi:,} ({multi / max(n_baskets, 1):.1%}) cu cel puțin 2 elemente")

    model = FPGrowth(itemsCol="items", minSupport=args.min_support,
                     minConfidence=args.min_confidence).fit(baskets)

    rules = (
        model.associationRules.where(F.col("lift") > 1)
        .select(
            F.concat_ws(" + ", "antecedent").alias("antecedent"),
            F.concat_ws(" + ", "consequent").alias("consequent"),
            F.round("support", 5).alias("support"),
            F.round("confidence", 4).alias("confidence"),
            F.round("lift", 3).alias("lift"),
        )
        .orderBy(F.desc("lift")).limit(500)
    )
    itemsets = (
        model.freqItemsets.where(F.size("items") >= 2)
        .select(F.concat_ws(" + ", "items").alias("itemset"), F.size("items").alias("size"),
                F.col("freq").alias("n_baskets"), F.round(F.col("freq") / n_baskets, 5).alias("support"))
        .orderBy(F.desc("freq")).limit(200)
    )

    nr = write_small(rules, out / f"assoc_rules_{args.level}")
    ni = write_small(itemsets, out / f"freq_itemsets_{args.level}")
    print(f"{args.level}: {nr:,} reguli, {ni:,} seturi frecvente "
          f"(min_support={args.min_support}, min_confidence={args.min_confidence}) în {time.time() - start:.0f}s")
    spark.read.parquet(str(out / f"assoc_rules_{args.level}")).orderBy(F.desc("lift")).show(8, truncate=60)
    spark.stop()


if __name__ == "__main__":
    main()
