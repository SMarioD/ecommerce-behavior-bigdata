"""gold/user_features -> segmente de clienți cu K-Means (Spark MLlib).

Pași: log1p pe trăsăturile asimetrice -> standardizare -> K-Means -> profil pe segment.
Denumirile segmentelor se atribuie automat din profil (regulile sunt în name_segments);
se verifică la final cu ochii și, dacă e cazul, se ajustează.

Tabele scrise în gold:
  user_segments    user_id, segment_id, segment
  segment_profile  un rând pe segment: mărime și medii ale trăsăturilor (în unități originale)
  segments_pca     eșantion de ~8000 utilizatori proiectat în 2D (pentru grafic)
  kmeans_tuning    silhouette și cost pentru k = 2..8 (doar cu --tune)

Rulare (din container, după user_features):
    python -m src.analysis.segments --k 5
    python -m src.analysis.segments --tune
    python -m src.analysis.segments --sample --k 4
"""
import argparse
import time

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.analysis.paths import resolve, write_small
from src.common.spark import get_spark

LOG_FEATURES = ["n_sessions", "n_purchases", "revenue", "avg_events_per_session",
                "avg_duration_sec", "avg_categories", "avg_view_price"]
LIN_FEATURES = ["recency_days", "cart_session_rate"]
PROFILE_COLS = ["recency_days", "n_sessions", "n_purchases", "revenue", "cart_session_rate",
                "avg_events_per_session", "avg_duration_sec", "avg_categories", "avg_view_price"]


def name_segments(profile) -> dict:
    """Nume pentru fiecare segment, în funcție de profil (profile = DataFrame pandas)."""
    names, left = {}, set(profile["segment_id"])
    p = profile.set_index("segment_id")

    def take(col, name, cond=None):
        cand = [i for i in left if cond is None or cond(p.loc[i])]
        if cand:
            best = max(cand, key=lambda i: p.loc[i, col])
            names[best] = name
            left.discard(best)

    take("revenue", "Clienți valoroși", lambda r: r["buyer_share"] >= 0.5)
    take("revenue", "Cumpărători ocazionali", lambda r: r["buyer_share"] >= 0.5)
    take("cart_session_rate", "Abandonatori de coș")
    take("avg_events_per_session", "Explorători")
    for i in sorted(left):
        names[i] = "Vizitatori ocazionali"
    # nume duplicate -> se adaugă numărul segmentului
    seen = {}
    for i, n in names.items():
        seen.setdefault(n, []).append(i)
    for n, ids in seen.items():
        if len(ids) > 1:
            for i in ids:
                names[i] = f"{n} ({i})"
    return names


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--tune", action="store_true", help="calculează silhouette pentru k=2..8")
    args = parser.parse_args()

    spark = get_spark("segments")
    spark.sparkContext.setLogLevel("ERROR")
    start = time.time()
    _, _, out = resolve(args.sample)

    users = spark.read.parquet(str(out / "user_features"))
    prepared = users
    for c in LOG_FEATURES:
        prepared = prepared.withColumn(f"f_{c}", F.log1p(F.col(c)))
    for c in LIN_FEATURES:
        prepared = prepared.withColumn(f"f_{c}", F.col(c))
    cols = [f"f_{c}" for c in LOG_FEATURES + LIN_FEATURES]

    prep = Pipeline(stages=[
        VectorAssembler(inputCols=cols, outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="scaled", withMean=True, withStd=True),
    ]).fit(prepared)
    data = prep.transform(prepared).cache()
    evaluator = ClusteringEvaluator(featuresCol="scaled", predictionCol="segment_id")

    if args.tune:
        eval_df = data.sample(False, min(1.0, 200_000 / data.count()), seed=42)
        rows = []
        for k in range(2, 9):
            m = KMeans(k=k, seed=42, maxIter=40, featuresCol="scaled", predictionCol="segment_id").fit(data)
            sil = evaluator.evaluate(m.transform(eval_df))
            rows.append((k, float(sil), float(m.summary.trainingCost)))
            print(f"  k={k}  silhouette={sil:.3f}  cost={m.summary.trainingCost:,.0f}")
        tuning = spark.createDataFrame(rows, ["k", "silhouette", "cost"])
        write_small(tuning, out / "kmeans_tuning")

    model = KMeans(k=args.k, seed=42, maxIter=60, featuresCol="scaled", predictionCol="segment_id").fit(data)
    scored = model.transform(data)

    profile = (
        scored.groupBy("segment_id").agg(
            F.count("*").alias("n_users"),
            F.round(F.avg((F.col("n_purchases") > 0).cast("int")), 4).alias("buyer_share"),
            *[F.round(F.avg(c), 3).alias(c) for c in PROFILE_COLS],
        ).orderBy("segment_id")
    )
    prof_pd = profile.toPandas()
    names = name_segments(prof_pd)
    total = int(prof_pd["n_users"].sum())
    prof_pd["segment"] = prof_pd["segment_id"].map(names)
    prof_pd["share"] = (prof_pd["n_users"] / total).round(4)
    name_df = spark.createDataFrame(prof_pd[["segment_id", "segment"]])

    segs = scored.select("user_id", "segment_id").join(name_df, "segment_id")
    write_small(segs, out / "user_segments")
    write_small(spark.createDataFrame(prof_pd), out / "segment_profile")

    pca = PCA(k=2, inputCol="scaled", outputCol="pca").fit(scored)
    frac = min(1.0, 8000 / total)
    proj = (
        pca.transform(scored.sample(False, frac, seed=7))
        .withColumn("xy", vector_to_array("pca"))
        .select(F.col("xy")[0].alias("x"), F.col("xy")[1].alias("y"), "segment_id")
        .join(name_df, "segment_id")
    )
    write_small(proj, out / "segments_pca")

    sil = evaluator.evaluate(scored.sample(False, min(1.0, 200_000 / total), seed=42))
    print(f"\nK-Means k={args.k}: silhouette={sil:.3f}, {total:,} utilizatori, {time.time() - start:.0f}s\n")
    show = prof_pd.sort_values("n_users", ascending=False)
    print(show[["segment", "n_users", "share", "buyer_share", "revenue", "recency_days",
                "n_sessions", "cart_session_rate"]].to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
