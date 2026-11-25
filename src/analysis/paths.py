"""Căi de intrare și de ieșire comune pentru scripturile din src/analysis.

Cu --sample se citește eșantionul de o zi (data/sample) și rezultatele se scriu în
data/sample/gold, ca să meargă totul rapid pe laptop. Fără --sample se citește silver
și se scrie în data/gold.
"""
from src.common.spark import GOLD, SAMPLE, SILVER


def resolve(sample: bool):
    """Întoarce (events, sessions, ieșire gold)."""
    if sample:
        return SAMPLE / "events", SAMPLE / "sessions", SAMPLE / "gold"
    return SILVER / "events", SILVER / "sessions", GOLD


def write_small(df, path) -> int:
    """Scrie un tabel mic de rezultate într-un singur fișier Parquet."""
    df.coalesce(1).write.mode("overwrite").parquet(str(path))
    return df.sparkSession.read.parquet(str(path)).count()
