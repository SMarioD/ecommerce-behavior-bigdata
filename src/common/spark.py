"""Crearea sesiunii Spark și căile comune ale proiectului."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(os.environ.get("PROJECT_ROOT", "/workspace"))
DATA = ROOT / "data"
RAW = DATA / "raw" / "rees46"
SAMPLE = DATA / "sample"
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"


def get_spark(app_name: str) -> SparkSession:
    """Sesiune Spark locală; memoria driverului vine din SPARK_DRIVER_MEMORY."""
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "10g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
