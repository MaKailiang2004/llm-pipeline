"""Prepare a processed HDFS dataset from raw parquet records.

This batch Spark job deduplicates records by `doc_id`, trims empty content, and
writes standardized parquet output used by the Ray labeling stage.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, length, row_number, to_timestamp, trim
from pyspark.sql.window import Window

from rag_poc.config import load_settings
from rag_poc.logging_utils import setup_logging

LOGGER = logging.getLogger(__name__)


def _build_spark_session() -> SparkSession:
    """Create Spark session for batch preparation job."""
    settings = load_settings()
    return (
        SparkSession.builder.appName("rag-prepare-processed")
        .master(settings.spark.master)
        .getOrCreate()
    )


def _prepare_dataset(raw_df: DataFrame) -> DataFrame:
    """Clean and deduplicate raw documents.

    Args:
        raw_df: Input DataFrame loaded from raw HDFS parquet path.

    Returns:
        Processed DataFrame ready for auto-labeling.
    """
    filtered = raw_df.where(length(trim(col("content"))) > 0)
    with_ts = filtered.withColumn("event_ts", to_timestamp(col("timestamp")))
    ranking = Window.partitionBy("doc_id").orderBy(col("event_ts").desc_nulls_last())
    deduped = with_ts.withColumn("rn", row_number().over(ranking)).where(col("rn") == 1)
    return deduped.drop("rn", "event_ts")


def main() -> None:
    """Run batch processing job from raw HDFS path to processed path."""
    settings = load_settings()
    setup_logging(settings.app.log_level)

    spark = _build_spark_session()
    raw_path = f"{settings.hdfs.uri}{settings.hdfs.raw_path}"
    processed_path = f"{settings.hdfs.uri}{settings.hdfs.processed_path}"

    LOGGER.info("Reading raw dataset from: %s", raw_path)
    raw_df = spark.read.parquet(raw_path)
    processed_df = _prepare_dataset(raw_df)

    LOGGER.info("Writing processed dataset to: %s", processed_path)
    processed_df.write.mode("overwrite").parquet(processed_path)
    LOGGER.info("Processed dataset write completed.")


if __name__ == "__main__":
    main()
