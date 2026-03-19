"""Stream documents from Kafka to HDFS using PySpark Structured Streaming.

The job subscribes to the raw Kafka topic, parses JSON payloads into columns,
and writes them as parquet files into the configured HDFS path.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

from rag_poc.config import load_settings
from rag_poc.logging_utils import setup_logging

LOGGER = logging.getLogger(__name__)


def build_schema() -> StructType:
    """Return the expected schema for Kafka JSON document payloads."""
    return StructType(
        [
            StructField("doc_id", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("content", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
        ]
    )


def build_spark_session() -> SparkSession:
    """Create and configure a Spark session for Kafka/HDFS integration."""
    settings = load_settings()
    return (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Create a streaming DataFrame from Kafka raw topic.

    Args:
        spark: Active Spark session.

    Returns:
        Kafka stream DataFrame with JSON `value` payload.
    """
    settings = load_settings()
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.raw_topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def transform_kafka_payload(kafka_df: DataFrame) -> DataFrame:
    """Transform Kafka byte payload into structured document rows.

    Args:
        kafka_df: Source Kafka DataFrame from Structured Streaming.

    Returns:
        Parsed document DataFrame.
    """
    schema = build_schema()
    return (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("record"))
        .select("record.*")
    )


def main() -> None:
    """Run the Kafka to HDFS streaming sink."""
    settings = load_settings()
    setup_logging(settings.app.log_level)

    spark = build_spark_session()
    LOGGER.info("Spark session created for app=%s", settings.spark.app_name)

    kafka_df = read_kafka_stream(spark)
    parsed_df = transform_kafka_payload(kafka_df)

    output_path = f"{settings.hdfs.uri}{settings.hdfs.raw_path}"
    checkpoint_path = settings.spark.checkpoint_dir
    LOGGER.info("Writing stream to HDFS path: %s", output_path)

    query = (
        parsed_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{settings.spark.batch_interval_seconds} seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
