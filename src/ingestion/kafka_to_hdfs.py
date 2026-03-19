"""使用 PySpark 结构化流将 Kafka 数据接入 HDFS Bronze 层。

从 Kafka 主题 `raw_reviews` 消费原始 JSON 字符串，不经清洗直接追加写入
HDFS `/data/bronze/`。微批间隔固定为 10 秒。

可靠性：启动重试、批写失败日志、checkpoint 流式容错。
"""

from __future__ import annotations

import logging
import os
import time
from typing import Dict

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

LOGGER = logging.getLogger(__name__)


# ==================== 【段落 1】日志与配置加载 ====================

def setup_logging() -> None:
    """配置日志并静音第三方库的嘈杂输出。"""
    from common.logging_config import setup_logging as _setup

    _setup()


def load_env() -> Dict[str, str]:
    """从环境变量加载运行时配置。

    Returns:
        包含 Kafka/HDFS 路径与重试控制的配置字典。
    """
    load_dotenv(override=False)
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port = os.getenv("HDFS_PORT", "9000")
    return {
        "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
        "app_name": os.getenv("SPARK_APP_NAME", "kafka-to-hdfs-bronze"),
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "topic": os.getenv("KAFKA_RAW_TOPIC", "raw_reviews"),
        "hdfs_bronze_path": os.getenv(
            "HDFS_BRONZE_PATH", f"hdfs://{hdfs_host}:{hdfs_port}/data/bronze/"
        ),
        "checkpoint_path": os.getenv(
            "SPARK_CHECKPOINT_DIR",
            "/tmp/spark-checkpoints/kafka-to-hdfs-bronze",
        ),
        "max_restarts": str(int(os.getenv("STREAM_MAX_RESTARTS", "5"))),
        "restart_backoff_seconds": str(
            float(os.getenv("STREAM_RESTART_BACKOFF_SECONDS", "10"))
        ),
    }


# ==================== 【段落 2】Spark 会话（含 Kafka 连接器） ====================

def build_spark_session(spark_master: str, app_name: str) -> SparkSession:
    """创建已配置 Kafka 连接器的 Spark 会话。

    Args:
        spark_master: Spark Master 地址。
        app_name: Spark UI 中显示的应用名。

    Returns:
        已配置的 SparkSession。
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .getOrCreate()
    )


# ==================== 【段落 3】核心：创建 Kafka 流式 DataFrame ====================

def create_stream_df(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    """从 Kafka 创建流式 DataFrame，value 列为原始 JSON 字符串。

    Args:
        spark: 当前 SparkSession。
        bootstrap_servers: Kafka 引导服务器列表。
        topic: Kafka 主题名。

    Returns:
        仅含 `value` 字符串列（原始 JSON）的 DataFrame。
    """
    # readStream 表示流式读取；format("kafka") 使用 Kafka 数据源
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # 从最早偏移量开始消费
        .load()
        .selectExpr("CAST(value AS STRING) AS value")  # 将二进制 value 转为字符串
    )


# ==================== 【段落 4】核心：流式写入 HDFS + 有界重试 ====================

def start_stream_with_retry(config: Dict[str, str]) -> None:
    """启动并维持流式查询，失败时在限定次数内重试。

    Args:
        config: 运行时配置字典。

    Raises:
        RuntimeError: 重试次数用尽仍无法启动或维持流时抛出。
    """
    max_restarts = int(config["max_restarts"])
    restart_backoff_seconds = float(config["restart_backoff_seconds"])

    for attempt in range(1, max_restarts + 1):
        spark = None
        try:
            LOGGER.info(
                "Starting stream attempt=%d/%d topic=%s bronze_path=%s",
                attempt,
                max_restarts,
                config["topic"],
                config["hdfs_bronze_path"],
            )
            spark = build_spark_session(
                spark_master=config["spark_master"],
                app_name=config["app_name"],
            )
            # 创建 Kafka 流 DataFrame，value 列为原始 JSON 字符串
            stream_df = create_stream_df(
                spark=spark,
                bootstrap_servers=config["bootstrap_servers"],
                topic=config["topic"],
            )

            # 流式写入 HDFS：format("text") 按行写文本，checkpoint 保证故障恢复
            query = (
                stream_df.writeStream.format("text")
                .outputMode("append")
                .option("path", config["hdfs_bronze_path"])
                .option("checkpointLocation", config["checkpoint_path"])
                .trigger(processingTime="10 seconds")  # 每 10 秒触发一次微批
                .start()
            )
            query.awaitTermination()
            return
        except Exception as exc:
            LOGGER.exception(
                "Streaming execution error on attempt=%d/%d: %s",
                attempt,
                max_restarts,
                exc,
            )
            if spark is not None:
                spark.stop()
            if attempt == max_restarts:
                raise RuntimeError("重试次数用尽，无法启动或维持流。") from exc
            time.sleep(restart_backoff_seconds * attempt)


# ==================== 【段落 5】main 入口 ====================

def main() -> None:
    """Kafka 到 HDFS Bronze 流式任务的入口。"""
    setup_logging()
    config = load_env()
    start_stream_with_retry(config)


if __name__ == "__main__":
    main()
