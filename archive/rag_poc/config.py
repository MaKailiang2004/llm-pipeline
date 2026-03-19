"""Centralized environment-based configuration for the RAG PoC.

This module defines strongly-typed settings objects used by all pipeline modules.
Every runtime option is loaded from `.env` compatible environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


def _get_env(name: str, default: Optional[str] = None) -> str:
    """Return a required environment variable with optional fallback.

    Args:
        name: Environment variable key.
        default: Optional fallback value if the variable is not set.

    Returns:
        The resolved environment variable value.

    Raises:
        ValueError: If the variable is missing and no default is provided.
    """
    value: Optional[str] = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_int(name: str, default: int) -> int:
    """Return an integer environment variable.

    Args:
        name: Environment variable key.
        default: Fallback integer value.

    Returns:
        Parsed integer value from environment.
    """
    return int(_get_env(name, str(default)))


def _get_float(name: str, default: float) -> float:
    """Return a float environment variable.

    Args:
        name: Environment variable key.
        default: Fallback float value.

    Returns:
        Parsed float value from environment.
    """
    return float(_get_env(name, str(default)))


@dataclass(frozen=True)
class AppConfig:
    """Application-level settings for logging and local input sources."""

    log_level: str
    input_jsonl_path: str


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka producer and consumer settings."""

    bootstrap_servers: str
    raw_topic: str
    label_topic: str
    group_id: str


@dataclass(frozen=True)
class HdfsConfig:
    """HDFS endpoint and storage layout configuration."""

    host: str
    port: int
    base_path: str
    raw_path: str
    processed_path: str
    label_path: str

    @property
    def uri(self) -> str:
        """Return the HDFS endpoint URI."""
        return f"hdfs://{self.host}:{self.port}"


@dataclass(frozen=True)
class SparkConfig:
    """Spark runtime settings for streaming and batch jobs."""

    master: str
    app_name: str
    batch_interval_seconds: int
    checkpoint_dir: str


@dataclass(frozen=True)
class RayConfig:
    """Ray cluster and distributed inference settings."""

    address: str
    namespace: str
    num_workers: int
    inference_batch_size: int
    read_batch_size: int


@dataclass(frozen=True)
class VllmConfig:
    """vLLM OpenAI-compatible API settings used by Ray workers."""

    base_url: str
    model_name: str
    max_tokens: int
    temperature: float
    top_p: float
    request_timeout_seconds: int


@dataclass(frozen=True)
class Settings:
    """Top-level settings object composed of all subsystem configurations."""

    app: AppConfig
    kafka: KafkaConfig
    hdfs: HdfsConfig
    spark: SparkConfig
    ray: RayConfig
    vllm: VllmConfig


def load_settings(dotenv_path: str = ".env") -> Settings:
    """Load all runtime settings from environment variables.

    Args:
        dotenv_path: Path to a dotenv file. The file is loaded when present.

    Returns:
        A fully populated `Settings` object.
    """
    load_dotenv(dotenv_path=dotenv_path, override=False)

    app = AppConfig(
        log_level=_get_env("LOG_LEVEL", "INFO"),
        input_jsonl_path=_get_env("INPUT_JSONL_PATH", "./data/input_documents.jsonl"),
    )
    kafka = KafkaConfig(
        bootstrap_servers=_get_env("KAFKA_BOOTSTRAP_SERVERS"),
        raw_topic=_get_env("KAFKA_RAW_TOPIC"),
        label_topic=_get_env("KAFKA_LABEL_TOPIC"),
        group_id=_get_env("KAFKA_GROUP_ID", "rag_poc_group"),
    )
    hdfs = HdfsConfig(
        host=_get_env("HDFS_HOST", "namenode"),
        port=_get_int("HDFS_PORT", 9000),
        base_path=_get_env("HDFS_BASE_PATH", "/data/rag_poc"),
        raw_path=_get_env("HDFS_RAW_PATH", "/data/rag_poc/raw"),
        processed_path=_get_env("HDFS_PROCESSED_PATH", "/data/rag_poc/processed"),
        label_path=_get_env("HDFS_LABEL_PATH", "/data/rag_poc/labels"),
    )
    spark = SparkConfig(
        master=_get_env("SPARK_MASTER", "spark://spark-master:7077"),
        app_name=_get_env("SPARK_APP_NAME", "rag-kafka-to-hdfs"),
        batch_interval_seconds=_get_int("SPARK_BATCH_INTERVAL_SECONDS", 30),
        checkpoint_dir=_get_env("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/rag-poc"),
    )
    ray_settings = RayConfig(
        address=_get_env("RAY_ADDRESS", "ray://ray-head:10001"),
        namespace=_get_env("RAY_NAMESPACE", "rag-labeling"),
        num_workers=_get_int("RAY_NUM_WORKERS", 2),
        inference_batch_size=_get_int("RAY_INFERENCE_BATCH_SIZE", 8),
        read_batch_size=_get_int("RAY_READ_BATCH_SIZE", 128),
    )
    vllm = VllmConfig(
        base_url=_get_env("VLLM_BASE_URL", "http://vllm:8000/v1"),
        model_name=_get_env("VLLM_MODEL_NAME"),
        max_tokens=_get_int("VLLM_MAX_TOKENS", 256),
        temperature=_get_float("VLLM_TEMPERATURE", 0.0),
        top_p=_get_float("VLLM_TOP_P", 1.0),
        request_timeout_seconds=_get_int("VLLM_REQUEST_TIMEOUT_SECONDS", 120),
    )
    return Settings(
        app=app,
        kafka=kafka,
        hdfs=hdfs,
        spark=spark,
        ray=ray_settings,
        vllm=vllm,
    )
