"""原始商品评论的异步 Kafka 生产者。

以固定吞吐（默认 50 条/秒）向 Kafka 推送评论事件。输入为 JSONL，
每行需包含 `id`、`text`、`timestamp`。

特性：按速率异步发送、失败重试、投递回调与错误日志、
本地无文件时可选从 URL 下载数据集。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import httpx
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)


# ==================== 【段落 1】日志与配置 ====================

def setup_logging() -> None:
    """配置日志并静音第三方库的嘈杂输出。"""
    from common.logging_config import setup_logging as _setup

    _setup()


def _load_env() -> Dict[str, str]:
    """从 `.env` 加载运行时配置并返回规范化后的字典。

    Returns:
        由环境变量得到的生产者配置映射。
    """
    load_dotenv(override=False)
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic": os.getenv("KAFKA_RAW_TOPIC", "raw_reviews"),
        "dataset_path": os.getenv("REVIEW_DATASET_PATH", "./data/reviews.jsonl"),
        "dataset_url": os.getenv("REVIEW_DATASET_URL", ""),
        "rate_per_second": str(int(os.getenv("PRODUCER_RATE_PER_SECOND", "50"))),
        "max_retries": str(int(os.getenv("PRODUCER_MAX_RETRIES", "3"))),
        "retry_backoff_seconds": str(
            float(os.getenv("PRODUCER_RETRY_BACKOFF_SECONDS", "1.0"))
        ),
    }


# ==================== 【段落 2】数据集下载（可选） ====================

def _download_dataset_if_needed(dataset_path: Path, dataset_url: str) -> None:
    """本地无文件且配置了 URL 时，从网络下载 JSONL 数据集。

    Args:
        dataset_path: 期望的 JSONL 本地路径。
        dataset_url: 可公网访问的 JSONL 下载地址。
    """
    if dataset_path.exists() or not dataset_url:
        return

    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Local dataset not found, downloading from: %s", dataset_url)
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(dataset_url)
        response.raise_for_status()
        dataset_path.write_bytes(response.content)
    LOGGER.info("Dataset downloaded to: %s", dataset_path)


# ==================== 【段落 3】核心：从 JSONL 逐行读取并校验 ====================

def _iter_reviews(dataset_path: Path) -> Iterator[Dict[str, Any]]:
    """从 JSONL 文件逐行读取并校验，yield 评论记录。

    Args:
        dataset_path: JSONL 文件路径。

    Yields:
        含 `id`、`text`、`timestamp` 的评论字典。

    Raises:
        ValueError: 某行非合法 JSON 或缺少必选字段时抛出。
    """
    required_keys = {"id", "text", "timestamp"}
    with dataset_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            payload = line.strip()
            if not payload:
                continue
            try:
                record: Dict[str, Any] = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON at line {line_number} in {dataset_path}: {exc}"
                ) from exc

            missing_keys = required_keys.difference(record.keys())
            if missing_keys:
                raise ValueError(
                    f"Missing keys at line {line_number}: {sorted(missing_keys)}"
                )
            # 只 yield 必选三列，便于下游 Kafka 与 ETL 一致
            yield {
                "id": str(record["id"]),
                "text": str(record["text"]),
                "timestamp": str(record["timestamp"]),
            }


# ==================== 【段落 4】Kafka 投递回调与带重试的 produce ====================

def _delivery_report(err: Optional[Exception], msg: Any) -> None:
    """处理 Kafka 生产者的异步投递回调。

    Args:
        err: 投递失败时的异常，成功则为 `None`。
        msg: confluent-kafka 提供的投递元数据对象。
    """
    if err is not None:
        LOGGER.error("Delivery failed for key=%s: %s", msg.key(), err)


def _produce_with_retry(
    producer: Producer,
    topic: str,
    record: Dict[str, Any],
    max_retries: int,
    retry_backoff_seconds: float,
) -> bool:
    """在限定重试次数内尝试将一条记录发送到 Kafka。

    Args:
        producer: 已配置的 Kafka 生产者。
        topic: 目标主题。
        record: 可 JSON 序列化的评论记录。
        max_retries: 放弃前的最大重试次数。
        retry_backoff_seconds: 重试间隔基数（秒）。

    Returns:
        发送成功为 True，否则 False。
    """
    for attempt in range(1, max_retries + 1):
        try:
            # 异步发送：key 为 id，value 为 JSON 字符串，on_delivery 为回调
            producer.produce(
                topic=topic,
                key=record["id"],
                value=json.dumps(record, ensure_ascii=False),
                on_delivery=_delivery_report,
            )
            producer.poll(0)  # 触发回调处理
            return True
        except BufferError as exc:
            LOGGER.warning(
                "Producer queue full (attempt=%d/%d): %s",
                attempt,
                max_retries,
                exc,
            )
            producer.poll(0.5)  # 等待队列腾出空间
        except KafkaException as exc:
            LOGGER.warning(
                "Kafka produce error (attempt=%d/%d): %s",
                attempt,
                max_retries,
                exc,
            )
        except Exception as exc:
            LOGGER.exception(
                "Unexpected produce error (attempt=%d/%d): %s",
                attempt,
                max_retries,
                exc,
            )

        sleep_seconds = retry_backoff_seconds * attempt
        time.sleep(sleep_seconds)

    return False


# ==================== 【段落 5】核心：按固定速率异步生产 ====================

async def produce_at_fixed_rate() -> None:
    """按配置的吞吐率运行异步生产者循环。"""
    env = _load_env()
    dataset_path = Path(env["dataset_path"])
    _download_dataset_if_needed(dataset_path=dataset_path, dataset_url=env["dataset_url"])
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset file not found: {dataset_path}. Set REVIEW_DATASET_PATH or REVIEW_DATASET_URL."
        )

    topic = env["topic"]
    rate_per_second = int(env["rate_per_second"])
    max_retries = int(env["max_retries"])
    retry_backoff_seconds = float(env["retry_backoff_seconds"])
    producer = Producer({"bootstrap.servers": env["bootstrap_servers"]})

    all_records: List[Dict[str, Any]] = list(_iter_reviews(dataset_path))
    if not all_records:
        LOGGER.warning("No records found in dataset: %s", dataset_path)
        return

    LOGGER.info(
        "Starting producer: records=%d topic=%s rate=%d/s",
        len(all_records),
        topic,
        rate_per_second,
    )

    sent_count = 0
    failed_count = 0
    index = 0
    while index < len(all_records):
        # 每轮最多发送 rate_per_second 条，实现固定吞吐
        batch = all_records[index : index + rate_per_second]
        tick_start = time.monotonic()

        for record in batch:
            ok = _produce_with_retry(
                producer=producer,
                topic=topic,
                record=record,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            if ok:
                sent_count += 1
            else:
                failed_count += 1
                LOGGER.error("Drop record after retries, id=%s", record["id"])

        index += len(batch)
        # 若本轮不足 1 秒，sleep 补足，使整体速率约等于 rate_per_second/s
        elapsed = time.monotonic() - tick_start
        wait_seconds = max(0.0, 1.0 - elapsed)
        await asyncio.sleep(wait_seconds)

    producer.flush(10.0)  # 等待未完成的发送，最多 10 秒
    LOGGER.info("Producer finished. sent=%d failed=%d", sent_count, failed_count)


# ==================== 【段落 6】main 入口 ====================

def main() -> None:
    """命令行入口。"""
    setup_logging()
    try:
        asyncio.run(produce_at_fixed_rate())
    except Exception as exc:
        LOGGER.exception("Producer stopped with fatal error: %s", exc)
        raise


if __name__ == "__main__":
    main()
