"""Produce raw RAG documents from JSONL into Kafka.

This module is intended as the ingestion entrypoint for the PoC. It reads line
delimited JSON documents, validates required fields, and publishes messages to
the configured Kafka raw topic.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable, Iterator

from confluent_kafka import Producer

from rag_poc.config import load_settings
from rag_poc.logging_utils import setup_logging
from rag_poc.schemas import RawDocument

LOGGER = logging.getLogger(__name__)


def _delivery_callback(err: object, msg: object) -> None:
    """Handle Kafka delivery reports.

    Args:
        err: Delivery error object, if any.
        msg: Kafka message metadata object.
    """
    if err is not None:
        LOGGER.error("Kafka delivery failed: %s", err)


def read_jsonl_documents(path: str) -> Iterator[RawDocument]:
    """Yield valid raw documents from a JSONL file.

    Args:
        path: Path to the input JSONL file.

    Yields:
        Parsed and validated `RawDocument` records.

    Raises:
        FileNotFoundError: If the input file does not exist.
        ValueError: If a line is invalid JSON or misses required fields.
    """
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input JSONL file does not exist: {path}")

    required_keys = {"doc_id", "title", "content", "source", "timestamp"}
    with input_path.open("r", encoding="utf-8") as file:
        for index, line in enumerate(file, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {index}: {exc}") from exc
            missing = required_keys.difference(record.keys())
            if missing:
                raise ValueError(
                    f"Missing keys at line {index}: {sorted(missing)}"
                )
            yield RawDocument(
                doc_id=str(record["doc_id"]),
                title=str(record["title"]),
                content=str(record["content"]),
                source=str(record["source"]),
                timestamp=str(record["timestamp"]),
            )


def produce_documents(documents: Iterable[RawDocument]) -> int:
    """Produce documents to Kafka and return message count.

    Args:
        documents: Iterable of validated raw document objects.

    Returns:
        Total number of messages successfully queued for delivery.
    """
    settings = load_settings()
    producer = Producer({"bootstrap.servers": settings.kafka.bootstrap_servers})
    count = 0

    for doc in documents:
        producer.produce(
            settings.kafka.raw_topic,
            key=doc["doc_id"],
            value=json.dumps(doc, ensure_ascii=False),
            callback=_delivery_callback,
        )
        count += 1
        if count % 1000 == 0:
            producer.poll(0)

    producer.flush()
    return count


def main() -> None:
    """Run the Kafka ingestion job from local JSONL input."""
    settings = load_settings()
    setup_logging(settings.app.log_level)

    LOGGER.info("Reading input file: %s", settings.app.input_jsonl_path)
    docs = read_jsonl_documents(settings.app.input_jsonl_path)
    total = produce_documents(docs)
    LOGGER.info("Successfully produced %d documents to Kafka.", total)


if __name__ == "__main__":
    main()
