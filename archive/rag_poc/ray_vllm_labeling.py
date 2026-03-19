"""Distributed auto-labeling pipeline using Ray workers and vLLM.

This module loads processed documents from HDFS via Spark, parallelizes batched
inference across Ray actors, and writes labeled outputs back to HDFS and Kafka.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Sequence

import httpx
import ray
from confluent_kafka import Producer
from pyspark.sql import DataFrame, SparkSession

from rag_poc.config import Settings, load_settings
from rag_poc.logging_utils import setup_logging
from rag_poc.schemas import LabeledDocument, RawDocument

LOGGER = logging.getLogger(__name__)

PROMPT_TEMPLATE = (
    "You are a strict data labeling assistant for a RAG corpus.\n"
    "Return only one JSON object with keys: label, confidence.\n"
    "Allowed labels: finance, legal, medical, technology, education, general.\n"
    "Confidence must be a float in [0,1].\n\n"
    "Title: {title}\n"
    "Content: {content}\n"
)


def _build_spark_session(settings: Settings) -> SparkSession:
    """Create Spark session used for HDFS read/write around Ray inference.

    Args:
        settings: Global settings object.

    Returns:
        Configured Spark session instance.
    """
    return (
        SparkSession.builder.appName("rag-ray-vllm-labeling")
        .master(settings.spark.master)
        .getOrCreate()
    )


def _read_documents_from_hdfs(spark: SparkSession, settings: Settings) -> List[RawDocument]:
    """Read processed documents from HDFS into Python records.

    Args:
        spark: Active Spark session.
        settings: Global settings object.

    Returns:
        A list of raw document dictionaries.
    """
    processed_path = f"{settings.hdfs.uri}{settings.hdfs.processed_path}"
    raw_path = f"{settings.hdfs.uri}{settings.hdfs.raw_path}"
    try:
        df: DataFrame = spark.read.parquet(processed_path)
        LOGGER.info("Loaded records from processed path: %s", processed_path)
    except Exception:
        LOGGER.warning(
            "Processed path unavailable; fallback to raw path: %s",
            raw_path,
        )
        df = spark.read.parquet(raw_path)
    rows = df.select("doc_id", "title", "content", "source", "timestamp").collect()
    return [
        RawDocument(
            doc_id=str(row["doc_id"]),
            title=str(row["title"]),
            content=str(row["content"]),
            source=str(row["source"]),
            timestamp=str(row["timestamp"]),
        )
        for row in rows
    ]


def _chunked(items: Sequence[RawDocument], batch_size: int) -> Iterable[List[RawDocument]]:
    """Yield sequence chunks of fixed batch size.

    Args:
        items: Sequence of input documents.
        batch_size: Number of items per chunk.

    Yields:
        Document batches.
    """
    for index in range(0, len(items), batch_size):
        yield list(items[index : index + batch_size])


@ray.remote
class VllmLabelActor:
    """Ray actor that performs batched inference against vLLM API.

    Batched prompt submission is used to maximize throughput and reduce
    per-request overhead. Each actor independently maintains an HTTP client.
    """

    def __init__(self, settings: Settings) -> None:
        """Initialize the actor with HTTP client and static parameters.

        Args:
            settings: Global settings object passed from the driver.
        """
        self._settings = settings
        self._endpoint = f"{settings.vllm.base_url}/completions"
        self._client = httpx.Client(timeout=settings.vllm.request_timeout_seconds)

    def _parse_label_json(self, text: str) -> Dict[str, Any]:
        """Parse model response text into label payload.

        Args:
            text: Raw completion text that should contain a JSON object.

        Returns:
            Parsed dictionary with fallback values on malformed output.
        """
        try:
            payload = json.loads(text.strip())
            label = str(payload.get("label", "general"))
            confidence = float(payload.get("confidence", 0.5))
            confidence = max(0.0, min(1.0, confidence))
            return {"label": label, "confidence": confidence}
        except Exception:
            return {"label": "general", "confidence": 0.5}

    def label_batch(self, batch: List[RawDocument]) -> List[LabeledDocument]:
        """Label a document batch using one batched vLLM completion request.

        Args:
            batch: Input raw document list.

        Returns:
            Labeled document records.
        """
        prompts = [
            PROMPT_TEMPLATE.format(title=item["title"], content=item["content"])
            for item in batch
        ]
        body: Dict[str, Any] = {
            "model": self._settings.vllm.model_name,
            "prompt": prompts,
            "max_tokens": self._settings.vllm.max_tokens,
            "temperature": self._settings.vllm.temperature,
            "top_p": self._settings.vllm.top_p,
        }
        response = self._client.post(self._endpoint, json=body)
        response.raise_for_status()
        payload = response.json()

        choices: List[Dict[str, Any]] = payload.get("choices", [])
        outputs: List[LabeledDocument] = []
        for idx, item in enumerate(batch):
            choice_text = ""
            if idx < len(choices):
                choice_text = str(choices[idx].get("text", "")).strip()
            parsed = self._parse_label_json(choice_text)
            outputs.append(
                LabeledDocument(
                    doc_id=item["doc_id"],
                    title=item["title"],
                    content=item["content"],
                    source=item["source"],
                    timestamp=item["timestamp"],
                    auto_label=str(parsed["label"]),
                    confidence=float(parsed["confidence"]),
                    model_name=self._settings.vllm.model_name,
                    metadata={"raw_response": choice_text},
                )
            )
        return outputs


def _write_labels_to_kafka(settings: Settings, records: Sequence[LabeledDocument]) -> None:
    """Publish labeled records to Kafka label topic.

    Args:
        settings: Global settings object.
        records: Sequence of labeled document dictionaries.
    """
    producer = Producer({"bootstrap.servers": settings.kafka.bootstrap_servers})
    for record in records:
        producer.produce(
            settings.kafka.label_topic,
            key=record["doc_id"],
            value=json.dumps(record, ensure_ascii=False),
        )
    producer.flush()


def _write_labels_to_hdfs(
    spark: SparkSession, settings: Settings, records: Sequence[LabeledDocument]
) -> None:
    """Persist labeled records to HDFS as parquet.

    Args:
        spark: Active Spark session.
        settings: Global settings object.
        records: Sequence of labeled records.
    """
    if not records:
        LOGGER.warning("No labeled records generated; skipping HDFS write.")
        return
    output_path = f"{settings.hdfs.uri}{settings.hdfs.label_path}"
    df = spark.createDataFrame(list(records))
    df.write.mode("append").parquet(output_path)


def _label_with_ray(settings: Settings, documents: List[RawDocument]) -> List[LabeledDocument]:
    """Distribute inference batches to Ray actors and collect outputs.

    Args:
        settings: Global settings object.
        documents: Input documents to label.

    Returns:
        Flattened labeled record list.
    """
    if not documents:
        return []

    ray.init(
        address=settings.ray.address,
        namespace=settings.ray.namespace,
        ignore_reinit_error=True,
    )
    actors = [VllmLabelActor.remote(settings) for _ in range(settings.ray.num_workers)]
    batches = list(_chunked(documents, settings.ray.inference_batch_size))

    futures = []
    for index, batch in enumerate(batches):
        actor = actors[index % len(actors)]
        futures.append(actor.label_batch.remote(batch))

    labeled_nested = ray.get(futures)
    labeled_flat: List[LabeledDocument] = [
        item for sublist in labeled_nested for item in sublist
    ]
    return labeled_flat


def main() -> None:
    """Run the full Ray + vLLM labeling job."""
    settings = load_settings()
    setup_logging(settings.app.log_level)
    spark = _build_spark_session(settings)

    LOGGER.info("Loading source documents from HDFS...")
    source_docs = _read_documents_from_hdfs(spark, settings)
    LOGGER.info("Loaded %d source documents for auto-labeling.", len(source_docs))

    labeled_docs = _label_with_ray(settings, source_docs)
    LOGGER.info("Generated %d labeled documents.", len(labeled_docs))

    _write_labels_to_hdfs(spark, settings, labeled_docs)
    _write_labels_to_kafka(settings, labeled_docs)
    LOGGER.info("Finished writing labels to HDFS and Kafka.")


if __name__ == "__main__":
    main()
