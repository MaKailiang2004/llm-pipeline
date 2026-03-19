"""基于 Ray Data 的 RAG 向量化流水线。

从 HDFS Silver 层读取文本，按句分块并保留元数据头，用开源模型生成稠密向量，
将结果写入 HDFS Gold 层。
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, Iterable, List, MutableMapping

import pandas as pd
import numpy as np
import ray
import torch
from dotenv import load_dotenv
from transformers import AutoModel, AutoTokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, max as spark_max
from pyspark.sql.types import ArrayType, FloatType, MapType, StringType, StructField, StructType

LOGGER = logging.getLogger(__name__)
STATUS_DONE = "DONE"
STATUS_SKIPPED_NO_NEW_DATA = "SKIPPED_NO_NEW_DATA"


# ==================== 【段落 1】配置 ====================

@dataclass(frozen=True)
class VectorizationConfig:
    """分块与向量化相关的配置。"""

    ray_address: str
    ray_namespace: str
    silver_path: str
    gold_embeddings_path: str
    embedding_model_name: str
    embed_batch_size: int
    embed_concurrency: int
    embed_num_gpus_per_worker: float
    embed_max_length: int
    chunk_max_chars: int
    chunk_max_sentences: int


def _normalize_vector_for_spark(value: Any) -> List[float]:
    """将向量统一为 Spark 可接受的 List[float]。"""
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [float(x) for x in value]
    # 单值兜底，避免类型推断失败
    return [float(value)]


def _normalize_metadata_for_spark(value: Any) -> Dict[str, str]:
    """将 metadata 统一为 Spark MapType(StringType, StringType)。"""
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    return {}


def _embedding_spark_schema() -> StructType:
    """向量结果写入 Spark 的显式 schema。"""
    return StructType(
        [
            StructField("chunk_id", StringType(), True),
            StructField("original_doc_id", StringType(), True),
            StructField("text_chunk", StringType(), True),
            StructField("vector", ArrayType(FloatType(), containsNull=False), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )


def _prepare_embeddings_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    """写回 Spark 前做强类型归一化，避免 vector/metadata 推断失败。"""
    if df.empty:
        return pd.DataFrame(columns=["chunk_id", "original_doc_id", "text_chunk", "vector", "metadata"])
    normalized_rows: List[Dict[str, Any]] = []
    for record in df.to_dict(orient="records"):
        normalized_rows.append(
            {
                "chunk_id": str(record.get("chunk_id", "")),
                "original_doc_id": str(record.get("original_doc_id", "")),
                "text_chunk": str(record.get("text_chunk", "")),
                "vector": _normalize_vector_for_spark(record.get("vector")),
                "metadata": _normalize_metadata_for_spark(record.get("metadata")),
            }
        )
    return pd.DataFrame(normalized_rows)


def setup_logging() -> None:
    """配置日志并静音第三方库的嘈杂输出。"""
    from common.logging_config import setup_logging as _setup

    _setup()


def load_config() -> VectorizationConfig:
    """从环境变量加载向量化运行时配置。

    Returns:
        解析后的向量化配置对象。
    """
    load_dotenv(override=False)
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port = os.getenv("HDFS_PORT", "9000")

    return VectorizationConfig(
        ray_address=os.getenv("RAY_ADDRESS", "ray://ray-head:10001"),
        ray_namespace=os.getenv("RAY_NAMESPACE", "rag-labeling"),
        silver_path=os.getenv(
            "HDFS_SILVER_PATH",
            f"hdfs://{hdfs_host}:{hdfs_port}/data/silver/",
        ),
        gold_embeddings_path=os.getenv(
            "HDFS_GOLD_EMBEDDINGS_PATH",
            f"hdfs://{hdfs_host}:{hdfs_port}/data/gold/embeddings/",
        ),
        embedding_model_name=os.getenv("EMBEDDING_MODEL_NAME", "BAAI/bge-large-zh-v1.5"),
        embed_batch_size=int(os.getenv("RAY_EMBED_BATCH_SIZE", "32")),
        embed_concurrency=int(os.getenv("RAY_EMBED_CONCURRENCY", "2")),
        embed_num_gpus_per_worker=float(os.getenv("RAY_EMBED_NUM_GPUS_PER_WORKER", "0.5")),
        embed_max_length=int(os.getenv("RAY_EMBED_MAX_LENGTH", "512")),
        chunk_max_chars=int(os.getenv("RAY_CHUNK_MAX_CHARS", "700")),
        chunk_max_sentences=int(os.getenv("RAY_CHUNK_MAX_SENTENCES", "6")),
    )


def _split_sentences(text: str) -> List[str]:
    """按标点边界将文本切分为句子列表。

    用于按句分块，避免粗暴按固定字符数截断。

    Args:
        text: 输入文档文本。

    Returns:
        保留标点的有序句子列表。
    """
    if not text:
        return []
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return []

    segments = re.split(r"(?<=[。！？!?；;\.])\s+", cleaned)
    sentences = [segment.strip() for segment in segments if segment.strip()]
    return sentences


def _build_metadata_header(title: str, timestamp: str, doc_id: str) -> str:
    """为每个块生成必选的元数据前缀。

    Args:
        title: 原文标题。
        timestamp: 原文时间戳。
        doc_id: 原文 id。

    Returns:
        元数据头字符串。
    """
    return f"Title: {title}\nTimestamp: {timestamp}\nDocumentID: {doc_id}\n\n"


# ==================== 【段落 2】核心：按句分块（带 metadata header） ====================

def _chunk_sentences(
    text: str,
    title: str,
    timestamp: str,
    doc_id: str,
    max_chars: int,
    max_sentences: int,
) -> List[str]:
    """按句子单元组成语义连贯的块。

    按句数与近似长度控制块大小，每块前加 title/timestamp/doc_id 元数据保留上下文。

    Args:
        text: 正文。
        title: 标题。
        timestamp: 时间戳。
        doc_id: 文档 id。
        max_chars: 每块正文的软性最大字符数。
        max_sentences: 每块正文的最大句数。

    Returns:
        带元数据头的块字符串列表。
    """
    header = _build_metadata_header(title=title, timestamp=timestamp, doc_id=doc_id)
    sentences = _split_sentences(text)
    if not sentences:
        return [header + (text or "")]

    chunks: List[str] = []
    current: List[str] = []
    current_chars = 0

    for sentence in sentences:
        sentence_len = len(sentence)
        reaches_sentence_limit = len(current) >= max_sentences
        reaches_char_limit = current_chars + sentence_len > max_chars and len(current) > 0
        if reaches_sentence_limit or reaches_char_limit:
            chunks.append(header + " ".join(current).strip())
            current = []
            current_chars = 0

        current.append(sentence)
        current_chars += sentence_len

    if current:
        chunks.append(header + " ".join(current).strip())
    return chunks


def _extract_doc_fields(record: Dict[str, Any]) -> Dict[str, str]:
    """将可能的 schema 变体统一为通用字段。

    Args:
        record: Silver Parquet 的一行字典。

    Returns:
        规范化后的文档字段，供分块使用。
    """
    doc_id = str(record.get("id") or record.get("doc_id") or "")
    text = str(record.get("text") or record.get("content") or "")
    title = str(record.get("title") or "Untitled")
    timestamp = str(record.get("timestamp") or "unknown")
    event_date = str(record.get("event_date") or "unknown")
    return {
        "doc_id": doc_id,
        "text": text,
        "title": title,
        "timestamp": timestamp,
        "event_date": event_date,
    }


def chunk_record(record: Dict[str, Any], config: VectorizationConfig) -> List[Dict[str, Any]]:
    """将一条记录拆成多条带元数据的块。

    Args:
        record: 源行记录。
        config: 流水线配置。

    Returns:
        嵌入前所需的块记录列表。
    """
    normalized = _extract_doc_fields(record)
    chunks = _chunk_sentences(
        text=normalized["text"],
        title=normalized["title"],
        timestamp=normalized["timestamp"],
        doc_id=normalized["doc_id"],
        max_chars=config.chunk_max_chars,
        max_sentences=config.chunk_max_sentences,
    )

    output: List[Dict[str, Any]] = []
    for idx, text_chunk in enumerate(chunks):
        chunk_key = f"{normalized['doc_id']}::{idx}::{normalized['timestamp']}"
        chunk_id = md5(chunk_key.encode("utf-8")).hexdigest()
        output.append(
            {
                "chunk_id": chunk_id,
                "original_doc_id": normalized["doc_id"],
                "text_chunk": text_chunk,
                "metadata": {
                    "title": normalized["title"],
                    "timestamp": normalized["timestamp"],
                    "event_date": normalized["event_date"],
                    "chunk_index": idx,
                    "chunk_count": len(chunks),
                },
            }
        )
    return output


# ==================== 【段落 3】Embedding 处理器与 OOM 降级 ====================

class EmbeddingBatchProcessor:
    """供 Ray map_batches 使用的批向量化可调用对象。"""

    def __init__(self, config: VectorizationConfig) -> None:
        """在每个 Worker 上加载一次 tokenizer/模型，保证高吞吐。

        Args:
            config: 向量化运行时配置。
        """
        self._config = config
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._tokenizer = AutoTokenizer.from_pretrained(config.embedding_model_name)
        self._model = AutoModel.from_pretrained(config.embedding_model_name)
        self._model.eval()
        self._model.to(self._device)
        LOGGER.info(
            "Embedding worker ready: model=%s device=%s",
            config.embedding_model_name,
            self._device,
        )

    def _embed_texts(self, texts: List[str]) -> List[List[float]]:
        """对文本列表做嵌入；可能抛出 OOM。"""
        with torch.inference_mode():
            encoded = self._tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=self._config.embed_max_length,
                return_tensors="pt",
            )
            encoded = {key: value.to(self._device) for key, value in encoded.items()}
            outputs = self._model(**encoded)

            hidden_state = outputs.last_hidden_state
            attention_mask = encoded["attention_mask"].unsqueeze(-1).expand(hidden_state.size()).float()
            summed = torch.sum(hidden_state * attention_mask, dim=1)
            counts = torch.clamp(attention_mask.sum(dim=1), min=1e-9)
            embeddings = summed / counts
            embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
            vectors = embeddings.detach().cpu().numpy().astype(np.float32)
        return [v.tolist() for v in vectors]

    def __call__(self, batch: MutableMapping[str, Iterable[Any]]) -> Dict[str, List[Any]]:
        """对一批文本块生成稠密向量。

        会做 max_length 截断，OOM 时按更小子批重试。
        """
        texts = [str(item) for item in batch.get("text_chunk", [])]
        if not texts:
            return {key: list(values) for key, values in batch.items()}

        max_len = self._config.embed_max_length
        max_chars = max_len * 3  # rough: ~2 chars/token for Chinese
        truncated = []
        for t in texts:
            truncated.append(t[:max_chars] if len(t) > max_chars else t)
        texts = truncated

        # ---------- 核心：OOM 时拆小 batch 重试 ----------
        fallback_batch = int(os.getenv("RAY_EMBED_OOM_FALLBACK_BATCH", "16"))
        try:
            vectors = self._embed_texts(texts)
        except RuntimeError as exc:
            if "out of memory" in str(exc).lower():
                LOGGER.warning(
                    "OOM during embedding, retrying with sub-batches of size %d", fallback_batch
                )
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                vectors = []
                for i in range(0, len(texts), fallback_batch):
                    sub = texts[i : i + fallback_batch]
                    vectors.extend(self._embed_texts(sub))
            else:
                raise

        result: Dict[str, List[Any]] = {key: list(values) for key, values in batch.items()}
        result["vector"] = vectors
        return result


# ==================== 【段落 4】Spark 回退与混合模式 ====================

def _run_vectorization_spark_fallback(config: VectorizationConfig) -> None:
    """本地向量化：Spark 读 + 分块 + 嵌入 + Spark 写，不使用 Ray。"""
    spark = (
        SparkSession.builder.appName("vectorization-spark-fallback")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    silver_df = spark.read.parquet(config.silver_path)
    embed_max_ts = _get_embeddings_max_timestamp(spark, config.gold_embeddings_path)
    if embed_max_ts:
        silver_df = silver_df.where(col("timestamp") > lit(embed_max_ts))
        LOGGER.info("Incremental vectorization window: timestamp > %s", embed_max_ts)
    if silver_df.rdd.isEmpty():
        spark.stop()
        LOGGER.info("Step5 status: %s (no new silver rows after watermark)", STATUS_SKIPPED_NO_NEW_DATA)
        return
    pandas_df = silver_df.toPandas()
    spark.stop()

    all_chunks: List[Dict[str, Any]] = []
    for _, row in pandas_df.iterrows():
        record = row.to_dict()
        chunks = chunk_record(record, config)
        all_chunks.extend(chunks)

    if not all_chunks:
        LOGGER.warning("No chunks produced; writing empty output.")
        spark = (
            SparkSession.builder.appName("vectorization-spark-write")
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .getOrCreate()
        )
        empty_pdf = _prepare_embeddings_for_spark(
            pd.DataFrame(columns=["chunk_id", "original_doc_id", "text_chunk", "vector", "metadata"])
        )
        spark.createDataFrame(empty_pdf, schema=_embedding_spark_schema()).write.mode("overwrite").parquet(
            config.gold_embeddings_path
        )
        spark.stop()
        return

    processor = EmbeddingBatchProcessor(config)
    batch_size = config.embed_batch_size
    results: List[Dict[str, Any]] = []

    for start in range(0, len(all_chunks), batch_size):
        batch_chunks = all_chunks[start : start + batch_size]
        batch_dict: Dict[str, List[Any]] = {
            "chunk_id": [c["chunk_id"] for c in batch_chunks],
            "original_doc_id": [c["original_doc_id"] for c in batch_chunks],
            "text_chunk": [c["text_chunk"] for c in batch_chunks],
            "metadata": [c["metadata"] for c in batch_chunks],
        }
        out = processor(batch_dict)
        for i in range(len(batch_chunks)):
            results.append({
                "chunk_id": out["chunk_id"][i],
                "original_doc_id": out["original_doc_id"][i],
                "text_chunk": out["text_chunk"][i],
                "vector": out["vector"][i],
                "metadata": out["metadata"][i],
            })

    out_df = _prepare_embeddings_for_spark(pd.DataFrame(results))
    spark = (
        SparkSession.builder.appName("vectorization-spark-write")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark_df = spark.createDataFrame(out_df, schema=_embedding_spark_schema())
    _append_embeddings_idempotent(spark, spark_df, config.gold_embeddings_path)
    spark.stop()
    LOGGER.info("Embeddings written to: %s (Spark fallback incremental)", config.gold_embeddings_path)


def _run_ray_vectorization_with_spark_io(config: VectorizationConfig) -> None:
    """混合模式：Spark 读写 HDFS，Ray 负责并行向量化。"""
    LOGGER.info("Using hybrid mode: Spark for HDFS I/O + Ray for parallel embedding")
    
    # Step 1: Spark 读取 Silver
    spark = (
        SparkSession.builder.appName("ray-vectorization-spark-io")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    silver_df = spark.read.parquet(config.silver_path)
    embed_max_ts = _get_embeddings_max_timestamp(spark, config.gold_embeddings_path)
    if embed_max_ts:
        silver_df = silver_df.where(col("timestamp") > lit(embed_max_ts))
        LOGGER.info("Incremental vectorization window: timestamp > %s", embed_max_ts)
    if silver_df.rdd.isEmpty():
        spark.stop()
        LOGGER.info("Step5 status: %s (no new silver rows after watermark)", STATUS_SKIPPED_NO_NEW_DATA)
        return
    silver_pandas = silver_df.toPandas()
    spark.stop()
    LOGGER.info("Loaded %d records from Silver via Spark", len(silver_pandas))
    
    # Step 2: 转换为 Ray Dataset
    source_ds = ray.data.from_pandas(silver_pandas)
    LOGGER.info("Converted to Ray Dataset")
    
    # Step 3: Ray 并行分块
    chunked_ds = source_ds.flat_map(lambda row: chunk_record(row, config))
    try:
        available_gpus = float(ray.available_resources().get("GPU", 0.0))
    except Exception:
        available_gpus = float(config.embed_concurrency)
    max_workers_by_gpu = max(1, int(available_gpus // max(config.embed_num_gpus_per_worker, 0.01)))
    effective_concurrency = max(1, min(config.embed_concurrency, max_workers_by_gpu))
    target_blocks = max(effective_concurrency * 4, 8)
    chunked_ds = chunked_ds.repartition(target_blocks)
    LOGGER.info("Chunking completed")
    
    # Step 4: Ray 并行 Embedding（可选 Placement Group 显式 GPU 绑定）
    map_batches_kwargs: Dict[str, Any] = {
        "fn": EmbeddingBatchProcessor,
        "fn_constructor_args": (config,),
        "batch_size": config.embed_batch_size,
        "concurrency": effective_concurrency,
        "num_gpus": config.embed_num_gpus_per_worker,
    }
    use_pg = os.getenv("RAY_EMBED_USE_PLACEMENT_GROUP", "0") == "1"
    if use_pg:
        try:
            pg = ray.util.placement_group(
                [{"GPU": config.embed_num_gpus_per_worker}] * config.embed_concurrency
            )
            ray.get(pg.ready())
            from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

            map_batches_kwargs["scheduling_strategy"] = PlacementGroupSchedulingStrategy(
                placement_group=pg
            )
            LOGGER.info(
                "Using Placement Group for embedding: %d bundles, %.2f GPU each",
                effective_concurrency,
                config.embed_num_gpus_per_worker,
            )
        except Exception as pg_exc:
            LOGGER.warning("Placement Group creation failed (%s), using default scheduling", pg_exc)
    embedded_ds = chunked_ds.map_batches(**map_batches_kwargs)
    
    # Step 5: 选择列并转回 Pandas
    final_ds = embedded_ds.select_columns(
        ["chunk_id", "original_doc_id", "text_chunk", "vector", "metadata"]
    )
    result_pandas = final_ds.to_pandas()
    result_pandas = _prepare_embeddings_for_spark(result_pandas)
    LOGGER.info("Embedding completed: %d chunks", len(result_pandas))
    
    # Step 6: Spark 写入 Gold
    spark = (
        SparkSession.builder.appName("ray-vectorization-spark-write")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    out_df = spark.createDataFrame(result_pandas, schema=_embedding_spark_schema())
    _append_embeddings_idempotent(spark, out_df, config.gold_embeddings_path)
    spark.stop()
    LOGGER.info("Embeddings written to: %s (via Spark incremental append)", config.gold_embeddings_path)


def _get_embeddings_max_timestamp(spark: SparkSession, embeddings_path: str) -> str | None:
    """从 Gold Embeddings 中读取 metadata.timestamp 最大值作为增量水位。"""
    try:
        max_ts_row = (
            spark.read.parquet(embeddings_path)
            .select(spark_max(expr("metadata['timestamp']")).alias("max_ts"))
            .first()
        )
    except Exception:
        return None
    if not max_ts_row:
        return None
    max_ts = max_ts_row["max_ts"]
    return str(max_ts) if max_ts is not None else None


def _has_new_rows_for_vectorization(config: VectorizationConfig) -> bool:
    """统一判定 Step5 本轮是否存在新增 Silver 数据。"""
    spark = (
        SparkSession.builder.appName("vectorization-incremental-gate")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    try:
        silver_df = spark.read.parquet(config.silver_path)
        embed_max_ts = _get_embeddings_max_timestamp(spark, config.gold_embeddings_path)
        if embed_max_ts:
            silver_df = silver_df.where(col("timestamp") > lit(embed_max_ts))
            LOGGER.info("Incremental vectorization window: timestamp > %s", embed_max_ts)
        return silver_df.limit(1).count() > 0
    finally:
        spark.stop()


def _append_embeddings_idempotent(
    spark: SparkSession, embedding_df, embeddings_path: str
) -> None:
    """幂等追加 embeddings：按 chunk_id 去重，不做全量覆盖。"""
    dedup_df = embedding_df.dropDuplicates(["chunk_id"])
    try:
        existing_keys = spark.read.parquet(embeddings_path).select("chunk_id").dropDuplicates(["chunk_id"])
        to_write_df = dedup_df.join(existing_keys, on=["chunk_id"], how="left_anti")
    except Exception:
        to_write_df = dedup_df

    if to_write_df.rdd.isEmpty():
        LOGGER.info("No new embedding rows to append after chunk_id upsert filter.")
        return

    to_write_df.write.mode("append").parquet(embeddings_path)


def run_vectorization(config: VectorizationConfig) -> None:
    """执行从 Silver 到 Gold 向量化的完整流程。

    Args:
        config: 向量化运行时配置。
    """
    if not _has_new_rows_for_vectorization(config):
        LOGGER.info("Step5 status: %s", STATUS_SKIPPED_NO_NEW_DATA)
        print(f"PIPELINE_STEP_STATUS:STEP5={STATUS_SKIPPED_NO_NEW_DATA}")
        return

    # 检查是否强制使用 Spark 回退模式
    use_spark_fallback = os.getenv("USE_SPARK_FALLBACK", "0") == "1"
    if use_spark_fallback:
        LOGGER.info("USE_SPARK_FALLBACK=1, using Spark local mode directly.")
        _run_vectorization_spark_fallback(config)
        print(f"PIPELINE_STEP_STATUS:STEP5={STATUS_DONE}")
        return
    
    # 初始化 Ray
    try:
        ray.init(address=config.ray_address, namespace=config.ray_namespace, ignore_reinit_error=True)
        LOGGER.info("Ray initialized at %s", config.ray_address)
    except Exception as ray_exc:
        LOGGER.error("Ray connection failed (%s)", ray_exc)
        LOGGER.warning("Falling back to pure Spark mode.")
        _run_vectorization_spark_fallback(config)
        print(f"PIPELINE_STEP_STATUS:STEP5={STATUS_DONE}")
        return
    
    # 使用混合模式
    try:
        _run_ray_vectorization_with_spark_io(config)
        print(f"PIPELINE_STEP_STATUS:STEP5={STATUS_DONE}")
    except Exception as exc:
        LOGGER.error("Hybrid mode failed (%s), falling back to pure Spark", exc)
        _run_vectorization_spark_fallback(config)
        print(f"PIPELINE_STEP_STATUS:STEP5={STATUS_DONE}")


def main() -> None:
    """Gold 向量生成的命令行入口。"""
    setup_logging()
    config = load_config()
    try:
        run_vectorization(config)
    except Exception as exc:
        LOGGER.exception("Vectorization pipeline failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
