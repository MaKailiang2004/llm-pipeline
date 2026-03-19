"""PySpark 批处理 ETL：清洗、脱敏与去重评论文本。

从 HDFS Bronze 层读取原始 JSON 评论，进行文本清洗与 PII 脱敏，
支持 MinHash 近似去重或与 Silver 的 content_hash anti-join 去重，
按日期分区写入 HDFS Silver 层 Parquet。
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass

from dotenv import load_dotenv
from pyspark.ml.feature import HashingTF, MinHashLSH, NGram, RegexTokenizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    col,
    coalesce,
    floor,
    from_json,
    max as spark_max,
    lit,
    monotonically_increasing_id,
    rand,
    sha2,
    to_date,
    udf,
)
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

LOGGER = logging.getLogger(__name__)


# ==================== 【段落 1】ETL 配置与 Spark 会话 ====================

@dataclass(frozen=True)
class EtlConfig:
    """从环境变量加载的 ETL 运行时配置。"""

    spark_master: str
    spark_app_name: str
    bronze_path: str
    silver_path: str
    parquet_block_size_bytes: int
    minhash_num_hash_tables: int
    dedup_similarity_threshold: float
    ngram_n: int
    hashing_num_features: int
    dedup_salt_buckets: int
    use_salted_dedup: bool
    dedup_mode: str  # "minhash" | "silver_anti_join"


def setup_logging() -> None:
    """配置全局日志并静音第三方库的嘈杂日志。"""
    from common.logging_config import setup_logging as _setup

    _setup()


def load_config() -> EtlConfig:
    """从 `.env` 环境变量加载 ETL 配置。

    Returns:
        强类型的 ETL 配置对象。
    """
    load_dotenv(override=False)
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port = os.getenv("HDFS_PORT", "9000")
    default_bronze = f"hdfs://{hdfs_host}:{hdfs_port}/data/bronze/"
    default_silver = f"hdfs://{hdfs_host}:{hdfs_port}/data/silver/"

    return EtlConfig(
        spark_master=os.getenv("SPARK_MASTER", "local[*]"),
        spark_app_name=os.getenv("SPARK_ETL_APP_NAME", "clean-and-dedup"),
        bronze_path=os.getenv("HDFS_BRONZE_PATH", default_bronze),
        silver_path=os.getenv("HDFS_SILVER_PATH", default_silver),
        # 256MB，位于 128MB~512MB 目标区间内。
        parquet_block_size_bytes=int(os.getenv("PARQUET_BLOCK_SIZE_BYTES", "268435456")),
        minhash_num_hash_tables=int(os.getenv("MINHASH_NUM_HASH_TABLES", "6")),
        dedup_similarity_threshold=float(os.getenv("DEDUP_SIMILARITY_THRESHOLD", "0.8")),
        ngram_n=int(os.getenv("NGRAM_N", "3")),
        hashing_num_features=int(os.getenv("HASHING_NUM_FEATURES", "262144")),
        dedup_salt_buckets=int(os.getenv("ETL_DEDUP_SALT_BUCKETS", "100")),
        use_salted_dedup=os.getenv("ETL_USE_SALTED_DEDUP", "1") == "1",
        dedup_mode=os.getenv("ETL_DEDUP_MODE", "minhash").lower(),
    )


def build_spark_session(config: EtlConfig) -> SparkSession:
    """创建批处理 ETL 用的 Spark 会话并设置 Parquet 写入选项。

    Args:
        config: ETL 运行时配置。

    Returns:
        已配置的 Spark 会话。
    """
    spark = (
        SparkSession.builder.appName(config.spark_app_name)
        .master(config.spark_master)
        .getOrCreate()
    )
    spark.conf.set("parquet.block.size", str(config.parquet_block_size_bytes))
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    return spark


# ==================== 【段落 2】文本清洗与 PII 脱敏 ====================

def _clean_text(raw_text: str) -> str:
    """去除 HTML 标签并规范化多余空白。

    Args:
        raw_text: 原始评论文本。

    Returns:
        清洗后、空白已合并的文本。
    """
    if raw_text is None:
        return ""
    no_html = re.sub(r"<[^>]+>", " ", raw_text)
    normalized = re.sub(r"\s+", " ", no_html).strip()
    return normalized


def _mask_pii(text: str) -> str:
    """对文本中可能的邮箱、电话号码进行脱敏。

    Args:
        text: 可能包含个人信息的输入文本。

    Returns:
        邮箱/电话被占位符替换后的文本。
    """
    if text is None:
        return ""
    masked_email = re.sub(
        r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",
        "[MASKED_EMAIL]",
        text,
    )
    masked_phone = re.sub(
        r"(?:(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{4})",
        "[MASKED_PHONE]",
        masked_email,
    )
    return masked_phone


# 将 Python 函数注册为 Spark UDF，供 DataFrame 列使用
clean_text_udf = udf(_clean_text, StringType())
mask_pii_udf = udf(_mask_pii, StringType())
feature_nnz_udf = udf(lambda v: int(v.numNonzeros()) if v is not None else 0, IntegerType())


# ==================== 【段落 3】Bronze 读取与清洗流水线 ====================

def read_bronze_reviews(spark: SparkSession, bronze_path: str) -> DataFrame:
    """从 HDFS Bronze 文本文件读取原始 JSON 记录。

    Args:
        spark: 当前 Spark 会话。
        bronze_path: 存放原始 JSON 行的 HDFS Bronze 目录。

    Returns:
        解析后的 DataFrame，列：id, text, timestamp。
    """
    schema = StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("text", StringType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
        ]
    )
    return (
        spark.read.text(bronze_path)
        .select(from_json(col("value"), schema).alias("record"))
        .select("record.*")
        .where(col("id").isNotNull() & col("text").isNotNull() & col("timestamp").isNotNull())
    )


def clean_reviews(df: DataFrame) -> DataFrame:
    """使用 UDF 做文本清洗与 PII 脱敏。

    Args:
        df: 含原始评论文本的输入 DataFrame。

    Returns:
        带清洗后 `text` 列的 DataFrame。
    """
    return df.withColumn("text", clean_text_udf(col("text"))).withColumn(
        "text", mask_pii_udf(col("text"))
    )


# ==================== 【段落 4】核心：MinHash 子集去重（返回要删除的 row_id） ====================

def _run_minhash_dedup_subset(featured_subset: DataFrame, config: EtlConfig) -> DataFrame:
    """在子集上执行 MinHashLSH 去重，返回重复行的 row_id。"""
    # 空子集直接返回空 DataFrame，避免后续 fit 报错
    if featured_subset.rdd.isEmpty():
        spark = featured_subset.sparkSession
        return spark.createDataFrame([], StructType([StructField("row_id", LongType(), False)]))

    # 用 MinHash LSH 对 ngram 特征做局部敏感哈希，numHashTables 越大召回越准、越慢
    model = MinHashLSH(
        inputCol="features",
        outputCol="hashes",
        numHashTables=config.minhash_num_hash_tables,
    ).fit(featured_subset)

    # Jaccard 距离 = 1 - 相似度；小于等于该距离的文档对会被视为相似
    max_distance = 1.0 - config.dedup_similarity_threshold
    # 自连接：在同一子集内找所有相似对 (A, B)，只保留 A.row_id < B.row_id，约定保留左删右
    pair_df = (
        model.approxSimilarityJoin(
            featured_subset.alias("left"),
            featured_subset.alias("right"),
            threshold=max_distance,
            distCol="jaccard_distance",
        )
        .where(col("datasetA.row_id") < col("datasetB.row_id"))
        .select(col("datasetB.row_id").alias("row_id"))
    )
    return pair_df.distinct()


# ==================== 【段落 5】核心：两阶段加盐去重（防倾斜 / 防 Executor OOM） ====================

def deduplicate_with_minhash(df: DataFrame, config: EtlConfig) -> DataFrame:
    """使用 MinHashLSH 与 Jaccard 相似度去除近似重复文档。

    Args:
        df: 已清洗的评论文本 DataFrame，含 `id`, `text`, `timestamp`。
        config: LSH 与相似度阈值的 ETL 配置。

    Returns:
        近似重复行已删除的 DataFrame。
    """
    # 为每行分配唯一 row_id，供后续 join 去重使用
    indexed = df.withColumn("row_id", monotonically_increasing_id())

    # 按字符级 tokenize，支持中文（\W+ 会把中文当分隔符导致空 token）
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="tokens",
        pattern=r".",
        gaps=False,
        minTokenLength=1,
        toLowercase=True,
    )
    tokenized = tokenizer.transform(indexed)

    # n-gram（默认 3）生成 ngrams 列
    ngram = NGram(n=config.ngram_n, inputCol="tokens", outputCol="ngrams")
    grams = ngram.transform(tokenized)

    # HashingTF 得到二值特征向量，用于 MinHash
    hashing_tf = HashingTF(
        inputCol="ngrams",
        outputCol="features",
        numFeatures=config.hashing_num_features,
        binary=True,
    )
    featured = hashing_tf.transform(grams)
    featured = featured.withColumn("feature_nnz", feature_nnz_udf(col("features")))
    featured = featured.where(col("feature_nnz") > 0).drop("feature_nnz")
    if featured.rdd.isEmpty():
        LOGGER.warning("All rows have empty token features; skipping MinHash deduplication.")
        return indexed.select("id", "text", "timestamp")

    # 不加盐时：全表做一次 MinHash 去重
    if not config.use_salted_dedup:
        duplicate_row_ids = _run_minhash_dedup_subset(featured, config)
        deduped = featured.join(duplicate_row_ids, on="row_id", how="left_anti")
        return deduped.select("id", "text", "timestamp")

    # ---------- Phase 1：加盐打散热点，按桶局部去重 ----------
    featured = featured.withColumn(
        "salt", (floor(rand() * config.dedup_salt_buckets) + 1).cast("int")
    )

    salt_values = [r.salt for r in featured.select("salt").distinct().collect()]
    duplicate_ids_list: list[DataFrame] = []
    for s in salt_values:
        subset = featured.filter(col("salt") == s)
        dup_ids = _run_minhash_dedup_subset(subset, config)
        duplicate_ids_list.append(dup_ids)

    duplicate_row_ids_phase1 = duplicate_ids_list[0]
    for dup_df in duplicate_ids_list[1:]:
        duplicate_row_ids_phase1 = duplicate_row_ids_phase1.union(dup_df).distinct()

    phase1_survivors = featured.join(duplicate_row_ids_phase1, on="row_id", how="left_anti")
    cols_to_drop = ["salt"] + (["hashes"] if "hashes" in phase1_survivors.columns else [])
    phase1_for_lsh = phase1_survivors.drop(*cols_to_drop)

    # ---------- Phase 2：对幸存者再做全局 MinHash 去重 ----------
    duplicate_row_ids_phase2 = _run_minhash_dedup_subset(phase1_for_lsh, config)
    deduped = phase1_survivors.join(duplicate_row_ids_phase2, on="row_id", how="left_anti")

    return deduped.select("id", "text", "timestamp")


def deduplicate_against_silver(
    df: DataFrame, spark: SparkSession, silver_path: str
) -> DataFrame:
    """与 Silver 层现有数据按 content_hash 精确去重，返回仅含新数据的 DataFrame。

    每批清洗后数据与已有 Silver 做 anti-join，避免重复流入下游。
    比 MinHash 轻量，无 LSH 阶段，适合流式增量。

    Args:
        df: 已清洗的 DataFrame，含 id, text, timestamp。
        spark: Spark 会话。
        silver_path: Silver 层 HDFS 路径。

    Returns:
        去重后的 DataFrame，含 id, text, timestamp, content_hash（供 write_silver 持久化）。
    """
    batch_with_hash = (
        df.withColumn("content_hash", sha2(col("text"), 256))
        .dropDuplicates(["content_hash"])
    )

    try:
        silver_df = spark.read.parquet(silver_path)
    except AnalysisException:
        LOGGER.info("Silver path empty or missing; no existing hashes to dedup against.")
        return batch_with_hash

    if silver_df.rdd.isEmpty():
        return batch_with_hash

    # 兼容旧 Silver：无 content_hash 时用 text 现场计算
    if "content_hash" not in silver_df.columns:
        existing_hashes = silver_df.withColumn(
            "content_hash", sha2(col("text"), 256)
        ).select("content_hash").distinct()
    else:
        existing_hashes = silver_df.select(
            coalesce(col("content_hash"), sha2(col("text"), 256)).alias("content_hash")
        ).distinct()

    deduped = batch_with_hash.join(existing_hashes, on="content_hash", how="left_anti")
    LOGGER.info("Silver anti-join dedup applied (batch vs existing silver by content_hash)")
    return deduped


# ==================== 【段落 6】写 Silver 与 main 入口 ====================

def write_silver(df: DataFrame, config: EtlConfig) -> None:
    """将清洗去重后的数据按分区写入 HDFS Silver 层 Parquet。

    Args:
        df: 清洗与去重后的输出 DataFrame。
        config: ETL 配置（含 silver 路径、parquet 块大小等）。
    """
    output_df = df.withColumn("event_date", to_date(col("timestamp")))
    if "content_hash" not in output_df.columns:
        output_df = output_df.withColumn("content_hash", sha2(col("text"), 256))
    output_df = output_df.withColumn(
        "event_date", col("event_date").cast(StringType())
    ).fillna({"event_date": "unknown"})

    # 幂等写入：同一 id+timestamp/content_hash 仅写入一次，避免并行轮次重复追加。
    output_df = output_df.dropDuplicates(["id", "timestamp", "content_hash"])
    spark = output_df.sparkSession

    try:
        existing_keys = (
            spark.read.parquet(config.silver_path)
            .select("id", "timestamp", coalesce(col("content_hash"), sha2(col("text"), 256)).alias("content_hash"))
            .dropDuplicates(["id", "timestamp", "content_hash"])
        )
        to_write_df = output_df.join(
            existing_keys, on=["id", "timestamp", "content_hash"], how="left_anti"
        )
    except AnalysisException:
        # Silver 首次写入时路径可能尚不存在。
        to_write_df = output_df

    if to_write_df.rdd.isEmpty():
        LOGGER.info("No new silver rows to append after id+timestamp upsert filter.")
        return

    (
        to_write_df.write.mode("append")
        .option("parquet.block.size", str(config.parquet_block_size_bytes))
        .partitionBy("event_date")
        .parquet(config.silver_path)
    )


def get_silver_max_timestamp(spark: SparkSession, silver_path: str) -> str | None:
    """读取 Silver 已处理到的最大 timestamp（用于增量流式窗口）。"""
    try:
        max_ts_row = spark.read.parquet(silver_path).select(spark_max(col("timestamp")).alias("max_ts")).first()
    except AnalysisException:
        return None
    if not max_ts_row:
        return None
    max_ts = max_ts_row["max_ts"]
    return str(max_ts) if max_ts is not None else None


def main() -> None:
    """执行从 Bronze 到 Silver 的完整 ETL 流程。"""
    setup_logging()
    config = load_config()
    LOGGER.info("ETL config loaded: bronze=%s silver=%s", config.bronze_path, config.silver_path)

    spark = build_spark_session(config)
    try:
        bronze_df = read_bronze_reviews(spark, config.bronze_path)
        if bronze_df.rdd.isEmpty():
            LOGGER.warning("No input records found in bronze path: %s", config.bronze_path)
            return

        # 真增量流式：每轮仅处理 silver 之后新增的 bronze 数据窗口。
        silver_max_ts = get_silver_max_timestamp(spark, config.silver_path)
        if silver_max_ts:
            bronze_df = bronze_df.where(col("timestamp") > lit(silver_max_ts))
            LOGGER.info("Incremental ETL window: timestamp > %s", silver_max_ts)
            if bronze_df.rdd.isEmpty():
                LOGGER.info("No new bronze rows beyond silver watermark; skipping this cycle.")
                return

        cleaned_df = clean_reviews(bronze_df)

        if config.dedup_mode == "silver_anti_join":
            deduped_df = deduplicate_against_silver(cleaned_df, spark, config.silver_path)
            LOGGER.info("Dedup mode: silver_anti_join (batch vs existing silver by content_hash)")
        elif config.dedup_mode == "minhash":
            deduped_df = deduplicate_with_minhash(cleaned_df, config)
            LOGGER.info("Dedup mode: minhash")
        else:
            LOGGER.warning("Unknown ETL_DEDUP_MODE=%s, fallback to minhash.", config.dedup_mode)
            deduped_df = deduplicate_with_minhash(cleaned_df, config)

        deduped_df = deduped_df.withColumn("etl_tag", lit("clean_and_dedup"))

        write_silver(deduped_df.drop("etl_tag"), config)
        LOGGER.info("ETL finished successfully and data written to silver zone.")
    except Exception as exc:
        LOGGER.exception("ETL execution failed: %s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
