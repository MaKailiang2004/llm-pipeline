"""基于 Ray Data + vLLM 的分布式批打标流水线。

本脚本完成 Gold 阶段自动打标：初始化 Ray、从 HDFS 读 Silver Parquet、
通过 map-batches 调用 vLLM、生成含情感与实体的结构化 JSON 标签、
将结果写入 HDFS `/data/gold/labeled/`。
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import re
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence

import pandas as pd
import ray
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max

LOGGER = logging.getLogger(__name__)
STATUS_DONE = "DONE"
STATUS_SKIPPED_NO_NEW_DATA = "SKIPPED_NO_NEW_DATA"

# ==================== 【段落 1】配置与工具函数 ====================

PROMPT_TEMPLATE = """你是一名高质量数据标注助手。请阅读评论文本并输出 JSON，必须严格满足以下 schema：
{{
  "sentiment": "positive|neutral|negative",
  "entities": ["实体1", "实体2"],
  "reason": "一句话说明判断依据"
}}

要求：
1) 只输出 JSON，不要输出任何额外文本。
2) sentiment 只能从 positive/neutral/negative 里选。
3) entities 必须且仅能从原文中逐字出现或明确指代的词汇中抽取；禁止推断、猜测或添加原文未提及的品牌/产品名称。
4) entities 最多 8 个。若原文中确实没有可识别的品牌/产品等实体，可返回 ["未知"] 或与原文最相关的通用词，切勿编造具体品牌名（如小米、华为等）。

评论文本：
{text}
"""

POSITIVE_HINTS = (
    "不错", "满意", "很好", "好用", "优秀", "推荐", "超出预期", "名不虚传", "喜欢", "棒",
    "值得", "对得起", "稳定", "扎实", "精美", "精细", "惊喜", "靠谱", "丝滑",
)
NEGATIVE_HINTS = (
    "很差", "失望", "不好", "问题", "卡顿", "不耐用", "发热严重", "死机", "不建议", "不符",
    "一般般", "不太", "很慢", "噪音", "掉电", "发烫", "划痕", "瑕疵", "漏光", "断连",
)
ENTITY_HINTS = (
    "华为", "小米", "苹果", "OPPO", "vivo", "三星", "荣耀",
    "手机", "耳机", "充电器", "保护壳", "平板", "手表", "键盘", "鼠标", "显示器", "音响",
    "外观", "性能", "续航", "拍照", "音质", "屏幕", "手感", "做工", "包装", "配件",
    "客服", "物流", "售后",
)
ENTITY_PATTERN_HINTS = (
    "售后服务", "客服", "物流", "价格", "拍照", "续航", "信号", "电池", "保护壳",
    "性能", "外观", "手感", "屏幕", "音质", "配件", "体验", "做工", "包装",
)
POSITIVE_REASON_HINTS = ("满意", "推荐", "稳定", "对得起", "优秀", "值得", "喜欢", "好")
NEGATIVE_REASON_HINTS = ("失望", "问题", "慢", "不好", "不符", "不建议", "差", "瑕疵")
CONTRAST_TOKENS = ("但是", "不过", "但", "然而")


# ==================== 【段落 1】配置与工具函数 ====================

@dataclass(frozen=True)
class LabelingConfig:
    """Ray/vLLM 打标任务的运行时配置。"""

    ray_address: str
    ray_namespace: str
    silver_path: str
    gold_labeled_path: str
    vllm_model_name: str
    llm_concurrency: int
    max_num_batched_tokens: int
    inference_batch_size: int
    max_tokens: int
    temperature: float
    top_p: float
    # vLLM Offline engine knobs (maximize throughput, saturate single GPU)
    offline_gpu_memory_utilization: float
    offline_max_num_seqs: int
    offline_max_model_len: int
    offline_dtype: str
    offline_trust_remote_code: bool
    offline_enable_prefix_caching: bool


def setup_logging() -> None:
    """配置日志并静音第三方库的嘈杂输出。"""
    from common.logging_config import setup_logging as _setup

    _setup()


def get_actor_config_dict(config: Optional[LabelingConfig] = None) -> Dict[str, Any]:
    """供压测等外部调用，获取 OfflineVllmLabelerActor 所需的 config_dict。"""
    cfg = config or load_config()
    return {
        "ray_address": cfg.ray_address,
        "ray_namespace": cfg.ray_namespace,
        "silver_path": cfg.silver_path,
        "gold_labeled_path": cfg.gold_labeled_path,
        "vllm_model_name": cfg.vllm_model_name,
        "llm_concurrency": cfg.llm_concurrency,
        "max_num_batched_tokens": cfg.max_num_batched_tokens,
        "inference_batch_size": cfg.inference_batch_size,
        "max_tokens": cfg.max_tokens,
        "temperature": cfg.temperature,
        "top_p": cfg.top_p,
        "offline_gpu_memory_utilization": cfg.offline_gpu_memory_utilization,
        "offline_max_num_seqs": cfg.offline_max_num_seqs,
        "offline_max_model_len": cfg.offline_max_model_len,
        "offline_dtype": cfg.offline_dtype,
        "offline_trust_remote_code": cfg.offline_trust_remote_code,
        "offline_enable_prefix_caching": cfg.offline_enable_prefix_caching,
    }


def load_config() -> LabelingConfig:
    """从 `.env` 加载并解析打标配置。

    Returns:
        解析后的打标配置对象。
    """
    load_dotenv(override=False)
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port = os.getenv("HDFS_PORT", "9000")

    return LabelingConfig(
        ray_address=os.getenv("RAY_ADDRESS", "ray://ray-head:10001"),
        ray_namespace=os.getenv("RAY_NAMESPACE", "rag-labeling"),
        silver_path=os.getenv(
            "HDFS_SILVER_PATH",
            f"hdfs://{hdfs_host}:{hdfs_port}/data/silver/",
        ),
        gold_labeled_path=os.getenv(
            "HDFS_GOLD_LABELED_PATH",
            f"hdfs://{hdfs_host}:{hdfs_port}/data/gold/labeled/",
        ),
        vllm_model_name=os.getenv("VLLM_MODEL_NAME", "Qwen/Qwen2.5-3B-Instruct"),
        llm_concurrency=int(os.getenv("RAY_LLM_CONCURRENCY", "4")),
        max_num_batched_tokens=int(os.getenv("RAY_LLM_MAX_BATCHED_TOKENS", "16384")),
        inference_batch_size=int(os.getenv("RAY_INFERENCE_BATCH_SIZE", "32")),
        max_tokens=int(os.getenv("VLLM_MAX_TOKENS", "256")),
        temperature=float(os.getenv("VLLM_TEMPERATURE", "0.0")),
        top_p=float(os.getenv("VLLM_TOP_P", "1.0")),
        offline_gpu_memory_utilization=float(os.getenv("VLLM_OFFLINE_GPU_MEMORY_UTILIZATION", "0.90")),
        offline_max_num_seqs=int(os.getenv("VLLM_OFFLINE_MAX_NUM_SEQS", "128")),
        offline_max_model_len=int(os.getenv("VLLM_OFFLINE_MAX_MODEL_LEN", "2048")),
        offline_dtype=os.getenv("VLLM_OFFLINE_DTYPE", "bfloat16"),
        offline_trust_remote_code=os.getenv("VLLM_OFFLINE_TRUST_REMOTE_CODE", "0") == "1",
        offline_enable_prefix_caching=os.getenv("VLLM_OFFLINE_ENABLE_PREFIX_CACHING", "1") == "1",
    )


def _extract_json_object(text: str) -> Dict[str, Any]:
    """从模型输出中安全提取第一个 JSON 对象。

    Args:
        text: vLLM 返回的原始文本。

    Returns:
        解析后的字典，解析失败时返回兜底默认值。
    """
    fallback = {
        "sentiment": "neutral",
        "entities": [],
        "reason": "parse_fallback",
    }
    if not text:
        return fallback

    match = re.search(r"\{[\s\S]*\}", text)
    candidate = match.group(0) if match else text.strip()
    try:
        parsed = json.loads(candidate)
        sentiment = str(parsed.get("sentiment", "neutral")).lower()
        if sentiment not in {"positive", "neutral", "negative"}:
            sentiment = "neutral"
        entities_raw = parsed.get("entities", [])
        entities = [str(item) for item in entities_raw] if isinstance(entities_raw, list) else []
        entities = entities[:8]
        reason = str(parsed.get("reason", ""))
        return {"sentiment": sentiment, "entities": entities, "reason": reason}
    except Exception:
        return fallback


def _dedup_keep_order(items: Iterable[str]) -> List[str]:
    """去重并保持原有顺序。"""
    seen = set()
    result: List[str] = []
    for item in items:
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _infer_sentiment_from_text(text: str) -> str:
    """根据文本中的词汇线索推断情感。"""
    if not text:
        return "neutral"
    # 转折后通常是作者更核心的观点，优先看后半句。
    for token in CONTRAST_TOKENS:
        if token in text:
            text = text.split(token, 1)[1].strip() or text
            break
    neg_hits = sum(1 for token in NEGATIVE_HINTS if token in text)
    pos_hits = sum(1 for token in POSITIVE_HINTS if token in text)
    if neg_hits > pos_hits:
        return "negative"
    if pos_hits > neg_hits:
        return "positive"
    return "neutral"


def _extract_entities_from_text(text: str) -> List[str]:
    """用词典线索从文本中抽取可能实体。"""
    found = [token for token in ENTITY_HINTS if token in text]
    found.extend(token for token in ENTITY_PATTERN_HINTS if token in text)
    # 抽取 2~6 个中文词组作为弱候选，减少常见实体漏提。
    found.extend(re.findall(r"[\u4e00-\u9fff]{2,6}", text))
    return _dedup_keep_order(found)[:8]


def _reason_sentiment_conflict(sentiment: str, reason: str) -> bool:
    if not reason:
        return False
    has_pos = any(token in reason for token in POSITIVE_REASON_HINTS)
    has_neg = any(token in reason for token in NEGATIVE_REASON_HINTS)
    if sentiment == "positive" and has_neg and not has_pos:
        return True
    if sentiment == "negative" and has_pos and not has_neg:
        return True
    return False


# ==================== 【段落 2】核心：规则兜底与输出规范化 ====================

def _normalize_label_output(text: str, output_text: str) -> Dict[str, Any]:
    """对模型输出做轻量规则修正与规范化。"""
    parsed = _extract_json_object(output_text)
    sentiment = str(parsed.get("sentiment", "neutral"))
    entities = parsed.get("entities", [])
    reason = str(parsed.get("reason", ""))
    postprocess_tags: List[str] = []

    if not isinstance(entities, list):
        entities = []
    entities = _dedup_keep_order(str(item) for item in entities)[:8]

    # 模型给 neutral 时用规则从原文推断情感，避免漏标
    inferred_sentiment = _infer_sentiment_from_text(text)
    if sentiment == "neutral" and inferred_sentiment != "neutral":
        sentiment = inferred_sentiment
        postprocess_tags.append("rule_sentiment_override")
        if not reason or reason == "parse_fallback":
            reason = "rule_sentiment_override"
    elif sentiment != "neutral" and inferred_sentiment != "neutral" and inferred_sentiment != sentiment:
        sentiment = inferred_sentiment
        postprocess_tags.append("rule_sentiment_conflict_override")

    # 模型未抽实体时用规则从原文抽取，并标记原因
    if not entities:
        entities = _extract_entities_from_text(text)
        postprocess_tags.append("rule_entities_fill")
        if not reason or reason == "parse_fallback":
            reason = "rule_entities_fill"

    if _reason_sentiment_conflict(sentiment, reason):
        reason = "reason_rewrite_for_sentiment_consistency"
        postprocess_tags.append("reason_rewrite")

    if not reason:
        reason = "model_output"
    if not postprocess_tags:
        postprocess_tags = ["model_output"]

    return {
        "sentiment": sentiment,
        "entities": entities[:8],
        "reason": reason,
        "postprocess_tag": "|".join(_dedup_keep_order(postprocess_tags)),
    }


def _instantiate_vllm_engine_processor_config(config: LabelingConfig) -> Any:
    """按当前 Ray 版本构造 vLLMEngineProcessorConfig。

    不同 Ray 版本构造函数参数名可能不同，本函数按签名只传支持的字段，
    同时保留吞吐相关配置。

    Args:
        config: 流水线运行时配置。

    Returns:
        实例化后的 vLLMEngineProcessorConfig，不可用时为 None。
    """
    try:
        llm_module = import_module("ray.data.llm")
    except ModuleNotFoundError:
        LOGGER.warning(
            "ray.data.llm is unavailable in current Ray build; using local offline vLLM path instead."
        )
        return None
    config_cls = getattr(llm_module, "vLLMEngineProcessorConfig")
    signature = inspect.signature(config_cls)
    available = set(signature.parameters.keys())

    candidate_values: Dict[str, Any] = {
        "model": config.vllm_model_name,
        "model_source": config.vllm_model_name,
        "model_id": config.vllm_model_name,
        "base_url": config.vllm_base_url,
        "engine_url": config.vllm_base_url,
        "endpoint": config.vllm_base_url,
        "service_url": config.vllm_base_url,
        "concurrency": config.llm_concurrency,
        "max_num_batched_tokens": config.max_num_batched_tokens,
        "max_tokens": config.max_tokens,
        "temperature": config.temperature,
    }
    kwargs = {key: value for key, value in candidate_values.items() if key in available}

    engine_config = config_cls(**kwargs)
    LOGGER.info(
        "Initialized vLLMEngineProcessorConfig with fields: %s",
        sorted(kwargs.keys()),
    )
    return engine_config


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _should_skip_spark_fallback(exc: Exception) -> bool:
    """遇到 vLLM/GPU 初始化失败时，避免 fallback 再次加载一份模型。"""
    text = f"{exc.__class__.__name__}: {exc}".lower()
    markers = (
        "cuda out of memory",
        "outofmemoryerror",
        "no available memory for the cache blocks",
        "actordiederror",
        "offlinevllmlabeleractor",
        "vllm",
    )
    return any(marker in text for marker in markers)


def _instantiate_vllm_offline_llm(config: LabelingConfig):
    """实例化 vLLM Offline LLM，引擎常驻在 Actor 内复用。"""
    from vllm import LLM  # vLLM 仅在 Ray GPU 容器中需要

    signature = inspect.signature(LLM)
    available = set(signature.parameters.keys())
    supports_kwargs = any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in signature.parameters.values()
    )

    # vLLM LLM 的不同版本参数名会有差异，这里按签名过滤，避免不兼容
    candidate_values: Dict[str, Any] = {
        "model": config.vllm_model_name,
        "dtype": config.offline_dtype,
        "trust_remote_code": config.offline_trust_remote_code,
        "max_model_len": config.offline_max_model_len,
        "max_num_seqs": config.offline_max_num_seqs,
        "max_num_batched_tokens": config.max_num_batched_tokens,
        "gpu_memory_utilization": config.offline_gpu_memory_utilization,
        "enable_prefix_caching": config.offline_enable_prefix_caching,
        # 尽量减少额外开销，专注吞吐
        "enforce_eager": _bool_env("VLLM_OFFLINE_ENFORCE_EAGER", True),
        "disable_log_stats": _bool_env("VLLM_OFFLINE_DISABLE_LOG_STATS", True),
    }
    if supports_kwargs:
        kwargs = candidate_values
    else:
        kwargs = {key: value for key, value in candidate_values.items() if key in available}

    LOGGER.info("Initializing vLLM Offline LLM with fields: %s", sorted(kwargs.keys()))
    return LLM(**kwargs)


def _sampling_params(config: LabelingConfig):
    from vllm import SamplingParams

    return SamplingParams(
        temperature=config.temperature,
        top_p=config.top_p,
        max_tokens=config.max_tokens,
    )


def _vllm_generate_texts(llm_outputs: Sequence[Any]) -> List[str]:
    """将 vLLM generate() 的返回结果尽量稳健地抽取为文本列表。"""
    texts: List[str] = []
    for out in llm_outputs:
        # vLLM: RequestOutput.outputs[0].text
        try:
            outputs = getattr(out, "outputs", None)
            if outputs and len(outputs) > 0:
                texts.append(str(getattr(outputs[0], "text", "")))
                continue
        except Exception:
            pass
        texts.append("")
    return texts


def _label_rows_with_offline_vllm(
    llm: Any, sampling_params: Any, rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    texts = [str(row.get("text", "")) for row in rows]
    prompts = [PROMPT_TEMPLATE.format(text=text) for text in texts]
    try:
        outputs = llm.generate(prompts, sampling_params=sampling_params)
        generated = _vllm_generate_texts(outputs)
    except Exception:
        generated = [""] * len(prompts)
        return [
            {
                **row,
                "sentiment": "neutral",
                "entities_json": "[]",
                "label_reason": "vllm_offline_failed",
                "postprocess_tag": "vllm_offline_failed",
                "prompt_used": prompts[idx],
            }
            for idx, row in enumerate(rows)
        ]

    enriched_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        parsed = _normalize_label_output(texts[idx], generated[idx] if idx < len(generated) else "")
        enriched_rows.append(
            {
                **row,
                "sentiment": str(parsed["sentiment"]),
                "entities_json": json.dumps(parsed["entities"], ensure_ascii=False),
                "label_reason": str(parsed["reason"]),
                "postprocess_tag": str(parsed.get("postprocess_tag", "model_output")),
                "prompt_used": prompts[idx],
            }
        )
    return enriched_rows


def _run_labeling_spark_fallback(
    config: LabelingConfig,
    pandas_df: pd.DataFrame,
    llm: Any,
    sampling_params: Any,
) -> None:
    """在本地按批打标并通过 Spark 将结果写回 HDFS。

    不使用 ray.put/ray.data.from_pandas，避免在 Ray 集群上出现 numpy 反序列化错误。
    """
    batch_size = config.inference_batch_size
    rows = pandas_df.shape[0]
    labeled_chunks: List[pd.DataFrame] = []

    for start in range(0, rows, batch_size):
        end = min(start + batch_size, rows)
        batch_pdf = pandas_df.iloc[start:end]
        batch_rows = batch_pdf.to_dict(orient="records")
        labeled_rows = _label_rows_with_offline_vllm(llm, sampling_params, batch_rows)
        labeled_chunks.append(pd.DataFrame(labeled_rows))

    merged = pd.concat(labeled_chunks, ignore_index=True)

    spark = (
        SparkSession.builder.appName("batch-labeling-spark-write")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    _append_gold_labeled_idempotent(spark, merged, config.gold_labeled_path)
    
    # 同时导出 CSV
    local_csv_path = "/tmp/labeled_output.csv"
    merged.to_csv(local_csv_path, index=False, encoding="utf-8")
    LOGGER.info("Exported labeled data to CSV: %s (rows=%d)", local_csv_path, len(merged))
    LOGGER.info("CSV preview:\n%s", merged.head(3).to_string())
    
    spark.stop()
    LOGGER.info("Wrote labeled gold dataset to %s (Spark fallback incremental)", config.gold_labeled_path)


def _get_gold_labeled_max_timestamp(spark: SparkSession, gold_labeled_path: str) -> str | None:
    """获取 Gold Labeled 层最大 timestamp，作为增量窗口水位。"""
    try:
        max_ts_row = (
            spark.read.parquet(gold_labeled_path)
            .select(spark_max(col("timestamp")).alias("max_ts"))
            .first()
        )
    except Exception:
        return None
    if not max_ts_row:
        return None
    max_ts = max_ts_row["max_ts"]
    return str(max_ts) if max_ts is not None else None


def _get_incremental_labeling_gate(
    spark: SparkSession, silver_path: str, gold_labeled_path: str
) -> tuple[str | None, bool]:
    """统一判定 Step4 是否有新增数据。

    Returns:
        (gold_max_ts, has_new_data)
    """
    gold_max_ts = _get_gold_labeled_max_timestamp(spark, gold_labeled_path)
    silver_df = spark.read.parquet(silver_path)
    if gold_max_ts:
        silver_df = silver_df.where(col("timestamp") > lit(gold_max_ts))
        LOGGER.info("Incremental labeling window: timestamp > %s", gold_max_ts)
    has_new_data = silver_df.limit(1).count() > 0
    return gold_max_ts, has_new_data


def _filter_ray_dataset_incremental(silver_ds: Any, gold_max_ts: str | None) -> Any:
    """优先使用表达式过滤 Ray Dataset，避免 lambda filter 性能告警。"""
    if not gold_max_ts:
        return silver_ds
    safe_ts = gold_max_ts.replace("'", "\\'")
    try:
        return silver_ds.filter(expr=f"timestamp > '{safe_ts}'")
    except Exception:
        return silver_ds.filter(lambda row: str(row.get("timestamp", "")) > gold_max_ts)


def _append_gold_labeled_idempotent(
    spark: SparkSession, labeled_pandas: pd.DataFrame, gold_labeled_path: str
) -> None:
    """幂等追加 Gold Labeled：按 id+timestamp 去重，避免全量覆盖。"""
    out_df = spark.createDataFrame(labeled_pandas).dropDuplicates(["id", "timestamp"])
    try:
        existing_keys = (
            spark.read.parquet(gold_labeled_path)
            .select("id", "timestamp")
            .dropDuplicates(["id", "timestamp"])
        )
        to_write_df = out_df.join(existing_keys, on=["id", "timestamp"], how="left_anti")
    except Exception:
        to_write_df = out_df

    if to_write_df.rdd.isEmpty():
        LOGGER.info("No new labeled rows to append after id+timestamp upsert filter.")
        return

    to_write_df.write.mode("append").parquet(gold_labeled_path)


def _run_spark_fallback_labeling(config: LabelingConfig, gold_max_ts: str | None = None) -> None:
    """纯 Spark 模式：Spark 读 Silver、本地按批打标、Spark 写 Gold，不使用 Ray。"""
    # 统一使用 namenode，ray-head 通过挂载宿主机 /etc/hosts 解析；spark-worker 经 Docker DNS 解析
    engine_config = _instantiate_vllm_engine_processor_config(config)
    spark = (
        SparkSession.builder.appName("batch-labeling-spark-fallback")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    try:
        silver_df = spark.read.parquet(config.silver_path)
    except Exception as exc:
        err_msg = str(exc).lower()
        if "unable_to_infer_schema" in err_msg or "path does not exist" in err_msg or "empty" in err_msg:
            LOGGER.warning(
                "Silver path is empty or invalid (no parquet data). Skipping labeling. "
                "Ensure bronze->silver ETL completed successfully. Err: %s",
                exc,
            )
            spark.stop()
            return
        raise
    if gold_max_ts is None:
        gold_max_ts = _get_gold_labeled_max_timestamp(spark, config.gold_labeled_path)
    if gold_max_ts:
        silver_df = silver_df.where(col("timestamp") > lit(gold_max_ts))
        LOGGER.info("Incremental labeling window: timestamp > %s", gold_max_ts)

    if silver_df.rdd.isEmpty():
        LOGGER.info("Step4 status: %s (no new silver rows after watermark)", STATUS_SKIPPED_NO_NEW_DATA)
        spark.stop()
        return
    pandas_df = silver_df.toPandas()
    spark.stop()
    llm = _instantiate_vllm_offline_llm(config)
    sampling_params = _sampling_params(config)
    _run_labeling_spark_fallback(config, pandas_df, llm, sampling_params)


# ==================== 【段落 4】Ray Actor：单 batch 串行，池限流 ====================

_RAY_LABEL_NUM_GPUS_PER_ACTOR = float(os.getenv("RAY_LABEL_NUM_GPUS_PER_ACTOR", "1"))


@ray.remote(num_gpus=_RAY_LABEL_NUM_GPUS_PER_ACTOR)
class OfflineVllmLabelerActor:
    """离线 vLLM 打标 Actor：进程内加载模型，最大化吞吐与显存利用。"""

    def __init__(self, config_dict: Dict[str, Any]) -> None:
        self._config = LabelingConfig(**config_dict)
        self._llm = _instantiate_vllm_offline_llm(self._config)
        self._sampling_params = _sampling_params(self._config)

    def label_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return _label_rows_with_offline_vllm(self._llm, self._sampling_params, rows)


# ==================== 【段落 5】核心：Ray Data / Spark I/O + Ray Actor 池 ====================

def _run_ray_with_ray_data_io(
    config: LabelingConfig, engine_config: Any, gold_max_ts: str | None = None
) -> None:
    """Ray Data 直接读 HDFS Parquet，流式分批送 Ray Actor，再写回 HDFS。

    替代 Spark + toPandas，避免 driver 全量加载，适合大规模 Silver 数据。
    要求 Ray 节点能访问 HDFS（如 ray-head 经 localhost:9000 连接 Namenode）。
    若 Ray Data 读 HDFS 失败（缺 libhdfs 等），应回退到 _run_ray_with_spark_io。
    """
    import ray.data

    LOGGER.info("Using Ray Data for HDFS I/O (direct read/write, no Spark toPandas)")

    # Step 1: Ray Data 直接读 HDFS Silver（流式，driver 不一次性加载）
    try:
        silver_ds = ray.data.read_parquet(config.silver_path)
    except Exception as exc:
        raise RuntimeError(f"Ray Data failed to read HDFS Silver {config.silver_path}: {exc}") from exc

    if gold_max_ts:
        silver_ds = _filter_ray_dataset_incremental(silver_ds, gold_max_ts)
        LOGGER.info("Incremental labeling window: timestamp > %s", gold_max_ts)

    total_rows = silver_ds.count()
    if total_rows == 0:
        LOGGER.info("Step4 status: %s (no new silver rows after watermark)", STATUS_SKIPPED_NO_NEW_DATA)
        return
    LOGGER.info("Ray Data loaded %d records from Silver (streaming)", total_rows)

    # Step 2: Ray Actor 池
    config_dict = get_actor_config_dict(config)
    available_gpus = float(ray.available_resources().get("GPU", 0.0))
    if available_gpus < _RAY_LABEL_NUM_GPUS_PER_ACTOR:
        raise RuntimeError(
            f"Ray cluster has insufficient GPU resources (available={available_gpus}, "
            f"required_per_actor={_RAY_LABEL_NUM_GPUS_PER_ACTOR})."
        )

    default_actor_count = max(1, int(available_gpus // _RAY_LABEL_NUM_GPUS_PER_ACTOR))
    actor_count = int(os.getenv("RAY_LABEL_ACTOR_COUNT", str(default_actor_count)))
    actor_count = max(1, min(actor_count, default_actor_count))

    LOGGER.info(
        "Using offline vLLM actor pool: actors=%d, gpus_per_actor=%s",
        actor_count,
        _RAY_LABEL_NUM_GPUS_PER_ACTOR,
    )

    actors = [OfflineVllmLabelerActor.remote(config_dict) for _ in range(actor_count)]

    # Step 3: 流式 iter_batches，每批送 actor，收集 labeled_rows
    labeled_rows: List[Dict[str, Any]] = []
    futures: List[Any] = []
    batch_idx = 0

    for batch_pdf in silver_ds.iter_batches(
        batch_size=config.inference_batch_size,
        batch_format="pandas",
    ):
        if batch_pdf.empty:
            continue
        batch_rows = batch_pdf.to_dict(orient="records")
        actor = actors[batch_idx % actor_count]
        futures.append(actor.label_batch.remote(batch_rows))
        batch_idx += 1

    LOGGER.info("Submitted %d labeling tasks to %d actors", len(futures), actor_count)

    for result in ray.get(futures):
        labeled_rows.extend(result)

    labeled_pandas = pd.DataFrame(labeled_rows)
    LOGGER.info("Labeling completed via Ray actor pool: %d records", len(labeled_pandas))

    # Step 4: Gold 侧做幂等 append（避免每轮全量覆盖）
    spark = (
        SparkSession.builder.appName("batch-labeling-ray-data-write")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    _append_gold_labeled_idempotent(spark, labeled_pandas, config.gold_labeled_path)
    spark.stop()
    LOGGER.info("Wrote labeled gold dataset to %s (via Ray Data incremental append)", config.gold_labeled_path)

    # Step 5: 导出 CSV（与 Spark 模式一致）
    local_csv_path = "/tmp/labeled_output.csv"
    labeled_pandas.to_csv(local_csv_path, index=False, encoding="utf-8")
    LOGGER.info("Exported labeled data to CSV: %s (rows=%d)", local_csv_path, len(labeled_pandas))
    LOGGER.info("CSV preview:\n%s", labeled_pandas.head(3).to_string())


def _run_ray_with_spark_io(
    config: LabelingConfig, engine_config: Any, gold_max_ts: str | None = None
) -> None:
    """混合模式：Spark 读写 HDFS，Ray 负责并行计算。

    避免 Ray Data 直接访问 HDFS（需要 libhdfs），同时利用 Ray 的并行能力。
    """
    LOGGER.info("Using hybrid mode: Spark for HDFS I/O + Ray for parallel compute")

    # Step 1: Spark 读取 Silver 数据（使用 namenode，ray-head 挂载 /etc/hosts 解析）
    spark = (
        SparkSession.builder.appName("ray-labeling-spark-io")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    try:
        silver_df = spark.read.parquet(config.silver_path)
    except Exception as exc:
        spark.stop()
        raise
    if gold_max_ts is None:
        gold_max_ts = _get_gold_labeled_max_timestamp(spark, config.gold_labeled_path)
    if gold_max_ts:
        silver_df = silver_df.where(col("timestamp") > lit(gold_max_ts))
        LOGGER.info("Incremental labeling window: timestamp > %s", gold_max_ts)

    if silver_df.rdd.isEmpty():
        spark.stop()
        LOGGER.info("Step4 status: %s (no new silver rows after watermark)", STATUS_SKIPPED_NO_NEW_DATA)
        return
    pandas_df = silver_df.toPandas()
    spark.stop()
    LOGGER.info("Loaded %d records from Silver via Spark", len(pandas_df))
    
    # Step 2: Ray Actor 池并行标注（每 GPU 一个 Actor，vLLM 内部做 batching）
    records = pandas_df.to_dict(orient="records")
    if not records:
        LOGGER.warning("No records found in Silver dataset.")
        labeled_pandas = pandas_df.copy()
    else:
        config_dict = get_actor_config_dict(config)
        available_gpus = float(ray.available_resources().get("GPU", 0.0))
        if available_gpus < _RAY_LABEL_NUM_GPUS_PER_ACTOR:
            raise RuntimeError(
                f"Ray cluster has insufficient GPU resources (available={available_gpus}, "
                f"required_per_actor={_RAY_LABEL_NUM_GPUS_PER_ACTOR})."
            )

        default_actor_count = max(1, int(available_gpus // _RAY_LABEL_NUM_GPUS_PER_ACTOR))
        actor_count = int(os.getenv("RAY_LABEL_ACTOR_COUNT", str(default_actor_count)))
        actor_count = max(1, min(actor_count, default_actor_count))

        LOGGER.info(
            "Using offline vLLM actor pool: actors=%d, gpus_per_actor=%s, available_gpus=%s",
            actor_count,
            _RAY_LABEL_NUM_GPUS_PER_ACTOR,
            available_gpus,
        )

        actors = [OfflineVllmLabelerActor.remote(config_dict) for _ in range(actor_count)]
        batch_starts = list(range(0, len(records), config.inference_batch_size))
        futures = []
        for i, start in enumerate(batch_starts):
            batch_rows = records[start : start + config.inference_batch_size]
            actor = actors[i % actor_count]  # 轮询分配，实现负载均衡
            futures.append(actor.label_batch.remote(batch_rows))
        LOGGER.info("Submitted %d labeling tasks to %d actors", len(futures), actor_count)

        labeled_rows: List[Dict[str, Any]] = []
        for result in ray.get(futures):
            labeled_rows.extend(result)
        labeled_pandas = pd.DataFrame(labeled_rows)
        LOGGER.info("Labeling completed via Ray actor pool: %d records", len(labeled_pandas))

    # Step 3: Spark 写入 Gold
    spark = (
        SparkSession.builder.appName("ray-labeling-spark-write")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    _append_gold_labeled_idempotent(spark, labeled_pandas, config.gold_labeled_path)
    spark.stop()
    LOGGER.info("Wrote labeled gold dataset to %s (via Spark incremental append)", config.gold_labeled_path)
    
    # Step 4: 导出 CSV
    local_csv_path = "/tmp/labeled_output.csv"
    labeled_pandas.to_csv(local_csv_path, index=False, encoding="utf-8")
    LOGGER.info("Exported labeled data to CSV: %s (rows=%d)", local_csv_path, len(labeled_pandas))
    LOGGER.info("CSV preview:\n%s", labeled_pandas.head(3).to_string())


# ==================== 【段落 6】入口：Ray 初始化与回退 ====================

def run_batch_labeling(config: LabelingConfig) -> None:
    """执行端到端批打标流程（Ray Dataset / 混合模式 / Spark 回退）。

    Args:
        config: 运行时配置对象。
    """
    engine_config = _instantiate_vllm_engine_processor_config(config)

    # 统一增量门控：先判定本轮是否有新增，避免空批次触发误报 fallback 链路。
    gate_spark = (
        SparkSession.builder.appName("batch-labeling-incremental-gate")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    try:
        gold_max_ts, has_new_data = _get_incremental_labeling_gate(
            gate_spark, config.silver_path, config.gold_labeled_path
        )
    finally:
        gate_spark.stop()
    if not has_new_data:
        LOGGER.info("Step4 status: %s", STATUS_SKIPPED_NO_NEW_DATA)
        print(f"PIPELINE_STEP_STATUS:STEP4={STATUS_SKIPPED_NO_NEW_DATA}")
        return

    # 检查是否强制使用 Spark 回退模式
    use_spark_fallback = os.getenv("USE_SPARK_FALLBACK", "0") == "1"
    if use_spark_fallback:
        LOGGER.info("USE_SPARK_FALLBACK=1, using Spark local mode directly.")
        _run_spark_fallback_labeling(config, gold_max_ts=gold_max_ts)
        print(f"PIPELINE_STEP_STATUS:STEP4={STATUS_DONE}")
        return

    # 初始化 Ray
    try:
        ray.init(address=config.ray_address, namespace=config.ray_namespace, ignore_reinit_error=True)
        LOGGER.info("Ray initialized at %s (namespace=%s)", config.ray_address, config.ray_namespace)
    except Exception as ray_exc:
        LOGGER.error("Ray connection failed (%s)", ray_exc)
        LOGGER.warning("Falling back to pure Spark mode.")
        _run_spark_fallback_labeling(config, gold_max_ts=gold_max_ts)
        print(f"PIPELINE_STEP_STATUS:STEP4={STATUS_DONE}")
        return

    # 优先尝试 Ray Data 直接读 HDFS（USE_RAY_DATA_HDFS_IO=1 时），避免 toPandas 全量加载
    use_ray_data_io = os.getenv("USE_RAY_DATA_HDFS_IO", "0") == "1"
    if use_ray_data_io:
        try:
            _run_ray_with_ray_data_io(config, engine_config, gold_max_ts=gold_max_ts)
            print(f"PIPELINE_STEP_STATUS:STEP4={STATUS_DONE}")
            return
        except Exception as exc:
            if _should_skip_spark_fallback(exc):
                raise
            LOGGER.warning(
                "Ray Data HDFS I/O failed (%s), falling back to Spark I/O",
                exc,
            )

    # 混合模式：Spark I/O + Ray Compute（或 Ray Data 失败后的回退）
    try:
        _run_ray_with_spark_io(config, engine_config, gold_max_ts=gold_max_ts)
        print(f"PIPELINE_STEP_STATUS:STEP4={STATUS_DONE}")
    except Exception as exc:
        if _should_skip_spark_fallback(exc):
            LOGGER.error(
                "Hybrid mode failed with vLLM/GPU error (%s); skipping Spark fallback to avoid "
                "loading another offline vLLM instance on the same GPU.",
                exc,
            )
            raise
        LOGGER.error("Hybrid mode failed (%s), falling back to pure Spark", exc)
        _run_spark_fallback_labeling(config, gold_max_ts=gold_max_ts)
        print(f"PIPELINE_STEP_STATUS:STEP4={STATUS_DONE}")


def main() -> None:
    """打标任务的命令行入口。"""
    setup_logging()
    config = load_config()
    try:
        run_batch_labeling(config)
    except Exception as exc:
        LOGGER.exception("Batch labeling failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
