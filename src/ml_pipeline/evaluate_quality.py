"""对打标后的 Gold 数据集做持续质量评估。

从 HDFS Gold 路径采样打标记录，发送到 OpenAI 兼容的 Judge 模型（如 GPT-4o、DeepSeek），
收集 G-Eval 风格分数并输出完整质量报告与告警。
"""

from __future__ import annotations

import json
import logging
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
import ray
from dotenv import load_dotenv
from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)


# ==================== 【段落 1】配置与 Judge 结果结构 ====================

@dataclass(frozen=True)
class EvalConfig:
    """持续评估流程的运行时配置。"""

    ray_address: str
    ray_namespace: str
    gold_labeled_path: str
    local_labeled_csv_path: str
    sample_size: int
    judge_api_base_url: str
    judge_api_key: str
    judge_model: str
    judge_timeout_seconds: float
    judge_max_retries: int
    judge_retry_backoff_seconds: float
    judge_concurrency: int
    quality_threshold: float
    random_seed: int


@dataclass(frozen=True)
class JudgeResult:
    """单条样本的规范化 Judge 响应。"""

    task_completion: int
    hallucination: int
    overall: float
    reason: str


# ==================== 【段落 2】配置加载与文本/实体提取工具 ====================

def setup_logging() -> None:
    """配置日志并静音第三方库（如 httpx）的嘈杂输出。"""
    from common.logging_config import setup_logging as _setup

    _setup()


def load_config() -> EvalConfig:
    """从 `.env` 加载评估配置。

    Returns:
        解析后的评估配置。
    """
    load_dotenv(override=False)
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port = os.getenv("HDFS_PORT", "9000")
    default_gold = f"hdfs://{hdfs_host}:{hdfs_port}/data/gold/labeled/"

    api_key = os.getenv("JUDGE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Missing JUDGE_API_KEY for external judge API.")

    return EvalConfig(
        ray_address=os.getenv("RAY_ADDRESS", "ray://ray-head:10001"),
        ray_namespace=os.getenv("RAY_NAMESPACE", "rag-labeling"),
        gold_labeled_path=os.getenv("HDFS_GOLD_LABELED_PATH", default_gold),
        local_labeled_csv_path=os.getenv("LOCAL_LABELED_CSV_PATH", "/tmp/labeled_output.csv"),
        sample_size=int(os.getenv("QUALITY_SAMPLE_SIZE", "100")),
        judge_api_base_url=os.getenv(
            "JUDGE_API_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"
        ),
        judge_api_key=api_key,
        judge_model=os.getenv("JUDGE_MODEL_NAME", "qwen-plus"),
        judge_timeout_seconds=float(os.getenv("JUDGE_TIMEOUT_SECONDS", "60")),
        judge_max_retries=int(os.getenv("JUDGE_MAX_RETRIES", "3")),
        judge_retry_backoff_seconds=float(os.getenv("JUDGE_RETRY_BACKOFF_SECONDS", "2")),
        judge_concurrency=int(os.getenv("JUDGE_CONCURRENCY", "8")),
        quality_threshold=float(os.getenv("QUALITY_PASS_THRESHOLD", "4.0")),
        random_seed=int(os.getenv("QUALITY_RANDOM_SEED", "42")),
    )


def _as_text(value: Any) -> str:
    """将任意值转为安全的字符串表示。

    Args:
        value: 数据集行中的某列值。

    Returns:
        字符串。
    """
    if value is None:
        return ""
    return str(value)


# ==================== 【段落 3】采样与 Judge 调用 ====================

def _extract_entities(row: Dict[str, Any]) -> str:
    """从可能的 schema 变体中抽取实体字段。

    Args:
        row: 打标行记录。

    Returns:
        实体字符串表示。
    """
    entities_value = row.get("entities_json", row.get("entities", "[]"))
    if isinstance(entities_value, list):
        return json.dumps([str(item) for item in entities_value], ensure_ascii=False)
    return _as_text(entities_value)


def _extract_source_text(row: Dict[str, Any]) -> str:
    """从若干可能的列名中取原文字段。

    Args:
        row: 打标行记录。

    Returns:
        用于评判幻觉与抽取准确度的原文。
    """
    candidates = ["text", "content", "text_chunk", "review_text"]
    for key in candidates:
        if key in row and row[key] is not None:
            return _as_text(row[key])
    return ""


def _extract_doc_id(row: Dict[str, Any]) -> str:
    """从已知 schema 变体中取文档 ID。

    Args:
        row: 打标行记录。

    Returns:
        文档标识。
    """
    candidates = ["id", "doc_id", "original_doc_id", "chunk_id"]
    for key in candidates:
        if key in row and row[key] is not None:
            return _as_text(row[key])
    return "unknown_doc_id"


def _reason_sentiment_conflict(row: Dict[str, Any]) -> bool:
    sentiment = _as_text(row.get("sentiment", "")).lower()
    reason = _as_text(row.get("label_reason", row.get("reason", "")))
    if not sentiment or not reason:
        return False
    pos_hints = ("满意", "推荐", "稳定", "值得", "好", "优秀")
    neg_hints = ("失望", "问题", "慢", "差", "不建议", "不符", "瑕疵")
    has_pos = any(token in reason for token in pos_hints)
    has_neg = any(token in reason for token in neg_hints)
    if sentiment == "positive" and has_neg and not has_pos:
        return True
    if sentiment == "negative" and has_pos and not has_neg:
        return True
    return False


def _is_empty_entities(row: Dict[str, Any]) -> bool:
    entities_value = row.get("entities_json", row.get("entities", "[]"))
    if isinstance(entities_value, list):
        return len(entities_value) == 0
    text = _as_text(entities_value).strip()
    return text in {"", "[]", "[ ]", "null", "None"}


def _entity_coverage_miss(row: Dict[str, Any]) -> bool:
    source_text = _extract_source_text(row)
    if not source_text:
        return False
    key_entities = ("价格", "拍照", "售后服务", "客服", "物流", "性能", "外观", "续航", "信号")
    if not any(token in source_text for token in key_entities):
        return False
    entities_str = _extract_entities(row)
    return not any(token in entities_str for token in key_entities if token in source_text)


def compute_functional_integrity_metrics(samples: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """补充功能完整性指标，衡量明显错误与规则修正效果。"""
    if not samples:
        return {
            "reason_sentiment_conflict_rate": 0.0,
            "empty_entities_rate": 0.0,
            "entity_coverage_miss_rate": 0.0,
            "postprocess_hit_rate": 0.0,
        }
    total = len(samples)
    conflict_count = sum(1 for row in samples if _reason_sentiment_conflict(row))
    empty_entities_count = sum(1 for row in samples if _is_empty_entities(row))
    coverage_miss_count = sum(1 for row in samples if _entity_coverage_miss(row))
    postprocess_hit_count = sum(
        1
        for row in samples
        if _as_text(row.get("postprocess_tag", "")).strip() not in {"", "model_output"}
    )
    return {
        "reason_sentiment_conflict_rate": round(conflict_count / total, 4),
        "empty_entities_rate": round(empty_entities_count / total, 4),
        "entity_coverage_miss_rate": round(coverage_miss_count / total, 4),
        "postprocess_hit_rate": round(postprocess_hit_count / total, 4),
    }


def _sample_local_labeled_csv(config: EvalConfig) -> List[Dict[str, Any]]:
    """从本地 CSV 兜底采样，避免 HDFS 地址上下文问题阻塞评估。"""
    csv_path = config.local_labeled_csv_path
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Local labeled CSV not found: {csv_path}")

    pandas_df = pd.read_csv(csv_path)
    if pandas_df.empty:
        return []

    rows = pandas_df.fillna("").to_dict("records")
    random.seed(config.random_seed)
    if len(rows) <= config.sample_size:
        return rows
    return random.sample(rows, config.sample_size)


def _is_hdfs_sampling_error(exc: Exception) -> bool:
    """识别 HDFS 地址/连通性问题，触发本地 CSV 兜底。"""
    text = f"{exc.__class__.__name__}: {exc}".lower()
    markers = (
        "unknownhostexception",
        "connection refused",
        "connectexception",
        "call from",
        "localhost:9000",
        "namenode:9000",
        "no file system for scheme",
        "path does not exist",
        "unable to infer schema",
        "hdfs",
    )
    return any(marker in text for marker in markers)


def build_geval_prompt(row: Dict[str, Any]) -> str:
    """为一条打标样本构建 G-Eval 风格 Judge 提示。

    Args:
        row: 打标记录。

    Returns:
        供 Judge 模型使用的提示文本。
    """
    doc_id = _extract_doc_id(row)
    source_text = _extract_source_text(row)
    sentiment = _as_text(row.get("sentiment", ""))
    entities = _extract_entities(row)
    label_reason = _as_text(row.get("label_reason", row.get("reason", "")))

    return f"""你是一名严谨的数据质量评审员。请根据给定原文与打标结果进行 G-Eval 风格评分。

[评测维度]
1) Task Completion（打标准确度，1-5分）
- 5分：实体提取完整准确，格式规范，情感判断与原文高度一致。
- 3分：存在部分遗漏或轻微错误，但总体可用。
- 1分：实体严重错误/缺失，或格式不合规，情感判断明显错误。

2) Hallucination（幻觉率，1-5分；分数越高表示幻觉越少）
- 5分：结论完全基于原文，无捏造。
- 3分：有轻微推断但基本可追溯原文。
- 1分：存在明显凭空捏造或与原文矛盾。

[待评估样本]
doc_id: {doc_id}
原文:
{source_text}

当前打标结果:
{{
  "sentiment": "{sentiment}",
  "entities": {entities},
  "reason": "{label_reason}"
}}

请仅输出一个 JSON，格式严格如下：
{{
  "task_completion": 1-5的整数,
  "hallucination": 1-5的整数,
  "overall": 1-5的小数,
  "reason": "不超过80字的中文说明"
}}
"""


def _parse_judge_json(content: str) -> JudgeResult:
    """将 Judge 模型输出解析为规范化分数对象。

    Args:
        content: 模型原始响应内容。

    Returns:
        解析后的 JudgeResult。
    """
    fallback = JudgeResult(task_completion=1, hallucination=1, overall=1.0, reason="parse_failed")
    try:
        start = content.find("{")
        end = content.rfind("}")
        candidate = content[start : end + 1] if start >= 0 and end > start else content
        data = json.loads(candidate)

        task = int(data.get("task_completion", 1))
        hall = int(data.get("hallucination", 1))
        overall = float(data.get("overall", (task + hall) / 2.0))
        reason = _as_text(data.get("reason", ""))

        task = max(1, min(5, task))
        hall = max(1, min(5, hall))
        overall = max(1.0, min(5.0, overall))
        return JudgeResult(task_completion=task, hallucination=hall, overall=overall, reason=reason)
    except Exception:
        return fallback


def evaluate_one_sample(
    row: Dict[str, Any], config: EvalConfig, client: httpx.Client
) -> Tuple[str, JudgeResult]:
    """对一条样本调用 Judge API 评估，带重试。

    Args:
        row: 打标数据行。
        config: 评估配置。
        client: 共用 HTTP 客户端。

    Returns:
        (doc_id, JudgeResult) 元组。
    """
    doc_id = _extract_doc_id(row)
    prompt = build_geval_prompt(row)
    endpoint = f"{config.judge_api_base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {config.judge_api_key}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "model": config.judge_model,
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": "你是严格、公正、可复现的数据质量评审模型。"},
            {"role": "user", "content": prompt},
        ],
    }

    for attempt in range(1, config.judge_max_retries + 1):
        try:
            resp = client.post(endpoint, headers=headers, json=payload)
            resp.raise_for_status()
            body = resp.json()
            content = _as_text(body["choices"][0]["message"]["content"])
            return doc_id, _parse_judge_json(content)
        except Exception as exc:
            LOGGER.warning(
                "Judge request failed doc_id=%s attempt=%d/%d: %s",
                doc_id,
                attempt,
                config.judge_max_retries,
                exc,
            )
            if attempt == config.judge_max_retries:
                return doc_id, JudgeResult(
                    task_completion=1,
                    hallucination=1,
                    overall=1.0,
                    reason="judge_request_failed",
                )
            time.sleep(config.judge_retry_backoff_seconds * attempt)

    return doc_id, JudgeResult(task_completion=1, hallucination=1, overall=1.0, reason="unknown")


def _sample_labeled_data_ray_spark_hybrid(config: EvalConfig) -> List[Dict[str, Any]]:
    """混合模式：Spark 读取 HDFS，Ray Data 处理采样。"""
    LOGGER.info("Using hybrid mode: Spark for HDFS read + Ray for sampling")
    
    # Step 1: Spark 读取 Gold
    spark = (
        SparkSession.builder.appName("evaluate-quality-spark-io")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    gold_df = spark.read.parquet(config.gold_labeled_path)
    gold_pandas = gold_df.toPandas()
    spark.stop()
    LOGGER.info("Loaded %d records from Gold via Spark", len(gold_pandas))
    
    # Step 2: 转换为 Ray Dataset 并采样
    import ray
    dataset = ray.data.from_pandas(gold_pandas)
    rows = dataset.take_all()
    if not rows:
        return []
    
    random.seed(config.random_seed)
    if len(rows) <= config.sample_size:
        return [dict(item) for item in rows]
    sampled = random.sample(rows, config.sample_size)
    return [dict(item) for item in sampled]


def sample_labeled_data(config: EvalConfig) -> List[Dict[str, Any]]:
    """从 HDFS Gold Parquet 中采样打标行。

    Args:
        config: 评估配置。

    Returns:
        随机采样行，数量不超过配置的 sample_size。
    """
    # 检查是否强制使用 Spark 回退模式
    use_spark_fallback = os.getenv("USE_SPARK_FALLBACK", "0") == "1"
    if use_spark_fallback:
        LOGGER.info("USE_SPARK_FALLBACK=1, using pure Spark for data sampling.")
        try:
            return _sample_labeled_data_spark(config)
        except Exception as exc:
            if _is_hdfs_sampling_error(exc):
                LOGGER.warning(
                    "Spark sampling failed on HDFS (%s). Falling back to local CSV: %s",
                    exc,
                    config.local_labeled_csv_path,
                )
                return _sample_local_labeled_csv(config)
            raise
    
    # 尝试 Ray + Spark 混合模式
    try:
        return _sample_labeled_data_ray_spark_hybrid(config)
    except Exception as exc:
        LOGGER.warning("Hybrid mode failed (%s), falling back to pure Spark", exc)
        try:
            return _sample_labeled_data_spark(config)
        except Exception as spark_exc:
            if _is_hdfs_sampling_error(spark_exc):
                LOGGER.warning(
                    "Spark fallback also failed on HDFS (%s). Falling back to local CSV: %s",
                    spark_exc,
                    config.local_labeled_csv_path,
                )
                return _sample_local_labeled_csv(config)
            raise


def _sample_labeled_data_spark(config: EvalConfig) -> List[Dict[str, Any]]:
    """纯 Spark 回退：从打标数据中采样，不使用 Ray。"""
    spark = (
        SparkSession.builder.appName("evaluate-quality-spark")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    try:
        labeled_df = spark.read.parquet(config.gold_labeled_path)
        # 转换为 pandas 再采样
        pandas_df = labeled_df.toPandas()
        if pandas_df.empty:
            return []
        
        rows = pandas_df.to_dict("records")
        random.seed(config.random_seed)
        if len(rows) <= config.sample_size:
            return rows
        return random.sample(rows, config.sample_size)
    finally:
        spark.stop()


# ==================== 【段落 4】报告聚合与告警 ====================

def compute_report(
    config: EvalConfig,
    evaluated: Sequence[Tuple[str, JudgeResult]],
    functional_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """将评估指标聚合成报告字典。

    Args:
        config: 评估配置。
        evaluated: 已评估的 Judge 结果列表。

    Returns:
        含汇总与低分样本的报告字典。
    """
    task_scores = [item[1].task_completion for item in evaluated]
    hall_scores = [item[1].hallucination for item in evaluated]
    overall_scores = [item[1].overall for item in evaluated]

    if not overall_scores:
        return {
            "sample_count": 0,
            "task_completion_avg": 0.0,
            "hallucination_avg": 0.0,
            "overall_avg": 0.0,
            "pass": False,
            "threshold": config.quality_threshold,
            "low_score_examples": [],
        }

    low_items: List[Dict[str, Any]] = []
    for doc_id, result in evaluated:
        if result.overall < config.quality_threshold:
            low_items.append(
                {
                    "doc_id": doc_id,
                    "task_completion": result.task_completion,
                    "hallucination": result.hallucination,
                    "overall": round(result.overall, 2),
                    "reason": result.reason,
                }
            )

    low_items = sorted(low_items, key=lambda item: item["overall"])[:10]
    overall_avg = statistics.mean(overall_scores)
    report = {
        "sample_count": len(evaluated),
        "task_completion_avg": round(statistics.mean(task_scores), 3),
        "hallucination_avg": round(statistics.mean(hall_scores), 3),
        "overall_avg": round(overall_avg, 3),
        "overall_std": round(statistics.pstdev(overall_scores), 3) if len(overall_scores) > 1 else 0.0,
        "pass": overall_avg >= config.quality_threshold,
        "threshold": config.quality_threshold,
        "below_threshold_count": len(low_items),
        "low_score_examples": low_items,
    }
    if functional_metrics:
        report["functional_integrity"] = functional_metrics
    return report


def notify_if_needed(report: Dict[str, Any]) -> None:
    """当质量低于阈值时打印告警并模拟通知。

    Args:
        report: 已计算的质量报告。
    """
    if report.get("pass", False):
        print("Quality gate passed.")
        return

    warning_message = (
        "!!! QUALITY ALERT !!! "
        f"overall_avg={report.get('overall_avg')} < threshold={report.get('threshold')}"
    )
    print(warning_message)
    print("Simulated notification: [ALERT] Continuous Evaluation failed, please inspect low-score samples.")


def main() -> None:
    """执行持续评估并打印完整报告。"""
    setup_logging()
    config = load_config()

    samples = sample_labeled_data(config)
    if not samples:
        LOGGER.warning("No labeled data found at path: %s", config.gold_labeled_path)
        print(json.dumps({"error": "no_data", "path": config.gold_labeled_path}, ensure_ascii=False, indent=2))
        return

    LOGGER.info("Sampled %d records for evaluation.", len(samples))
    evaluated: List[Tuple[str, JudgeResult]] = []
    with httpx.Client(timeout=config.judge_timeout_seconds) as client:
        with ThreadPoolExecutor(max_workers=config.judge_concurrency) as executor:
            futures = [executor.submit(evaluate_one_sample, row, config, client) for row in samples]
            for future in as_completed(futures):
                evaluated.append(future.result())

    functional_metrics = compute_functional_integrity_metrics(samples)
    report = compute_report(config, evaluated, functional_metrics=functional_metrics)
    notify_if_needed(report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
