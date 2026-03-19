#!/usr/bin/env python3
"""Offline 模式 vLLM 预热：Ray Actor + label_batch。

在 Ray OfflineVllmLabelerActor 就绪后执行，发送 Dummy 批次到 actor.label_batch.remote()，
触发模型加载与 CUDA Graph 编译，避免压测首批请求的 JIT 卡顿和 OOM。

使用方式:
    python vllm_warmup.py
    # 在 ray-head 内执行以使用集群 GPU:
    docker exec -it ray-head bash -lc "cd /workspace && python tools/vllm_bench/vllm_warmup.py"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

import ray
from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

from ml_pipeline.batch_labeling import OfflineVllmLabelerActor, get_actor_config_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)

RAY_ADDRESS = os.getenv("RAY_ADDRESS", "ray://ray-head:10001")
INFERENCE_BATCH_SIZE = int(os.getenv("RAY_INFERENCE_BATCH_SIZE", "32"))
OFFLINE_MAX_MODEL_LEN = int(os.getenv("VLLM_OFFLINE_MAX_MODEL_LEN", "2048"))
OFFLINE_MAX_NUM_SEQS = int(os.getenv("VLLM_OFFLINE_MAX_NUM_SEQS", "128"))


def _create_dummy_rows(count: int, prompt_len_chars: int = 50) -> List[Dict[str, Any]]:
    """生成 Dummy 行，含 text 字段供 label_batch 使用。"""
    pad = "测" * max(1, prompt_len_chars // 2)
    return [{"text": f"预热批次{i}：{pad}"} for i in range(count)]


def warmup_offline(ray_address: str) -> None:
    """通过 Ray Actor 执行 Offline 预热。"""
    ray.init(address=ray_address, ignore_reinit_error=True)
    config_dict = get_actor_config_dict()
    actor = OfflineVllmLabelerActor.remote(config_dict)

    # 阶段 1：短批次，触发模型加载
    short_rows = _create_dummy_rows(min(4, INFERENCE_BATCH_SIZE), 20)
    LOGGER.info("Warmup phase 1: short batch (%d rows)", len(short_rows))
    ray.get(actor.label_batch.remote(short_rows))
    LOGGER.info("Phase 1 OK")

    # 阶段 2：满批次，触发 max_num_seqs 路径
    batch_rows = _create_dummy_rows(min(INFERENCE_BATCH_SIZE, 32), 40)
    LOGGER.info("Warmup phase 2: full batch (%d rows)", len(batch_rows))
    ray.get(actor.label_batch.remote(batch_rows))
    LOGGER.info("Phase 2 OK")

    # 阶段 3：中等长度，模拟真实 chunked prefill
    medium_rows = _create_dummy_rows(8, max(100, OFFLINE_MAX_MODEL_LEN // 10))
    LOGGER.info("Warmup phase 3: medium-length batch (%d rows)", len(medium_rows))
    ray.get(actor.label_batch.remote(medium_rows))
    LOGGER.info("Phase 3 OK")

    LOGGER.info("Offline warmup complete. vLLM KV cache and CUDA graphs are primed.")


def main() -> int:
    global RAY_ADDRESS
    parser = argparse.ArgumentParser(description="Offline vLLM warmup via Ray Actor")
    parser.add_argument("--ray-address", default=RAY_ADDRESS, help="Ray cluster address")
    args = parser.parse_args()

    try:
        warmup_offline(args.ray_address)
    except Exception as e:
        LOGGER.exception("Warmup failed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
