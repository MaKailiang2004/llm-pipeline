#!/usr/bin/env python3
"""Offline 模式压测：泊松到达、JSONL 数据集、Ray Actor label_batch.remote()。

从 JSONL 读取 prompt/text，构建 rows 格式，按目标 QPS 泊松分布提交到
OfflineVllmLabelerActor.label_batch.remote()，观测 OOM、超时与吞吐。
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)


@dataclass
class LoadTestConfig:
    """压测配置。"""
    test_dataset_path: str
    expected_qps: float
    duration_seconds: float
    request_timeout_seconds: float
    inference_batch_size: int
    ray_address: str


def load_config() -> LoadTestConfig:
    dataset = os.getenv("TEST_DATASET_PATH", "")
    qps = float(os.getenv("EXPECTED_QPS", "20"))
    duration = float(os.getenv("LOAD_TEST_DURATION_SECONDS", "120"))
    timeout = float(os.getenv("LOAD_TEST_REQUEST_TIMEOUT", "120"))
    batch_size = int(os.getenv("RAY_INFERENCE_BATCH_SIZE", "32"))
    ray_addr = os.getenv("RAY_ADDRESS", "ray://ray-head:10001")
    return LoadTestConfig(
        test_dataset_path=dataset,
        expected_qps=qps,
        duration_seconds=duration,
        request_timeout_seconds=timeout,
        inference_batch_size=batch_size,
        ray_address=ray_addr,
    )


def load_jsonl_dataset(path: str) -> List[Dict[str, Any]]:
    """从 JSONL 加载，每行需含 prompt 或 text。"""
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                records.append(obj)
            except json.JSONDecodeError as e:
                print(f"[WARN] Line {i} invalid JSON: {e}", file=sys.stderr)
    return records


def record_to_row(record: Dict[str, Any]) -> Dict[str, Any]:
    """将 JSONL 记录转为 label_batch 所需的 row（含 text）。"""
    text = record.get("prompt") or record.get("text", "")
    return {"text": str(text), **{k: v for k, v in record.items() if k not in ("prompt", "text")}}


def next_poisson_arrival(rate: float) -> float:
    """泊松到达：返回下一次到达的间隔秒数。"""
    if rate <= 0:
        return 1.0
    return random.expovariate(rate)


def run_load_test(config: LoadTestConfig) -> None:
    """主压测逻辑：泊松到达、Ray Actor 提交、统计报告。"""
    import ray

    from ml_pipeline.batch_labeling import OfflineVllmLabelerActor, get_actor_config_dict

    if not config.test_dataset_path or not os.path.isfile(config.test_dataset_path):
        print(f"[ERR] TEST_DATASET_PATH 无效或文件不存在: {config.test_dataset_path}", file=sys.stderr)
        sys.exit(1)

    records = load_jsonl_dataset(config.test_dataset_path)
    if not records:
        print("[ERR] 数据集为空", file=sys.stderr)
        sys.exit(1)

    ray.init(address=config.ray_address, ignore_reinit_error=True)
    config_dict = get_actor_config_dict()
    default_actor_count = max(1, int(ray.available_resources().get("GPU", 0)))
    actor_count = int(os.getenv("RAY_LABEL_ACTOR_COUNT", str(default_actor_count)))
    actor_count = max(1, min(actor_count, default_actor_count))

    actors = [OfflineVllmLabelerActor.remote(config_dict) for _ in range(actor_count)]

    print(f"[INFO] 加载 {len(records)} 条样本，目标 QPS={config.expected_qps}，"
          f"持续 {config.duration_seconds}s，actors={actor_count}，batch_size={config.inference_batch_size}")

    sent = 0
    results: List[tuple[int, float, Optional[str], bool]] = []
    # QPS = prompts/sec，每批 inference_batch_size 条 → 批到达率 = QPS / batch_size
    rate = config.expected_qps / max(1, config.inference_batch_size)
    next_arrival = 0.0
    end_time = time.perf_counter() + config.duration_seconds

    futures: List[tuple[int, Any]] = []  # (req_id, future)
    in_flight: Dict[int, float] = {}     # req_id -> start_time

    while time.perf_counter() < end_time or futures:
        now = time.perf_counter()

        # 提交新请求（泊松到达）
        while now >= next_arrival and now < end_time:
            batch_rows: List[Dict[str, Any]] = []
            for _ in range(config.inference_batch_size):
                idx = (sent + len(batch_rows)) % len(records)
                batch_rows.append(record_to_row(records[idx]))
            actor = actors[sent % actor_count]
            fut = actor.label_batch.remote(batch_rows)
            futures.append((sent, fut))
            in_flight[sent] = now
            sent += len(batch_rows)
            next_arrival = now + next_poisson_arrival(rate)
            now = time.perf_counter()

        # 收割已完成的 future（非阻塞轮询）
        if futures:
            refs = [f for _, f in futures]
            ref_to_id = {id(f): req_id for req_id, f in futures}
            ready, _ = ray.wait(refs, num_returns=min(len(refs), 50), timeout=0.01)
            remaining = []
            for req_id, fut in futures:
                if fut in ready:
                    elapsed = time.perf_counter() - in_flight.get(req_id, time.perf_counter())
                    try:
                        ray.get(fut)
                        results.append((req_id, elapsed, None, True))
                    except Exception as e:
                        results.append((req_id, elapsed, str(e)[:200], False))
                else:
                    remaining.append((req_id, fut))
            futures = remaining

        if now < end_time and not futures:
            time.sleep(0.01)

    # 等待剩余
    for req_id, fut in futures:
        start = in_flight.get(req_id, time.perf_counter())
        try:
            ray.get(fut)
            results.append((req_id, time.perf_counter() - start, None, True))
        except Exception as e:
            results.append((req_id, time.perf_counter() - start, str(e)[:200], False))

    ok_count = sum(1 for _, _, _, ok in results if ok)
    err_count = len(results) - ok_count
    latencies = [lat for _, lat, _, ok in results if ok]
    oom_count = sum(1 for _, _, err, _ in results if err and "out of memory" in (err or "").lower())
    timeout_count = sum(1 for _, _, err, _ in results if err and "timeout" in (err or "").lower())

    elapsed_total = config.duration_seconds
    actual_qps = sent / elapsed_total if elapsed_total > 0 else 0
    success_qps = ok_count / elapsed_total if elapsed_total > 0 else 0

    print("\n========== 压测报告 (Offline) ==========")
    print(f"发送 batch 数: {len(results)}, 总记录数: {sent}, 成功: {ok_count}, 失败: {err_count}")
    print(f"OOM 次数: {oom_count}, 超时/异常: {timeout_count}")
    print(f"实际 QPS(记录): {actual_qps:.2f}, 成功 QPS: {success_qps:.2f}")
    if latencies:
        print(f"延迟 P50: {statistics.median(latencies)*1000:.0f} ms")
        if len(latencies) >= 10:
            latencies.sort()
            p99_idx = max(0, int(len(latencies) * 0.99) - 1)
            print(f"延迟 P99: {latencies[p99_idx]*1000:.0f} ms")

    if err_count > 0:
        sample_errors = [(i, e) for i, _, e, _ in results if e][:5]
        print("失败样本:")
        for rid, err in sample_errors:
            print(f"  req {rid}: {err}")


def main() -> None:
    parser = argparse.ArgumentParser(description="vLLM Offline 压测（泊松到达）")
    parser.add_argument("--dataset", default=None, help="JSONL 数据集路径")
    parser.add_argument("--qps", type=float, default=None, help="目标 QPS")
    parser.add_argument("--duration", type=float, default=None, help="压测时长(s)")
    parser.add_argument("--ray-address", default=None, help="Ray cluster 地址")
    args = parser.parse_args()

    config = load_config()
    if args.dataset:
        config.test_dataset_path = args.dataset
    if args.qps is not None:
        config.expected_qps = args.qps
    if args.duration is not None:
        config.duration_seconds = args.duration
    if args.ray_address:
        config.ray_address = args.ray_address

    run_load_test(config)


if __name__ == "__main__":
    main()
