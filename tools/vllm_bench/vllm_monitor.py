#!/usr/bin/env python3
"""vLLM Offline 模式监控：Ray 资源与 GPU 使用率。

Offline 模式无 HTTP /metrics，本脚本轮询 Ray cluster 资源与 nvidia-smi，
输出 GPU 显存、利用率及 Ray 槽位使用情况。

用法:
    python vllm_monitor.py
    python vllm_monitor.py --interval 2 --gpu-alert 90
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

RAY_ADDRESS = os.getenv("RAY_ADDRESS", "ray://ray-head:10001")
POLL_INTERVAL_SEC = float(os.getenv("VLLM_MONITOR_INTERVAL", "2"))
GPU_UTIL_ALERT_PERC = float(os.getenv("VLLM_GPU_UTIL_ALERT_PERC", "95"))
GPU_MEM_ALERT_PERC = float(os.getenv("VLLM_GPU_MEM_ALERT_PERC", "95"))


def fetch_ray_resources() -> dict:
    """获取 Ray 集群资源。不 shutdown，避免 monitor 频繁重连。"""
    import ray
    ray.init(address=RAY_ADDRESS, ignore_reinit_error=True)
    cluster = ray.cluster_resources()
    available = ray.available_resources()
    return {
        "gpu_total": cluster.get("GPU", 0),
        "gpu_available": available.get("GPU", 0),
        "memory_total_gb": cluster.get("memory", 0) / (1024**3),
        "memory_available_gb": available.get("memory", 0) / (1024**3),
    }


def fetch_nvidia_smi() -> list[dict]:
    """解析 nvidia-smi 输出，返回每卡显存与利用率。"""
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,memory.used,memory.total,utilization.gpu", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if out.returncode != 0:
            return []
        gpus = []
        for line in out.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 4:
                idx, mem_used, mem_total, util = parts[0], parts[1], parts[2], parts[3]
                mem_used = int(mem_used) if mem_used.isdigit() else 0
                mem_total = int(mem_total) if mem_total.isdigit() else 1
                util = int(util) if util.isdigit() else 0
                gpus.append({
                    "index": idx,
                    "mem_used_mb": mem_used,
                    "mem_total_mb": mem_total,
                    "mem_perc": 100.0 * mem_used / mem_total if mem_total else 0,
                    "util_perc": util,
                })
        return gpus
    except Exception:
        return []


def main() -> None:
    parser = argparse.ArgumentParser(description="vLLM Offline 监控：Ray + nvidia-smi")
    parser.add_argument("--ray-address", default=RAY_ADDRESS, help="Ray 地址")
    parser.add_argument("--interval", "-i", type=float, default=POLL_INTERVAL_SEC, help="轮询间隔(秒)")
    parser.add_argument("--gpu-alert", type=float, default=GPU_UTIL_ALERT_PERC, help="GPU 利用率告警阈值(%)")
    parser.add_argument("--mem-alert", type=float, default=GPU_MEM_ALERT_PERC, help="显存使用率告警阈值(%)")
    parser.add_argument("--once", action="store_true", help="只拉取一次并打印")
    args = parser.parse_args()

    global RAY_ADDRESS
    RAY_ADDRESS = args.ray_address

    def do_poll() -> None:
        try:
            ray_res = fetch_ray_resources()
        except Exception as e:
            print(f"[ERROR] Ray: {e}", file=sys.stderr)
            ray_res = {}
        gpus = fetch_nvidia_smi()

        ts = time.strftime("%H:%M:%S", time.localtime())
        line = f"[{ts}] Ray GPU: total={ray_res.get('gpu_total', '?')} available={ray_res.get('gpu_available', '?')}"
        if ray_res.get("memory_total_gb"):
            line += f" | mem_avail={ray_res.get('memory_available_gb', 0):.1f}GB"
        print(line)

        for g in gpus:
            msg = f"  GPU{g['index']}: mem={g['mem_perc']:.0f}% util={g['util_perc']}%"
            print(msg)
            if g["mem_perc"] >= args.mem_alert:
                print(f"    >>> [ALERT] 显存使用率 {g['mem_perc']:.0f}% >= {args.mem_alert}%", file=sys.stderr)
            if g["util_perc"] >= args.gpu_alert:
                print(f"    >>> [ALERT] GPU 利用率 {g['util_perc']}% >= {args.gpu_alert}%", file=sys.stderr)

    if args.once:
        do_poll()
        return

    print(f"Monitoring Ray + nvidia-smi every {args.interval}s (gpu_alert>={args.gpu_alert}%, mem_alert>={args.mem_alert}%)")
    print("Press Ctrl+C to stop.")
    while True:
        do_poll()
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
