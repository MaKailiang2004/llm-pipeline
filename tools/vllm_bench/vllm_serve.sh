#!/usr/bin/env bash
# =============================================================================
# Offline 模式 vLLM 压测：Ray 就绪检查
# =============================================================================
# Offline 模式不启动 HTTP 服务，vLLM 在 Ray Actor 内进程内加载。
# 本脚本仅检查 Ray 集群是否就绪，供 warmup/load_test 使用。
#
# 用法:
#   bash tools/vllm_bench/vllm_serve.sh
#   # 在 ray-head 内执行以使用集群 GPU:
#   docker exec -it ray-head bash -lc "cd /workspace && bash tools/vllm_bench/vllm_serve.sh"
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

RAY_ADDRESS="${RAY_ADDRESS:-ray://ray-head:10001}"

# 若当前环境无 ray，则在 ray-head 容器内执行
if ! python3 -c "import ray" 2>/dev/null; then
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^ray-head$'; then
    echo "[vllm_serve] Ray not found locally, re-running inside ray-head container..."
    exec docker exec -it ray-head bash -lc "cd /workspace && export RAY_ADDRESS='${RAY_ADDRESS}' && bash tools/vllm_bench/vllm_serve.sh"
  else
    echo "[vllm_serve] ERROR: Ray not found and ray-head container not running." >&2
    echo "  Start Ray first: docker-compose up -d ray-head" >&2
    exit 1
  fi
fi

echo "[vllm_serve] Offline mode: checking Ray cluster at $RAY_ADDRESS"

if python3 -c "
import os
import ray
ray.init(address=os.environ.get('RAY_ADDRESS', 'ray://ray-head:10001'), ignore_reinit_error=True)
resources = ray.cluster_resources()
gpus = resources.get('GPU', 0)
print('GPU count:', gpus)
ray.shutdown()
" 2>/dev/null; then
  echo "[vllm_serve] Ray cluster ready. Proceed with: python3 tools/vllm_bench/vllm_warmup.py"
  exit 0
else
  echo "[vllm_serve] ERROR: Failed to connect to Ray or no GPU available." >&2
  exit 1
fi
