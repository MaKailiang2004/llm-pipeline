#!/usr/bin/env bash
# 通过国产源 ModelScope 预下载 Qwen2.5-3B-Instruct，并落到固定本地目录。

set -euo pipefail

MODEL_NAME="${MODEL_NAME:-Qwen/Qwen2.5-3B-Instruct}"
MODEL_DIR="${MODEL_DIR:-/workspace/models/Qwen2.5-3B-Instruct}"
MODELSCOPE_CACHE="${MODELSCOPE_CACHE:-${HOME}/.cache/modelscope}"
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"

echo "=========================================="
echo "Downloading ${MODEL_NAME} from ModelScope..."
echo "Model directory: ${MODEL_DIR}"
echo "ModelScope cache: ${MODELSCOPE_CACHE}"
echo "=========================================="

python3 -m pip install -q -i "${PIP_INDEX_URL}" --trusted-host "${PIP_TRUSTED_HOST}" modelscope
mkdir -p "${MODEL_DIR}"

MODEL_NAME="${MODEL_NAME}" MODEL_DIR="${MODEL_DIR}" MODELSCOPE_CACHE="${MODELSCOPE_CACHE}" python3 - <<'PY'
import os
from modelscope import snapshot_download

model_dir = snapshot_download(
    os.environ["MODEL_NAME"],
    local_dir=os.environ["MODEL_DIR"],
    cache_dir=os.environ["MODELSCOPE_CACHE"],
    allow_file_pattern=["*.safetensors", "*.json", "*.txt", "*.py"],
    ignore_file_pattern=["*.bin", "*.msgpack"],
)
print(model_dir)
PY

echo "=========================================="
echo "Model downloaded successfully!"
echo "Size check:"
du -sh "${MODEL_DIR}" 2>/dev/null || echo "Model directory missing"
echo "=========================================="
