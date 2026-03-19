# vLLM Offline 模式压测工具链

用于 **Offline 模式**（Ray + 进程内 `LLM.generate()`）的 Ray 就绪检查、预热、真实流量压测和 GPU 监控。

> **说明**：本工具链与 `batch_labeling.py` 共用同一套 `OfflineVllmLabelerActor` 与配置，无 HTTP 服务、无 `/metrics`。详见 `MANUAL.md`。

---

## 1. 环境变量

在 `.env` 或运行时导出：

```bash
# 必填
MODEL_PATH="/workspace/models/Qwen2.5-3B-Instruct"   # 模型路径
MAX_MODEL_LEN=4096                                   # 业务预估最大上下文长度
EXPECTED_QPS=20                                      # 业务预估峰值 QPS
TEST_DATASET_PATH="./tools/vllm_bench/sample_dataset.jsonl"  # 压测数据

# 可选
VLLM_API_BASE="http://localhost:8000"                 # vLLM 服务地址
VLLM_GPU_MEMORY_UTIL=0.88                            # 显存利用率 (0.85-0.92)
VLLM_MAX_NUM_SEQS=64                                 # 最大并发序列数
VLLM_WARMUP_BATCHES=4                                # 预热批次数
VLLM_LOAD_TEST_DURATION=60                            # 压测时长(秒)
VLLM_MONITOR_INTERVAL=5                               # 监控轮询间隔(秒)
VLLM_MONITOR_CACHE_ALERT=0.95                         # KV Cache 告警阈值
```

---

## 2. 快速运行（复制即用）

```bash
cd /home/ubuntu/llm-pipeline

# 1. 配置环境变量
export MODEL_PATH="/workspace/models/Qwen2.5-3B-Instruct"
export MAX_MODEL_LEN=4096
export EXPECTED_QPS=20
export TEST_DATASET_PATH="$(pwd)/tools/vllm_bench/sample_dataset.jsonl"

# 2. 启动 vLLM（物理防 OOM）
# 宿主机无 vLLM 时会自动在 ray-head 容器内执行，请确保 docker compose 已启动 ray-head
bash tools/vllm_bench/vllm_serve.sh

# 3. 另开终端：预热（等服务加载完成后执行）
python3 tools/vllm_bench/vllm_warmup.py

# 4. 另开终端：后台监控
python3 tools/vllm_bench/vllm_monitor.py --poll-interval 2

# 5. 运行压测
python3 tools/vllm_bench/vllm_load_test.py --duration 120
```

---

## 3. 完整运行步骤（分步说明）

### Step 0：准备压测数据

JSONL 每行格式：`{"prompt": "评论文本", "output_len": 预估生成长度}`。

```bash
# 使用示例数据
export TEST_DATASET_PATH="$(pwd)/tools/vllm_bench/sample_dataset.jsonl"

# 或从业务数据生成（需包含 prompt 和 output_len 字段）
# head -1000 /path/to/your/reviews.jsonl | jq -c '{prompt: .text, output_len: 64}' > /tmp/load_test.jsonl
```

### Step 1：启动 vLLM 服务（物理防 OOM 配置）

```bash
cd /home/ubuntu/llm-pipeline

# 加载环境变量
set -a && source .env 2>/dev/null || true && set +a

# 启动 vLLM（前台运行，便于观察日志）
# 若宿主机无 vLLM，脚本会自动在 ray-head 容器内执行
bash tools/vllm_bench/vllm_serve.sh

# 或后台运行
nohup bash tools/vllm_bench/vllm_serve.sh > logs/vllm_serve.log 2>&1 &
```

**验证**：`curl -s http://localhost:8000/health` 应返回 `{"status":"ok"}`。

### Step 2：预热与 CUDA 图编译

```bash
# 等服务就绪后执行（通常需等待模型加载完成，约 1-3 分钟）
python3 tools/vllm_bench/vllm_warmup.py
```

**成功输出**：`Warmup completed successfully`。若出现连接拒绝，检查服务是否已就绪。

### Step 3：启动监控探针（另开终端）

```bash
python3 tools/vllm_bench/vllm_monitor.py --poll-interval 2 &
MONITOR_PID=$!
```

监控会持续轮询 `/metrics`，当 KV Cache 使用率 >95% 或出现排队积压时打印告警。

### Step 4：执行压测

```bash
python3 tools/vllm_bench/vllm_load_test.py --duration 120
```

压测结束后会输出汇总：总请求数、成功/失败、实际 QPS、P50/P95/P99 延迟、超时与错误统计。

### Step 5：停止监控

```bash
kill $MONITOR_PID
```

---

## 3. 一键脚本（开发/联调用）

```bash
#!/bin/bash
cd /home/ubuntu/llm-pipeline

source .env 2>/dev/null || true
export TEST_DATASET_PATH="${TEST_DATASET_PATH:-$(pwd)/tools/vllm_bench/sample_dataset.jsonl}"

# 1. 启动 vLLM（若未启动）
if ! curl -s --connect-timeout 2 "${VLLM_API_BASE:-http://localhost:8000}/health" >/dev/null 2>&1; then
  echo "[INFO] Starting vLLM server..."
  nohup bash tools/vllm_bench/vllm_serve.sh > logs/vllm_serve.log 2>&1 &
  sleep 90  # 等待模型加载
fi

# 2. 预热
python tools/vllm_bench/vllm_warmup.py || exit 1

# 3. 后台监控
python tools/vllm_bench/vllm_monitor.py &
MONITOR_PID=$!

# 4. 压测
python tools/vllm_bench/vllm_load_test.py

# 5. 停止监控
kill $MONITOR_PID 2>/dev/null || true
```

---

## 4. 排障指南

| 现象 | 可能原因 | 处理建议 |
|------|----------|----------|
| `Connection refused` 启动失败 | 端口被占用或 vLLM 未启动 | `lsof -i :8000` 检查；调整 `VLLM_PORT` |
| OOM 崩溃 | `gpu-memory-utilization` 或 `max-num-seqs` 过高 | 降低 `VLLM_GPU_MEMORY_UTIL` 至 0.85；减小 `VLLM_MAX_NUM_SEQS` |
| 预热/压测连接超时 | 服务未就绪或网络问题 | 延长等待时间；检查 `VLLM_API_BASE` |
| KV Cache 使用率告警 | 并发或长序列超出预估 | 减小 `max-num-seqs`；降低 `MAX_MODEL_LEN` |
| 排队积压持续 | 峰值 QPS 超过服务能力 | 降低 `EXPECTED_QPS`；增加 GPU 或横向扩容 |
| `/metrics` 404 | vLLM 版本过旧 | 升级 vLLM 至 0.6+ 并启用 `--metric-logging-interval` |
| 压测实际 QPS 远低于预期 | 单请求延迟高 | 检查 `max-num-batched-tokens`；适当提高 `max-num-seqs` |

---

## 5. vLLM 启动参数推导逻辑

| 参数 | 推导依据 | 默认值 |
|------|----------|--------|
| `--max-model-len` | 业务 P99 上下文长度，避免超长请求 OOM | `MAX_MODEL_LEN` |
| `--max-num-seqs` | 预期 QPS × 单请求平均耗时（秒），预留余量 | `min(128, 4 * EXPECTED_QPS)` |
| `--gpu-memory-utilization` | 留 10-15% 余量防 OOM 抖动 | 0.88 |
| `--max-num-batched-tokens` | Prefill 批次上限，接近 `max-model-len` | `MAX_MODEL_LEN` |

---

## 6. 文件说明

| 文件 | 用途 |
|------|------|
| `vllm_serve.sh` | 按 OOM 防护参数启动 vLLM HTTP 服务 |
| `vllm_warmup.py` | 发送边界 dummy 请求，触发 CUDA 图编译与 KV cache 预热 |
| `vllm_load_test.py` | 从 JSONL 读取真实样本，按泊松到达率异步压测 |
| `vllm_monitor.py` | 轮询 `/metrics`，监控 KV cache、排队、吞吐并告警 |
| `sample_dataset.jsonl` | 示例压测数据 |
