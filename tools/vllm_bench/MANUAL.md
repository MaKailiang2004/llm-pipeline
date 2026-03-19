# vLLM Offline 模式压测工具链 使用说明手册

本文档为「Ray 就绪检查-预热-压测-监控」自动化工具链的详细使用说明，面向 **Offline 模式**（Ray + 进程内 `LLM.generate()`），与 `batch_labeling.py` 共用同一套 Actor 与配置。

---

## 目录

1. [概述与适用场景](#1-概述与适用场景)
2. [架构与组件](#2-架构与组件)
3. [环境准备](#3-环境准备)
4. [环境变量完整清单](#4-环境变量完整清单)
5. [压测数据准备](#5-压测数据准备)
6. [分步操作指南](#6-分步操作指南)
7. [输出指标解读](#7-输出指标解读)
8. [参数调优指南](#8-参数调优指南)
9. [排障与 FAQ](#9-排障与-faq)

---

## 1. 概述与适用场景

### 1.1 功能定位

本工具链针对 **vLLM Offline 模式**（Ray + `OfflineVllmLabelerActor` + `llm.generate()`）提供：

| 能力 | 说明 |
|------|------|
| **Ray 就绪检查** | 校验 Ray 集群可连接、有可用 GPU |
| **预热与 CUDA 图编译** | 通过 `actor.label_batch.remote()` 发送 Dummy 批次，触发模型加载与最大 Batch 路径编译 |
| **真实流量压测** | 从 JSONL 读取样本，按泊松分布提交到 Ray Actor，观测 OOM、超时与吞吐 |
| **实时监控告警** | 轮询 Ray 资源与 nvidia-smi，监控显存与 GPU 利用率 |

### 1.2 与主流水线的关系

- **主流水线**（`batch_labeling.py`）与 **本压测工具链** 使用相同的 `OfflineVllmLabelerActor` 与配置
- 压测时无需 HDFS/Spark，仅需 Ray 集群与 GPU
- 压测参数（`RAY_INFERENCE_BATCH_SIZE`、`VLLM_OFFLINE_*` 等）与 `.env` 一致

---

## 2. 架构与组件

```
┌─────────────────────────────────────────────────────────────────┐
│                        宿主机 / 开发机                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ vllm_warmup  │  │ vllm_load_test│  │ vllm_monitor │           │
│  │   (Python)   │  │   (Python)    │  │   (Python)   │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
│         │                  │                  │                   │
│         └──────────────────┼──────────────────┘                   │
│                            │ Ray RPC                              │
└────────────────────────────┼─────────────────────────────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────────┐
│                   ray-head 容器 (GPU)                             │
│  ┌────────────────────────▼─────────────────────────┐           │
│  │ OfflineVllmLabelerActor (Ray Actor)               │           │
│  │ - 进程内加载 vLLM LLM                             │           │
│  │ - label_batch(rows) -> llm.generate(prompts)      │           │
│  │ - 无 HTTP、无 /metrics                             │           │
│  └──────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

| 文件 | 作用 |
|------|------|
| `vllm_serve.sh` | Offline 模式：检查 Ray 集群就绪，不启动 HTTP 服务 |
| `vllm_warmup.py` | 通过 Ray Actor 发送 Dummy 批次预热 |
| `vllm_load_test.py` | 泊松到达 + JSONL 样本，提交到 `label_batch.remote()` |
| `vllm_monitor.py` | 轮询 Ray + nvidia-smi，无 `/metrics` |
| `sample_dataset.jsonl` | 示例压测数据 |

---

## 3. 环境准备

### 3.1 系统要求

- Linux（Ubuntu 20.04+）
- Docker、Docker Compose
- NVIDIA GPU、驱动与 CUDA
- Python 3.10+（宿主机需能连 Ray，推荐在 ray-head 内跑 warmup/load_test）

### 3.2 依赖

与主项目一致，需安装 `ray`、`vllm`、`python-dotenv`。压测脚本会导入 `ml_pipeline.batch_labeling`。

### 3.3 启动 ray-head 容器

```bash
cd /home/ubuntu/llm-pipeline
docker-compose up -d ray-head
docker ps | grep ray-head   # 确认 Up
```

### 3.4 模型与路径

- 模型路径由 `VLLM_MODEL_NAME` 或 `MODEL_PATH` 指定，与 `batch_labeling` 一致
- 默认 `/workspace/models/Qwen2.5-3B-Instruct`（容器内）

---

## 4. 环境变量完整清单

### 4.1 Ray 与 Actor

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RAY_ADDRESS` | `ray://ray-head:10001` | Ray 集群地址 |
| `RAY_LABEL_ACTOR_COUNT` | 自动按 GPU 数 | Actor 数量上限 |
| `RAY_INFERENCE_BATCH_SIZE` | `32` | 每批记录数（QPS = 批到达率 × batch_size） |
| `RAY_LABEL_NUM_GPUS_PER_ACTOR` | `1` | 每个 Actor 占用 GPU 数 |

### 4.2 vLLM Offline 引擎（与 batch_labeling 共用）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `VLLM_MODEL_NAME` | `Qwen/Qwen2.5-3B-Instruct` | 模型路径或 ID |
| `VLLM_OFFLINE_GPU_MEMORY_UTILIZATION` | `0.90` | 显存利用率 |
| `VLLM_OFFLINE_MAX_NUM_SEQS` | `128` | 最大并发序列数 |
| `VLLM_OFFLINE_MAX_MODEL_LEN` | `2048` | 最大上下文长度 |
| `VLLM_OFFLINE_ENABLE_PREFIX_CACHING` | `1` | 前缀缓存 |

### 4.3 压测相关

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TEST_DATASET_PATH` | （无） | JSONL 压测数据路径，**必填** |
| `EXPECTED_QPS` | `20` | 目标 QPS（prompts/秒） |
| `LOAD_TEST_DURATION_SECONDS` | `120` | 压测时长（秒） |
| `LOAD_TEST_REQUEST_TIMEOUT` | `120` | 单批超时（秒） |

### 4.4 监控相关

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `VLLM_MONITOR_INTERVAL` | `2` | 轮询间隔（秒） |
| `VLLM_GPU_UTIL_ALERT_PERC` | `95` | GPU 利用率告警阈值（%） |
| `VLLM_GPU_MEM_ALERT_PERC` | `95` | 显存使用率告警阈值（%） |

---

## 5. 压测数据准备

### 5.1 JSONL 格式

每行一个 JSON 对象，需含 `prompt` 或 `text`（供 `label_batch` 的 `row["text"]` 使用）：

| 字段 | 必填 | 说明 |
|------|------|------|
| `prompt` | 与 `text` 二选一 | 用户输入文本 |
| `text` | 与 `prompt` 二选一 | 同上 |
| `output_len` | 否 | Offline 打标不单独控制，可忽略 |

**示例行：**

```json
{"prompt": "这是一条测试评论：手机很好用，续航不错。", "output_len": 64}
{"text": "物流太慢了，等了半个月才到。"}
```

### 5.2 使用自带示例

```bash
export TEST_DATASET_PATH="$(pwd)/tools/vllm_bench/sample_dataset.jsonl"
```

---

## 6. 分步操作指南

### 6.1 快速命令（复制即用）

```bash
cd /home/ubuntu/llm-pipeline

# 1. 激活虚拟环境（可选）
source .venv/bin/activate

# 2. 配置环境变量
export TEST_DATASET_PATH="$(pwd)/tools/vllm_bench/sample_dataset.jsonl"
export EXPECTED_QPS=20

# 3. 检查 Ray 就绪（不启动 HTTP 服务）
bash tools/vllm_bench/vllm_serve.sh

# 4. 预热（在 ray-head 内执行以使用 GPU）
docker exec -it ray-head bash -lc "cd /workspace && python3 tools/vllm_bench/vllm_warmup.py"

# 5. 新开终端：监控
python3 tools/vllm_bench/vllm_monitor.py --interval 2

# 6. 新开终端：压测（建议在 ray-head 内执行）
docker exec -it ray-head bash -lc "cd /workspace && python3 tools/vllm_bench/vllm_load_test.py --duration 120"
```

### 6.2 步骤 1：Ray 就绪检查

```bash
bash tools/vllm_bench/vllm_serve.sh
```

- 若宿主机无 `ray`，脚本会自动在 ray-head 容器内重跑
- 成功输出：`[vllm_serve] Ray cluster ready. Proceed with: python3 tools/vllm_bench/vllm_warmup.py`

### 6.3 步骤 2：预热

```bash
# 在 ray-head 内执行（GPU 在容器内）
docker exec -it ray-head bash -lc "cd /workspace && python3 tools/vllm_bench/vllm_warmup.py"
```

**可选参数：** `--ray-address ray://ray-head:10001`

### 6.4 步骤 3：监控

```bash
python3 tools/vllm_bench/vllm_monitor.py --interval 2
```

- 输出 Ray GPU 槽位与 nvidia-smi 显存/利用率
- 显存或 GPU 利用率超阈值时告警

### 6.5 步骤 4：压测

```bash
# 在 ray-head 内执行
docker exec -it ray-head bash -lc "cd /workspace && python3 tools/vllm_bench/vllm_load_test.py --duration 120"
```

**可选参数：**

```bash
python3 tools/vllm_bench/vllm_load_test.py \
  --dataset "$TEST_DATASET_PATH" \
  --qps 20 \
  --duration 120 \
  --ray-address ray://ray-head:10001
```

---

## 7. 输出指标解读

### 7.1 压测报告（vllm_load_test）

| 指标 | 含义 |
|------|------|
| 发送 batch 数 | 提交到 Actor 的批次数 |
| 总记录数 | 总 prompt 数 |
| 成功 / 失败 | 成功与失败批次数 |
| OOM 次数 | 显存不足导致的失败数 |
| 实际 QPS(记录) | 总记录数 / 时长 |
| 成功 QPS | 成功记录数 / 时长 |
| 延迟 P50 / P99 | 单批从提交到完成的延迟（ms） |

### 7.2 监控告警（vllm_monitor）

| 告警 | 含义 |
|------|------|
| `[ALERT] 显存使用率` | 显存使用超过阈值 |
| `[ALERT] GPU 利用率` | GPU 利用率超过阈值 |

---

## 8. 参数调优指南

### 8.1 出现 OOM 时

1. 降低 `VLLM_OFFLINE_GPU_MEMORY_UTILIZATION`：0.90 → 0.85
2. 降低 `VLLM_OFFLINE_MAX_MODEL_LEN`：2048 → 1024
3. 降低 `RAY_INFERENCE_BATCH_SIZE`：32 → 16

### 8.2 实际 QPS 远低于目标时

1. 适当提高 `VLLM_OFFLINE_MAX_NUM_SEQS`
2. 检查单批延迟，若 P99 很高，考虑减小 `RAY_INFERENCE_BATCH_SIZE` 或 `VLLM_OFFLINE_MAX_MODEL_LEN`

---

## 9. 排障与 FAQ

### 9.1 常见错误

| 现象 | 原因 | 处理 |
|------|------|------|
| `Ray not found` | 宿主机未装 ray | 在 ray-head 内执行：`docker exec -it ray-head bash -lc "..."` |
| `Failed to connect to Ray` | Ray 集群未启动 | `docker-compose up -d ray-head` |
| `insufficient GPU` | 无可用 GPU | 确认 ray-head 能访问 GPU |
| `TEST_DATASET_PATH 无效` | 未设置或文件不存在 | `export TEST_DATASET_PATH="$(pwd)/tools/vllm_bench/sample_dataset.jsonl"` |
| `ModuleNotFoundError: ml_pipeline` | PYTHONPATH 未包含 src | 脚本已自动添加，确保从项目根执行 |

### 9.2 一键压测脚本示例

```bash
#!/bin/bash
set -e
cd /home/ubuntu/llm-pipeline
source .venv/bin/activate 2>/dev/null || true

export TEST_DATASET_PATH="${TEST_DATASET_PATH:-$(pwd)/tools/vllm_bench/sample_dataset.jsonl}"
export EXPECTED_QPS=20

bash tools/vllm_bench/vllm_serve.sh || { echo "Ray not ready"; exit 1; }
docker exec ray-head bash -lc "cd /workspace && python3 tools/vllm_bench/vllm_warmup.py" || exit 1
python3 tools/vllm_bench/vllm_monitor.py --interval 2 &
MONITOR_PID=$!
docker exec ray-head bash -lc "cd /workspace && python3 tools/vllm_bench/vllm_load_test.py --duration 120"
kill $MONITOR_PID 2>/dev/null || true
echo "[OK] Offline load test complete."
```
