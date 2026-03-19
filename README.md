# 大模型 RAG 数据流水线（中文 README）

本项目是一个可在单机云服务器（推荐 GPU）运行的端到端数据流水线，覆盖：

- Kafka 实时接入
- Spark/HDFS 分层存储（Bronze/Silver/Gold）
- Ray（2 Worker 冗余）+ vLLM Offline 批处理推理自动标注（每 GPU 常驻 Actor，最大化吞吐）
- BGE 向量化
- LLM-as-a-Judge 质量评估

当前推荐使用一键脚本：`scripts/run_all.sh`。

---

## 1. 技术栈

- Python 3.10+
- Docker Compose
- Apache Kafka
- Hadoop HDFS（单节点）
- Apache Spark / PySpark
- Ray
- vLLM（Offline Batched Inference API，进程内推理）
- DashScope（第 6 步评估 API，可替换）

---

## 2. 流水线步骤（6 步）

1. **Kafka -> Bronze（流式）**  
   `ingestion.kafka_to_hdfs` 将 Kafka 原始评论写入 HDFS Bronze。
2. **数据生产**  
   `ingestion.producer` 将 `data/reviews.jsonl` 写入 Kafka。
3. **Bronze -> Silver（ETL）**  
   `etl.clean_and_dedup` 清洗、脱敏、近似去重（含两阶段加盐防倾斜，避免「默认好评」等热点导致 Executor OOM）。
4. **Silver -> Gold Labeled（打标）**  
   `ml_pipeline.batch_labeling` 使用 Ray + vLLM Offline 批量打标（含 Spark Fallback）。
5. **向量化**  
   `ml_pipeline.vectorization` 进行 Chunking + BGE 向量生成，写入 Gold Embeddings。
6. **质量评估**  
   `ml_pipeline.evaluate_quality` 对打标结果进行 LLM 评估与告警。

---

## 3. 关键目录

```text
.
├── .env
├── docker-compose.yml
├── scripts/
│   ├── run_all.sh
│   ├── run_poc.sh
│   └── generate_reviews.py
├── data/
│   ├── reviews.jsonl
│   └── input_documents.jsonl
├── logs/
└── src/
    ├── ingestion/
    ├── etl/
    ├── ml_pipeline/
    └── rag_poc/
```

---

## 4. 环境准备

1. 确认已安装 Docker；Compose 使用 `docker-compose`（v1）或 `docker compose`（v2）。
2. 若使用 GPU，确认 `nvidia-smi` 正常。
3. 检查 `.env` 中关键配置（至少以下项）：
   - `VLLM_MODEL_NAME`、`VLLM_OFFLINE_MAX_NUM_SEQS`、`VLLM_OFFLINE_GPU_MEMORY_UTILIZATION` 等离线推理参数
   - `RAY_ADDRESS`、`RAY_LABEL_NUM_GPUS_PER_ACTOR`（离线 vLLM Actor 的 GPU 资源声明）
   - 多机 Ray 时还需检查 `RAY_HEAD_NODE_IP`、`RAY_CLUSTER_JOIN_ADDRESS`、`STEP4_HDFS_*`
   - `JUDGE_API_BASE_URL`、`JUDGE_API_KEY`、`JUDGE_MODEL_NAME`
   - `ETL_USE_SALTED_DEDUP=1`（ETL 两阶段加盐去重，防倾斜）

---

## 5. 一键运行（推荐）

```bash
cd /home/ubuntu/llm-pipeline
bash scripts/run_all.sh
```

运行成功后会输出：

- HDFS Gold 标注结果
- HDFS Gold 向量结果
- 质量评估报告
- 本地 CSV：`/tmp/labeled_output.csv`

---

## 6. 常用命令

### 查看关键日志

```bash
tail -n 100 logs/batch_labeling.log
tail -n 100 logs/vectorization.log
tail -n 100 logs/evaluate_quality.log
```

### 查看 CSV

```bash
ls -lh /tmp/labeled_output.csv
python3 -c "import pandas as pd; df=pd.read_csv('/tmp/labeled_output.csv'); print(df.shape); print(df.head(3))"
```

### 查看 Ray 集群资源

```bash
docker exec ray-head ray status
docker exec ray-head python3 - <<'PY'
import ray
ray.init(address="auto", namespace="rag-labeling", ignore_reinit_error=True)
print(ray.available_resources())
PY
```

### 第二台 GPU 机器接入为 Ray Worker

在第 2 台机器上准备同一份代码目录后，执行：

```bash
cd /home/ubuntu/llm-pipeline
export RAY_CLUSTER_JOIN_ADDRESS=<服务器1可达IP>:6379
export RAY_WORKER_NODE_IP=<服务器2可达IP>
docker compose -f docker-compose.ray-worker.yml up -d --build
```

验证是否加入成功：

```bash
docker exec ray-head ray status
```

### 第 2 台空白 Ubuntu 机器从 0 自举

如果第 2 台机器还是空白环境，可以先在服务器 1 上安装 `sshpass`：

```bash
sudo apt-get update
sudo apt-get install -y sshpass
```

然后从服务器 1 直接推送代码、安装 Docker / NVIDIA Container Toolkit，并启动远端 worker：

```bash
cd /home/ubuntu/llm-pipeline
export REMOTE_HOST=<服务器2IP>
export REMOTE_PASSWORD=<服务器2SSH密码>
export RAY_CLUSTER_JOIN_ADDRESS=<服务器1可达IP>:6379
export RAY_WORKER_NODE_IP=<服务器2可达IP>
bash scripts/bootstrap_remote_ray_worker.sh
```

若服务器 2 还没有 GPU 驱动，可额外加：

```bash
export INSTALL_NVIDIA_DRIVER=1
bash scripts/bootstrap_remote_ray_worker.sh
```

说明：

- `scripts/bootstrap_gpu_worker_host.sh` 负责在服务器 2 上安装 Docker / NVIDIA Container Toolkit，并为 Ray worker 准备运行环境
- `scripts/bootstrap_remote_ray_worker.sh` 负责从服务器 1 打包代码、远程拷贝、执行自举并拉起 `docker-compose.ray-worker.yml`
- 若脚本提示驱动安装完成但需要重启，请先重启服务器 2，再重新执行一次 `bootstrap_remote_ray_worker.sh`

---

## 7. 常见问题

### Q1: `Loaded xxxx records from Silver` 远大于 200

原因通常是历史数据累积（Kafka/Bronze/Silver）。  
`run_all.sh` 已支持运行隔离开关（清理状态 + run-scoped topic）。

### Q2: `QUALITY ALERT overall_avg < threshold`

表示第 6 步质量门禁未通过。优先检查：

- `task_completion_avg`（实体漏提、情感误判）
- 打标输出是否有较多 `parse_fallback`
- 提示词、模型参数是否过于保守

### Q3: Ray 和 Spark 都有 warning，是否失败？

`WARN` 不等于失败，关注是否有 `Pipeline run completed` 与各步骤 `INFO ... finished`。

### Q4: `unknown shorthand flag: 'd' in -d` 或 docker compose 报错

若使用旧版 `docker-compose`（带连字符），请用：

```bash
docker-compose up -d ray-head ray-worker ray-worker-2
```

### Q5: 第二台机器加入 Ray 后仍看不到新增 GPU

优先检查：

- `RAY_HEAD_NODE_IP` 是否为空（使用自动探测）或显式设成了服务器 1 对服务器 2 可达的地址
- 服务器 2 的 `RAY_CLUSTER_JOIN_ADDRESS` 是否为 `<服务器1可达IP>:6379`
- 安全组 / 防火墙是否放通了 `6379`、`10001`
- 服务器 2 的 worker 是否与服务器 1 使用同一份代码和兼容的 `Dockerfile.ray`

### Q6: 为什么 Step 4 里不再使用 `namenode:9000`

Ray Head 现在使用 host networking 以便让跨主机 worker 稳定加入集群，因此 Step 4 在
`ray-head` 容器里访问 HDFS / Kafka 时，应走服务器 1 已发布到宿主机的端口，默认使用：

- `STEP4_HDFS_HOST=localhost`
- `STEP4_KAFKA_BOOTSTRAP_SERVERS=localhost:9092`

---

## 8. 结果下载（Windows）

从服务器下载 CSV 到 Windows 桌面（在本机 PowerShell 执行）：

```powershell
scp ubuntu@<服务器公网IP>:/tmp/labeled_output.csv "$env:USERPROFILE\Desktop\labeled_output.csv"
```

---

## 9. 说明

- `scripts/run_all.sh`：当前主流程入口（推荐）。
- `scripts/run_poc.sh`：早期 PoC 流程入口（`src/rag_poc`）。
- `docker-compose.ray-worker.yml`：第 2 台 GPU 服务器专用的远端 Ray Worker 部署文件。
- `scripts/start_ray_node.sh`：Ray head / worker 共用启动脚本，统一处理跨主机地址参数。
- `scripts/bootstrap_gpu_worker_host.sh`：在远端 Ubuntu 主机上安装 Docker / NVIDIA Container Toolkit 并准备 worker 运行环境。
- `scripts/bootstrap_remote_ray_worker.sh`：从服务器 1 通过 SSH 推送并启动远端 Ray worker。

---

## 10. 架构与调度策略

| 模块 | 策略 | 说明 |
|------|------|------|
| **Ray 集群** | 主机 1 为 Head，可横向扩到主机 2 | 本机默认包含 `ray-head` + `ray-worker*`；第 2 台 GPU 服务器可通过 `docker-compose.ray-worker.yml` 以远端 Worker 身份加入 |
| **LLM 打标** | 每 GPU 常驻 Actor + vLLM 内部 batching | `RAY_LABEL_NUM_GPUS_PER_ACTOR`、`RAY_LABEL_ACTOR_COUNT` 控制 Actor 数；吞吐主要由 `VLLM_OFFLINE_MAX_NUM_SEQS` 与 `RAY_INFERENCE_BATCH_SIZE` 决定 |
| **向量化** | 超长截断 + OOM 降级 | 超长文本截断；OOM 时自动按 `RAY_EMBED_OOM_FALLBACK_BATCH` 拆 batch 重试 |
| **ETL 去重** | 两阶段加盐 | `ETL_USE_SALTED_DEDUP=1` 时，Phase 1 加盐局部去重，Phase 2 全局去重，防止「默认好评」等热点导致 Executor OOM |

## 11. 双机部署建议

推荐拓扑：

- 服务器 1：`zookeeper`、`kafka`、`namenode`、`datanode`、`spark-master`、`ray-head`
- 服务器 2：仅运行远端 `ray-worker`

这样做的好处：

- 不需要把 HDFS / Kafka 拆到多机，改动最小
- `src/ml_pipeline/batch_labeling.py` 现有的离线 vLLM actor 池逻辑可直接扩展到更多 GPU
- Step 4 仍固定在服务器 1 的 `ray-head` 中执行，远端 worker 只负责模型推理

跨机前至少放通这些端口：

- `6379`：Ray head / worker 加入集群
- `10001`：Ray Client
- `9000`：HDFS RPC（仅当远端进程需要直接访问 HDFS 时）
- `9870`：HDFS Web UI（调试可选）

