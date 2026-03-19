#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

COLOR_RED="\033[31m"
COLOR_GREEN="\033[32m"
COLOR_YELLOW="\033[33m"
COLOR_BLUE="\033[34m"
COLOR_RESET="\033[0m"

log_info() {
  echo -e "${COLOR_BLUE}[INFO]${COLOR_RESET} $1"
}

log_ok() {
  echo -e "${COLOR_GREEN}[OK]${COLOR_RESET} $1"
}

log_warn() {
  echo -e "${COLOR_YELLOW}[WARN]${COLOR_RESET} $1"
}

log_err() {
  echo -e "${COLOR_RED}[ERR]${COLOR_RESET} $1"
}

debug_log() {
  local run_id="$1"
  local hypothesis_id="$2"
  local location="$3"
  local message="$4"
  local data_json="$5"
  python3 - "$run_id" "$hypothesis_id" "$location" "$message" "$data_json" <<'PY'
import json
import sys
import time
from pathlib import Path

run_id, hypothesis_id, location, message, data_json = sys.argv[1:6]
try:
    data = json.loads(data_json)
except Exception:
    data = {"raw": data_json}

payload = {
    "sessionId": "761422",
    "runId": run_id,
    "hypothesisId": hypothesis_id,
    "location": location,
    "message": message,
    "data": data,
    "timestamp": int(time.time() * 1000),
}
log_path = Path("/home/ubuntu/llm-pipeline/.cursor/debug-761422.log")
log_path.parent.mkdir(parents=True, exist_ok=True)
with log_path.open("a", encoding="utf-8") as file:
    file.write(json.dumps(payload, ensure_ascii=False) + "\n")
PY
}

require_cmd() {
  local cmd="$1"
  local resolved_cmd
  resolved_cmd="$(command -v "$cmd" 2>/dev/null || true)"
  #region agent log
  debug_log "pre-fix" "H2" "scripts/run_all.sh:require_cmd" "command_probe" "{\"cmd\":\"$cmd\",\"resolved\":\"$resolved_cmd\"}"
  #endregion
  if [[ -z "$resolved_cmd" ]]; then
    #region agent log
    debug_log "pre-fix" "H1" "scripts/run_all.sh:require_cmd" "required_command_missing" "{\"cmd\":\"$cmd\",\"path\":\"$PATH\"}"
    #endregion
    if [[ "$cmd" == "docker" ]]; then
      #region agent log
      debug_log "pre-fix" "H3" "scripts/run_all.sh:require_cmd" "docker_alternatives_probe" "{\"docker_compose\":\"$(command -v docker-compose 2>/dev/null || true)\",\"docker_binary_exists\":\"$(test -x /usr/bin/docker && echo yes || echo no)\"}"
      #endregion
    fi
    log_err "Missing required command: $cmd"
    exit 1
  fi
}

require_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=("docker" "compose")
    #region agent log
    debug_log "post-fix" "H5" "scripts/run_all.sh:require_docker_compose" "compose_command_selected" "{\"mode\":\"docker compose\"}"
    #endregion
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=("docker-compose")
    #region agent log
    debug_log "post-fix" "H5" "scripts/run_all.sh:require_docker_compose" "compose_command_selected" "{\"mode\":\"docker-compose\"}"
    #endregion
    return 0
  fi
  #region agent log
  debug_log "post-fix" "H5" "scripts/run_all.sh:require_docker_compose" "compose_command_missing" "{}"
  #endregion
  log_err "Missing docker compose command. Install Docker Compose plugin or docker-compose."
  exit 1
}

configure_docker_access() {
  if docker info >/dev/null 2>&1; then
    DOCKER_RUNNER=()
    #region agent log
    debug_log "post-fix" "H6" "scripts/run_all.sh:configure_docker_access" "docker_access_mode" "{\"mode\":\"direct\"}"
    #endregion
    return 0
  fi
  if sudo -n docker info >/dev/null 2>&1; then
    DOCKER_RUNNER=("sudo" "-n")
    #region agent log
    debug_log "post-fix" "H6" "scripts/run_all.sh:configure_docker_access" "docker_access_mode" "{\"mode\":\"sudo\"}"
    #endregion
    return 0
  fi
  #region agent log
  debug_log "post-fix" "H6" "scripts/run_all.sh:configure_docker_access" "docker_access_mode" "{\"mode\":\"denied\"}"
  #endregion
  log_err "Docker is installed but current user cannot access daemon. Add user to docker group or use sudo."
  exit 1
}

run_compose() {
  if [[ ${#DOCKER_RUNNER[@]} -eq 0 ]]; then
    "${DOCKER_COMPOSE_CMD[@]}" "$@"
  else
    "${DOCKER_RUNNER[@]}" "${DOCKER_COMPOSE_CMD[@]}" "$@"
  fi
}

run_docker() {
  if [[ ${#DOCKER_RUNNER[@]} -eq 0 ]]; then
    docker "$@"
  else
    "${DOCKER_RUNNER[@]}" docker "$@"
  fi
}

probe_docker_mirrors() {
  local mirrors
  if [[ ${#DOCKER_RUNNER[@]} -eq 0 ]]; then
    mirrors="$(docker info --format '{{json .RegistryConfig.Mirrors}}' 2>/dev/null || true)"
  else
    mirrors="$("${DOCKER_RUNNER[@]}" docker info --format '{{json .RegistryConfig.Mirrors}}' 2>/dev/null || true)"
  fi
  if [[ -z "$mirrors" ]]; then
    mirrors="unknown"
  fi
  #region agent log
  debug_log "pre-fix" "H8" "scripts/run_all.sh:probe_docker_mirrors" "docker_registry_mirrors" "{\"mirrors\":\"$mirrors\"}"
  #endregion
}

probe_registry_network() {
  local dns_result
  local curl_result

  dns_result="$(getent hosts registry-1.docker.io 2>/dev/null | head -n 1 || true)"
  if [[ -z "$dns_result" ]]; then
    dns_result="unresolved"
  fi
  #region agent log
  debug_log "pre-fix" "N2" "scripts/run_all.sh:probe_registry_network" "dns_probe_registry" "{\"result\":\"$dns_result\"}"
  #endregion

  curl_result="$(curl -I --max-time 8 https://registry-1.docker.io/v2/ 2>&1 | tr '\n' ' ' | sed 's/\"/\\"/g' || true)"
  if [[ -z "$curl_result" ]]; then
    curl_result="no_output"
  fi
  REGISTRY_CURL_PROBE_RESULT="$curl_result"
  #region agent log
  debug_log "pre-fix" "N1" "scripts/run_all.sh:probe_registry_network" "https_probe_registry" "{\"result\":\"$curl_result\"}"
  #endregion
}

check_required_images() {
  MISSING_IMAGES=()
  local required_images=(
    "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/zookeeper:3.9.2"
    "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/kafka:3.7"
    "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8"
    "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8"
    "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/spark:3.5.3"
    "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/vllm/vllm-openai:latest"
  )

  local image
  for image in "${required_images[@]}"; do
    if ! "${DOCKER_RUNNER[@]}" docker image inspect "$image" >/dev/null 2>&1; then
      MISSING_IMAGES+=("$image")
    fi
  done

  #region agent log
  debug_log "post-fix" "H10" "scripts/run_all.sh:check_required_images" "required_images_scan" "{\"missing_count\":\"${#MISSING_IMAGES[@]}\"}"
  #endregion
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local name="$3"
  local retries="${4:-60}"
  local delay_seconds="${5:-2}"

  log_info "Waiting for $name at $host:$port ..."
  for ((i=1; i<=retries; i++)); do
    if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then
      log_ok "$name is ready."
      return 0
    fi
    sleep "$delay_seconds"
  done
  log_err "$name did not become ready in time."
  return 1
}

wait_for_http() {
  local url="$1"
  local name="$2"
  local retries="${3:-120}"
  local delay_seconds="${4:-2}"

  log_info "Waiting for $name at $url ..."
  for ((i=1; i<=retries; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      log_ok "$name is ready."
      return 0
    fi
    sleep "$delay_seconds"
  done
  log_err "$name did not become ready in time."
  return 1
}

hdfs_bronze_lines() {
  local raw value
  # hdfs dfs -cat 在容器内可能触发 UnresolvedAddressException；改用 host 上 PySpark 读取（host /etc/hosts 能解析 namenode/datanode）
  raw="$(python3 -c '
try:
  from pyspark.sql import SparkSession
  spark=SparkSession.builder.master("local[1]").config("spark.ui.enabled","false").getOrCreate()
  n=spark.read.text("hdfs://namenode:9000/data/bronze/part-*").count()
  spark.stop()
  print(n)
except Exception:
  print(0)
' 2>/dev/null || echo "0")"
  value="$(printf '%s\n' "$raw" | awk 'match($0,/([0-9]+)/){print substr($0,RSTART,RLENGTH); exit}')"
  [[ -z "$value" ]] && value="0"
  echo "$((value + 0))"
}

run_step3_to_step5_cycle() {
  local cycle_name="$1"
  local cycle_lines="$2"
  local step4_status="FAILED"
  local step5_status="FAILED"
  local step4_tmp_log="$LOG_DIR/.step4_${cycle_name}.log"
  local step5_tmp_log="$LOG_DIR/.step5_${cycle_name}.log"

  log_info "Pipeline cycle [$cycle_name]: run Step 3/4/5 on bronze_lines=$cycle_lines"

  log_info "Step 3/6: run bronze->silver ETL clean and dedup."
  python3 -m etl.clean_and_dedup | tee -a "$LOG_DIR/clean_and_dedup.log"

  log_info "Step 4/6: run silver->gold labeling."
  run_docker exec \
    -e HDFS_HOST="$STEP4_HDFS_HOST" \
    -e HDFS_SILVER_PATH="$STEP4_HDFS_SILVER_PATH" \
    -e HDFS_GOLD_LABELED_PATH="$STEP4_HDFS_GOLD_LABELED_PATH" \
    -e KAFKA_BOOTSTRAP_SERVERS="$STEP4_KAFKA_BOOTSTRAP_SERVERS" \
    -e RAY_ADDRESS="$STEP4_RAY_ADDRESS" \
    -e SPARK_MASTER="local[*]" \
    -e JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \
    -w /workspace \
    ray-head \
    bash -lc "python3 -m ml_pipeline.batch_labeling" \
    | tee -a "$LOG_DIR/batch_labeling.log" | tee "$step4_tmp_log"
  step4_status="$(awk -F'=' '/PIPELINE_STEP_STATUS:STEP4=/{s=$2} END{print s}' "$step4_tmp_log")"
  if [[ -z "$step4_status" ]]; then
    step4_status="DONE"
  fi
  rm -f "$step4_tmp_log" >/dev/null 2>&1 || true
  log_info "Step 4 status: $step4_status"
  run_docker cp ray-head:/tmp/labeled_output.csv /tmp/labeled_output.csv >/dev/null 2>&1 || true

  log_info "Step 5/6: run vectorization to gold embeddings."
  run_docker exec \
    -e HDFS_HOST="$STEP4_HDFS_HOST" \
    -e HDFS_PORT="$STEP4_HDFS_PORT" \
    -e HDFS_SILVER_PATH="$STEP4_HDFS_SILVER_PATH" \
    -e HDFS_GOLD_EMBEDDINGS_PATH="${HDFS_GOLD_EMBEDDINGS_PATH:-hdfs://${STEP4_HDFS_HOST}:${STEP4_HDFS_PORT}/data/gold/embeddings/}" \
    -e RAY_ADDRESS="$STEP4_RAY_ADDRESS" \
    -e SPARK_MASTER="local[*]" \
    -e JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \
    -w /workspace \
    ray-head \
    bash -lc "python3 -m ml_pipeline.vectorization" \
    | tee -a "$LOG_DIR/vectorization.log" | tee "$step5_tmp_log"
  step5_status="$(awk -F'=' '/PIPELINE_STEP_STATUS:STEP5=/{s=$2} END{print s}' "$step5_tmp_log")"
  if [[ -z "$step5_status" ]]; then
    step5_status="DONE"
  fi
  rm -f "$step5_tmp_log" >/dev/null 2>&1 || true
  log_info "Step 5 status: $step5_status"
  log_info "Pipeline cycle [$cycle_name] summary: Step4=$step4_status Step5=$step5_status"
}

cleanup() {
  if [[ -n "${STREAM_PID:-}" ]]; then
    if kill -0 "$STREAM_PID" >/dev/null 2>&1; then
      log_info "Stopping ingestion stream process (pid=$STREAM_PID)."
      kill "$STREAM_PID" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

#region agent log
debug_log "pre-fix" "H2" "scripts/run_all.sh:startup" "script_start_context" "{\"pwd\":\"$PWD\",\"path\":\"$PATH\",\"shell\":\"${SHELL:-unknown}\",\"user\":\"${USER:-unknown}\"}"
#endregion

require_cmd docker
require_cmd python3
require_cmd curl
require_docker_compose
configure_docker_access
probe_docker_mirrors

if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.venv/bin/activate"
  log_ok "Activated virtual environment: .venv"
else
  log_warn ".venv not found. Using system Python interpreter."
fi

OVERRIDE_ENV_KEYS=(
  EXPECTED_QPS
  PRODUCER_RATE_PER_SECOND
  KAFKA_STABILIZE_DELAY
  REVIEW_COUNT
  RESET_PIPELINE_STATE
  USE_RUN_SCOPED_TOPIC
)
declare -A OVERRIDE_ENV_SNAPSHOT=()
for key in "${OVERRIDE_ENV_KEYS[@]}"; do
  if [[ -v "$key" ]]; then
    OVERRIDE_ENV_SNAPSHOT["$key"]="${!key}"
  fi
done

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
  if [[ "${#OVERRIDE_ENV_SNAPSHOT[@]}" -gt 0 ]]; then
    for key in "${!OVERRIDE_ENV_SNAPSHOT[@]}"; do
      export "$key=${OVERRIDE_ENV_SNAPSHOT[$key]}"
    done
    log_ok "Loaded .env configuration (kept command-line env overrides)."
  else
    log_ok "Loaded .env configuration."
  fi
else
  log_warn ".env not found. Falling back to defaults."
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  if [[ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]]; then
    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
  elif [[ -d "/usr/lib/jvm/default-java" ]]; then
    export JAVA_HOME="/usr/lib/jvm/default-java"
  elif command -v java >/dev/null 2>&1; then
    JAVA_BIN_PATH="$(readlink -f "$(command -v java)" 2>/dev/null || command -v java)"
    export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN_PATH")")"
  fi
fi

if [[ -z "${JAVA_HOME:-}" ]] || [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  log_err "JAVA_HOME is not configured correctly. Install Java (OpenJDK 17+) and retry."
  exit 1
fi
log_ok "JAVA_HOME is set to ${JAVA_HOME}"

# Host execution mode: ensure local endpoints are reachable from host Python processes.
if [[ "${KAFKA_BOOTSTRAP_SERVERS:-}" == "kafka:9092" ]]; then
  export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
fi
# 使用 namenode 服务名供容器（ray-worker、spark-worker）访问 HDFS；host/ray-head 需能解析 namenode
if [[ -z "${HDFS_HOST:-}" ]]; then
  export HDFS_HOST="namenode"
fi
# 多机 Ray 时，DataNode 需公布宿主机 IP 供远端 Worker 连接（libhdfs 可能不用 hostname，直接连 IP）
if [[ -n "${RAY_HEAD_NODE_IP:-}" && -z "${HDFS_DATANODE_ANNOUNCE_HOST:-}" ]]; then
  export HDFS_DATANODE_ANNOUNCE_HOST="$RAY_HEAD_NODE_IP"
  log_info "HDFS_DATANODE_ANNOUNCE_HOST=${HDFS_DATANODE_ANNOUNCE_HOST} (for remote Ray workers)"
fi
# 远端 Worker 读 HDFS 时若遇到 ConnectTimeout（Parquet sampling 卡 50%），可先在远端停掉 ray-worker-remote
if [[ -n "${RAY_HEAD_NODE_IP:-}" ]]; then
  log_warn "Multi-host mode: if Step 4 hangs at Parquet sampling 50%%, stop remote worker: ssh remote-host 'docker stop ray-worker-remote'"
fi
# 确保 host/ray-head 能解析 namenode 和 datanode（host 网络下用 127.0.0.1）
for h in namenode datanode; do
  if ! grep -qE "127\.0\.0\.1[[:space:]]+${h}" /etc/hosts 2>/dev/null; then
    if ! grep -qE "[[:space:]]${h}([[:space:]]|$)" /etc/hosts 2>/dev/null; then
      log_info "Adding $h to /etc/hosts for host/ray-head HDFS access (may need sudo)"
      if (echo "127.0.0.1 $h" | sudo tee -a /etc/hosts) 2>/dev/null; then
        log_ok "Added 127.0.0.1 $h to /etc/hosts"
      else
        log_warn "Could not add $h to /etc/hosts. If HDFS access fails, run: echo '127.0.0.1 $h' | sudo tee -a /etc/hosts"
      fi
    fi
  fi
done
# 不再将 namenode 替换为 localhost，避免容器内 Spark/Ray worker 无法连接 HDFS
if [[ "${SPARK_MASTER:-}" == "spark://spark-master:7077" ]] || [[ "${SPARK_MASTER:-}" == "spark://localhost:7077" ]]; then
  # Host-side PySpark: use local mode so executors run on host and can reach
  # localhost:9000 (HDFS) and localhost:9092 (Kafka). Docker workers see localhost as the container, not the host.
  export SPARK_MASTER="local[*]"
fi
if [[ "${RAY_ADDRESS:-}" == "ray://ray-head:10001" ]]; then
  export RAY_ADDRESS="ray://localhost:10001"
fi

STEP4_HDFS_HOST="${STEP4_HDFS_HOST:-namenode}"
STEP4_HDFS_PORT="${STEP4_HDFS_PORT:-${HDFS_PORT:-9000}}"
STEP4_HDFS_SILVER_PATH="${STEP4_HDFS_SILVER_PATH:-hdfs://${STEP4_HDFS_HOST}:${STEP4_HDFS_PORT}/data/silver/}"
STEP4_HDFS_GOLD_LABELED_PATH="${STEP4_HDFS_GOLD_LABELED_PATH:-hdfs://${STEP4_HDFS_HOST}:${STEP4_HDFS_PORT}/data/gold/labeled/}"
STEP4_KAFKA_BOOTSTRAP_SERVERS="${STEP4_KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
STEP4_RAY_ADDRESS="${STEP4_RAY_ADDRESS:-auto}"
STEP6_HDFS_HOST="${STEP6_HDFS_HOST:-${RAY_HEAD_NODE_IP:-$STEP4_HDFS_HOST}}"
STEP6_HDFS_PORT="${STEP6_HDFS_PORT:-${STEP4_HDFS_PORT}}"
STEP6_HDFS_GOLD_LABELED_PATH="${STEP6_HDFS_GOLD_LABELED_PATH:-hdfs://${STEP6_HDFS_HOST}:${STEP6_HDFS_PORT}/data/gold/labeled/}"

export PYTHONPATH="$ROOT_DIR/src"

# Isolation controls: keep each run independent by default.
# Set RESET_PIPELINE_STATE=0 to keep historical HDFS/checkpoint data.
# Set USE_RUN_SCOPED_TOPIC=0 to reuse KAFKA_RAW_TOPIC from env.
RESET_PIPELINE_STATE="${RESET_PIPELINE_STATE:-1}"
USE_RUN_SCOPED_TOPIC="${USE_RUN_SCOPED_TOPIC:-1}"

# 显示配置模式
if [[ "${USE_SPARK_FALLBACK:-0}" == "1" ]]; then
  log_info "Config: USE_SPARK_FALLBACK=1 (using Spark local mode, skipping Ray)"
else
  log_info "Config: USE_SPARK_FALLBACK=0 (will try Ray first, fallback to Spark if needed)"
fi

log_info "Starting infrastructure containers..."
check_required_images
if [[ "${#MISSING_IMAGES[@]}" -gt 0 ]]; then
  probe_registry_network
  if [[ "${REGISTRY_CURL_PROBE_RESULT:-}" == *"Failed to connect"* ]] || [[ "${REGISTRY_CURL_PROBE_RESULT:-}" == *"Connection timed out"* ]] || [[ "${REGISTRY_CURL_PROBE_RESULT:-}" == *"request canceled"* ]]; then
    #region agent log
    debug_log "post-fix" "H9" "scripts/run_all.sh:start_infra" "registry_connectivity_blocked" "{\"probe\":\"$REGISTRY_CURL_PROBE_RESULT\"}"
    #endregion
    log_err "Cannot reach Docker Hub and required images are missing locally."
    log_warn "Please preload these images on server, then rerun:"
    for image in "${MISSING_IMAGES[@]}"; do
      echo "  - $image"
    done
    exit 1
  fi
else
  log_ok "All required infrastructure images exist locally. Skipping external registry check."
fi
COMPOSE_SERVICES="zookeeper kafka namenode datanode spark-master spark-worker ray-head ray-worker ray-worker-2"
# docker-compose v1 may fail with KeyError('ContainerConfig') during recreate.
# Work around it by removing affected containers before `up` so compose does create, not recreate.
if [[ "${DOCKER_COMPOSE_CMD[*]}" == "docker-compose" ]]; then
  log_warn "Detected docker-compose v1; cleaning stale ray/datanode containers to avoid ContainerConfig bug."
  run_compose rm -sf ray-head ray-worker ray-worker-2 datanode >/dev/null 2>&1 || true
  STALE_RAY_CONTAINERS="$(run_docker ps -a --format '{{.Names}}' | awk '/ray-head|ray-worker|_ray-head|_ray-worker/ {print}')"
  if [[ -n "$STALE_RAY_CONTAINERS" ]]; then
    while IFS= read -r container_name; do
      [[ -z "$container_name" ]] && continue
      run_docker rm -f "$container_name" >/dev/null 2>&1 || true
    done <<< "$STALE_RAY_CONTAINERS"
  fi
fi
if run_compose up -d $COMPOSE_SERVICES; then
  :
else
  compose_status="$?"
  #region agent log
  debug_log "pre-fix" "N3" "scripts/run_all.sh:start_infra" "compose_up_failed" "{\"exit_code\":\"$compose_status\"}"
  #endregion
  exit "$compose_status"
fi

wait_for_tcp "localhost" "9092" "Kafka"
wait_for_tcp "localhost" "9000" "HDFS NameNode RPC"
# HDFS safe mode blocks writes and causes lease/FileNotFoundException; leave it before pipeline.
log_info "Leaving HDFS safe mode (required for Silver/Gold writes)."
run_docker exec hdfs-namenode hdfs dfsadmin -safemode leave 2>/dev/null || true
wait_for_tcp "localhost" "7077" "Spark Master"
# Ray dashboard HTTP may be flaky in some container images; prefer Ray Client port readiness.
wait_for_tcp "localhost" "10001" "Ray Client"

log_info "Stabilizing Kafka broker startup..."
sleep 8

log_info "Ensuring HDFS output directories and permissions."
run_docker exec hdfs-namenode hdfs dfs -mkdir -p /data/bronze /data/silver /data/gold || true
run_docker exec hdfs-namenode hdfs dfs -chmod -R 777 /data || true

# Default behavior for reproducible runs: clear historical pipeline state.
if [[ "$RESET_PIPELINE_STATE" == "1" ]]; then
  log_info "RESET_PIPELINE_STATE=1, clearing HDFS bronze/silver/gold and local Spark checkpoints."
  run_docker exec hdfs-namenode hdfs dfs -rm -r -f /data/bronze/* >/dev/null 2>&1 || true
  run_docker exec hdfs-namenode hdfs dfs -rm -r -f /data/silver/* >/dev/null 2>&1 || true
  run_docker exec hdfs-namenode hdfs dfs -rm -r -f /data/gold/labeled/* >/dev/null 2>&1 || true
  run_docker exec hdfs-namenode hdfs dfs -rm -r -f /data/gold/embeddings/* >/dev/null 2>&1 || true
  rm -rf /tmp/spark-checkpoints || true
fi

# Use run-scoped Kafka topic to avoid consuming historical backlog.
if [[ "$USE_RUN_SCOPED_TOPIC" == "1" ]]; then
  KAFKA_RAW_TOPIC_BASE="${KAFKA_RAW_TOPIC:-raw_reviews}"
  export KAFKA_RAW_TOPIC="${KAFKA_RAW_TOPIC_BASE}_run_$(date +%s)"
  log_info "Using run-scoped Kafka topic: ${KAFKA_RAW_TOPIC}"
fi

# Create topic before starting consumer so Spark does not hit UnknownTopicOrPartitionException.
log_info "Ensuring Kafka topic exists: ${KAFKA_RAW_TOPIC:-raw_reviews}"
run_docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "${KAFKA_RAW_TOPIC:-raw_reviews}" --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null || true
log_ok "Kafka topic ready."

# Generate 200 mock reviews for testing (if generator script exists)
if [[ -f "$ROOT_DIR/scripts/generate_reviews.py" ]]; then
  log_info "Generating 200 mock review records..."
  python3 "$ROOT_DIR/scripts/generate_reviews.py"
  export REVIEW_DATASET_PATH="$ROOT_DIR/data/reviews.jsonl"
else
  # Fallback to default 3 records
  if [[ ! -f "${REVIEW_DATASET_PATH:-$ROOT_DIR/data/reviews.jsonl}" ]]; then
    mkdir -p "$ROOT_DIR/data"
    cat > "$ROOT_DIR/data/reviews.jsonl" <<'EOF'
{"id":"r-001","text":"这个手机的续航很不错，拍照效果也满意。","timestamp":"2026-03-06T10:00:00Z"}
{"id":"r-002","text":"客服响应速度太慢，物流也延迟。","timestamp":"2026-03-06T10:01:00Z"}
{"id":"r-003","text":"包装完整，做工一般，整体中规中矩。","timestamp":"2026-03-06T10:02:00Z"}
EOF
    export REVIEW_DATASET_PATH="$ROOT_DIR/data/reviews.jsonl"
    log_warn "REVIEW_DATASET_PATH missing, generated demo dataset at data/reviews.jsonl"
  fi
fi

log_info "Step 1/6: start Kafka->HDFS bronze streaming (background)."
python3 -m ingestion.kafka_to_hdfs > "$LOG_DIR/kafka_to_hdfs.log" 2>&1 &
STREAM_PID=$!
log_ok "Streaming process started. pid=$STREAM_PID log=$LOG_DIR/kafka_to_hdfs.log"

# Give Kafka + streaming consumer time to stabilize (cold start often needs 20-30s)
KAFKA_STABILIZE_DELAY="${KAFKA_STABILIZE_DELAY:-30}"
log_info "Waiting ${KAFKA_STABILIZE_DELAY}s for Kafka broker and streaming consumer to stabilize..."
sleep "$KAFKA_STABILIZE_DELAY"

PIPELINE_PARALLEL_MODE="${PIPELINE_PARALLEL_MODE:-1}"
PIPELINE_REFRESH_INTERVAL_SECONDS="${PIPELINE_REFRESH_INTERVAL_SECONDS:-5}"
PIPELINE_REFRESH_MIN_NEW_LINES="${PIPELINE_REFRESH_MIN_NEW_LINES:-50}"
PIPELINE_REFRESH_MIN_LINES="${PIPELINE_REFRESH_MIN_LINES:-50}"
LAST_REFRESH_LINES=0
CYCLE_COUNT=0

log_info "Step 2/6: produce review events to Kafka."
python3 -m ingestion.producer > "$LOG_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!
log_ok "Producer started in background. pid=$PRODUCER_PID log=$LOG_DIR/producer.log"

if [[ "$PIPELINE_PARALLEL_MODE" == "1" ]]; then
  log_info "Parallel pipeline mode enabled: Step 3/4/5 will run while Step 2 is still producing."
  while kill -0 "$PRODUCER_PID" >/dev/null 2>&1; do
    sleep "$PIPELINE_REFRESH_INTERVAL_SECONDS"
    BRONZE_LINES="$(hdfs_bronze_lines)"
    BRONZE_LINES="$((BRONZE_LINES + 0))"
    LAST_REFRESH_LINES="$((LAST_REFRESH_LINES + 0))"
    if [[ "$BRONZE_LINES" -lt "$PIPELINE_REFRESH_MIN_LINES" ]]; then
      log_info "Parallel cycle wait: bronze_lines=$BRONZE_LINES (<$PIPELINE_REFRESH_MIN_LINES)."
      continue
    fi
    NEW_LINES=$((BRONZE_LINES - LAST_REFRESH_LINES))
    if [[ "$NEW_LINES" -lt "$PIPELINE_REFRESH_MIN_NEW_LINES" ]]; then
      log_info "Parallel cycle wait: new_lines=$NEW_LINES (<$PIPELINE_REFRESH_MIN_NEW_LINES)."
      continue
    fi
    CYCLE_COUNT=$((CYCLE_COUNT + 1))
    run_step3_to_step5_cycle "streaming-$CYCLE_COUNT" "$BRONZE_LINES"
    LAST_REFRESH_LINES="$BRONZE_LINES"
  done
else
  log_warn "Parallel pipeline mode disabled; will run Step 3/4/5 after producer finishes."
fi

wait "$PRODUCER_PID"
log_ok "Producer finished."

log_info "Waiting for data to flush to HDFS Bronze (checking actual content, max 90s)..."
BRONZE_LINES=0
FILE_COUNT=0
for i in {1..18}; do
  sleep 5
  FILE_COUNT_RAW=$(run_docker exec hdfs-namenode hdfs dfs -ls /data/bronze 2>/dev/null | grep -c "part-" || echo "0")
  FILE_COUNT=$(echo "$FILE_COUNT_RAW" | grep -oE '[0-9]+' | head -1)
  FILE_COUNT=${FILE_COUNT:-0}
  BRONZE_LINES="$(hdfs_bronze_lines)"
  if [[ "$FILE_COUNT" -gt 0 ]] && [[ "$BRONZE_LINES" -ge 10 ]]; then
    log_ok "Bronze data ready ($BRONZE_LINES records in $FILE_COUNT files)"
    break
  fi
  log_info "  ... retry $i/18, waiting for bronze data (current: $FILE_COUNT files, $BRONZE_LINES lines)"
done

if kill -0 "$STREAM_PID" >/dev/null 2>&1; then
  log_info "Stopping Kafka->HDFS streaming process."
  kill "$STREAM_PID" >/dev/null 2>&1 || true
  STREAM_PID=""
fi

# Final catch-up cycle to ensure latest bronze data is fully processed.
FINAL_LINES="$(hdfs_bronze_lines)"
if [[ "$FINAL_LINES" -ge 10 ]]; then
  run_step3_to_step5_cycle "final" "$FINAL_LINES"
else
  log_err "Bronze still has no data after producer finished."
  log_err "  - Check $LOG_DIR/kafka_to_hdfs.log for connection or write failures."
  exit 1
fi

log_info "Step 6/6: run continuous quality evaluation."
if [[ -n "${JUDGE_API_KEY:-}" ]]; then
  # Run inside ray-head to share the Ray/Python environment from Step 4 and to use
  # host-networked service endpoints consistently. Step 6 must read HDFS Gold, and
  # Spark inside ray-head is more reliable against the host-published NameNode port
  # when using the head node IP instead of localhost.
  run_docker exec \
    -e HDFS_HOST="$STEP6_HDFS_HOST" \
    -e HDFS_GOLD_LABELED_PATH="$STEP6_HDFS_GOLD_LABELED_PATH" \
    -e RAY_ADDRESS="$STEP4_RAY_ADDRESS" \
    -e SPARK_MASTER="local[*]" \
    -e USE_SPARK_FALLBACK="1" \
    -e JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \
    -w /workspace \
    ray-head \
    bash -lc "python3 -m ml_pipeline.evaluate_quality" | tee "$LOG_DIR/evaluate_quality.log"
else
  log_warn "JUDGE_API_KEY is empty, skip evaluate_quality.py."
  log_warn "Set JUDGE_API_KEY in .env and rerun: python3 -m ml_pipeline.evaluate_quality"
fi

# Check for CSV output
if [[ -f "/tmp/labeled_output.csv" ]]; then
  log_ok "CSV output found: /tmp/labeled_output.csv"
  log_info "CSV preview (first 5 lines):"
  head -5 /tmp/labeled_output.csv | while read line; do
    echo "  $line"
  done
  ROW_COUNT=$(wc -l < /tmp/labeled_output.csv)
  log_info "Total rows in CSV: $ROW_COUNT"
else
  log_warn "CSV output not found at /tmp/labeled_output.csv"
  log_info "Check $LOG_DIR/batch_labeling.log for details"
fi

log_ok "Pipeline run completed. Logs are under $LOG_DIR"
