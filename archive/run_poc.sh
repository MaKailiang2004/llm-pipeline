#!/usr/bin/env bash

set -euo pipefail

# Prefer "docker compose" (V2 plugin), fallback to "docker-compose" (V1 standalone)
if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
else
  COMPOSE_CMD=(docker-compose)
fi

echo "[1/5] Starting infrastructure..."
"${COMPOSE_CMD[@]}" up -d zookeeper kafka namenode datanode spark-master spark-worker ray-head vllm app

echo "[2/5] Starting Spark streaming consumer..."
"${COMPOSE_CMD[@]}" exec -d app bash -lc "PYTHONPATH=/workspace/src python -m rag_poc.spark_kafka_to_hdfs"

echo "[3/5] Producing sample documents to Kafka..."
"${COMPOSE_CMD[@]}" exec app bash -lc "PYTHONPATH=/workspace/src python -m rag_poc.kafka_ingest"

echo "[4/5] Preparing processed dataset..."
"${COMPOSE_CMD[@]}" exec app bash -lc "PYTHONPATH=/workspace/src python -m rag_poc.spark_prepare_processed"

echo "[5/5] Running Ray + vLLM auto-labeling job..."
"${COMPOSE_CMD[@]}" exec app bash -lc "PYTHONPATH=/workspace/src python -m rag_poc.ray_vllm_labeling"

echo "PoC pipeline execution completed."
