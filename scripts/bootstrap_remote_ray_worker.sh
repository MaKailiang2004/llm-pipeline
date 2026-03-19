#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  REMOTE_HOST=<ip> REMOTE_PASSWORD=<password> RAY_CLUSTER_JOIN_ADDRESS=<server1-ip>:6379 \
    bash scripts/bootstrap_remote_ray_worker.sh

Optional:
  REMOTE_USER=ubuntu
  REMOTE_WORKSPACE_DIR=/home/ubuntu/llm-pipeline
  RAY_WORKER_NODE_IP=<server2-ip>
  REMOTE_NAMENODE_IP=<server1-ip>
  RAY_IMAGE_NAME=llm-pipeline/ray:latest
  REMOTE_FORCE_BUILD=0
  INSTALL_NVIDIA_DRIVER=1
  NVIDIA_VISIBLE_DEVICES=all
EOF
}

REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_USER="${REMOTE_USER:-ubuntu}"
REMOTE_PASSWORD="${REMOTE_PASSWORD:-}"
REMOTE_WORKSPACE_DIR="${REMOTE_WORKSPACE_DIR:-/home/ubuntu/llm-pipeline}"
RAY_CLUSTER_JOIN_ADDRESS="${RAY_CLUSTER_JOIN_ADDRESS:-}"
RAY_WORKER_NODE_IP="${RAY_WORKER_NODE_IP:-}"
REMOTE_NAMENODE_IP="${REMOTE_NAMENODE_IP:-${RAY_CLUSTER_JOIN_ADDRESS%%:*}}"
INSTALL_NVIDIA_DRIVER="${INSTALL_NVIDIA_DRIVER:-0}"
NVIDIA_VISIBLE_DEVICES="${NVIDIA_VISIBLE_DEVICES:-all}"
RAY_IMAGE_NAME="${RAY_IMAGE_NAME:-llm-pipeline/ray:latest}"
REMOTE_FORCE_BUILD="${REMOTE_FORCE_BUILD:-0}"

if [[ -z "$REMOTE_HOST" || -z "$REMOTE_PASSWORD" || -z "$RAY_CLUSTER_JOIN_ADDRESS" ]]; then
  usage
  exit 1
fi

if ! command -v sshpass >/dev/null 2>&1; then
  echo "sshpass is required. Install it first: sudo apt-get install -y sshpass" >&2
  exit 1
fi

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
)

ARCHIVE_PATH="/tmp/llm-pipeline-remote.tgz"
tar \
  --exclude='.git' \
  --exclude='.venv' \
  --exclude='.cursor' \
  --exclude='logs' \
  --exclude='data' \
  --exclude='models' \
  --exclude='__pycache__' \
  --exclude='*.log' \
  --exclude='node_modules' \
  -czf "$ARCHIVE_PATH" .

sshpass -p "$REMOTE_PASSWORD" ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" \
  "mkdir -p '${REMOTE_WORKSPACE_DIR}'"

sshpass -p "$REMOTE_PASSWORD" scp "${SSH_OPTS[@]}" "$ARCHIVE_PATH" \
  "${REMOTE_USER}@${REMOTE_HOST}:/tmp/llm-pipeline-remote.tgz"

sshpass -p "$REMOTE_PASSWORD" ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "
  set -euo pipefail
  mkdir -p '${REMOTE_WORKSPACE_DIR}'
  tar -xzf /tmp/llm-pipeline-remote.tgz -C '${REMOTE_WORKSPACE_DIR}' --strip-components=0
  cd '${REMOTE_WORKSPACE_DIR}'
  export WORKSPACE_DIR='${REMOTE_WORKSPACE_DIR}'
  export RAY_CLUSTER_JOIN_ADDRESS='${RAY_CLUSTER_JOIN_ADDRESS}'
  export RAY_WORKER_NODE_IP='${RAY_WORKER_NODE_IP}'
  export INSTALL_NVIDIA_DRIVER='${INSTALL_NVIDIA_DRIVER}'
  sudo -E bash scripts/bootstrap_gpu_worker_host.sh
"

sshpass -p "$REMOTE_PASSWORD" ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}" "
  set -euo pipefail
  if ! grep -qE '(^|[[:space:]])namenode([[:space:]]|$)' /etc/hosts; then
    echo '${REMOTE_NAMENODE_IP} namenode' | sudo tee -a /etc/hosts >/dev/null
  fi
  if ! grep -qE '(^|[[:space:]])datanode([[:space:]]|$)' /etc/hosts; then
    echo '${REMOTE_NAMENODE_IP} datanode' | sudo tee -a /etc/hosts >/dev/null
  fi
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD='docker compose'
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD='docker-compose'
  else
    echo 'Missing Docker Compose on remote host.' >&2
    exit 1
  fi
  cd '${REMOTE_WORKSPACE_DIR}'
  export RAY_CLUSTER_JOIN_ADDRESS='${RAY_CLUSTER_JOIN_ADDRESS}'
  export RAY_WORKER_NODE_IP='${RAY_WORKER_NODE_IP}'
  export NVIDIA_VISIBLE_DEVICES='${NVIDIA_VISIBLE_DEVICES}'
  export RAY_IMAGE_NAME='${RAY_IMAGE_NAME}'
  if [[ '${REMOTE_FORCE_BUILD}' == '1' ]]; then
    \$COMPOSE_CMD -f docker-compose.ray-worker.yml up -d --build
  else
    \$COMPOSE_CMD -f docker-compose.ray-worker.yml up -d
  fi
"

rm -f "$ARCHIVE_PATH"

echo "Remote Ray worker bootstrap submitted for ${REMOTE_HOST}"
