#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[bootstrap] %s\n' "$1"
}

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

export DEBIAN_FRONTEND=noninteractive

WORKSPACE_DIR="${WORKSPACE_DIR:-/home/ubuntu/llm-pipeline}"
INSTALL_NVIDIA_DRIVER="${INSTALL_NVIDIA_DRIVER:-0}"
RAY_CLUSTER_JOIN_ADDRESS="${RAY_CLUSTER_JOIN_ADDRESS:-}"
RAY_WORKER_NODE_IP="${RAY_WORKER_NODE_IP:-}"
DOCKER_APT_MIRROR_URL="${DOCKER_APT_MIRROR_URL:-https://mirrors.aliyun.com/docker-ce/linux/ubuntu}"

if [[ -f /etc/apt/sources.list ]]; then
  sed -i \
    -e 's|http://archive.ubuntu.com/ubuntu/|http://mirrors.tuna.tsinghua.edu.cn/ubuntu/|g' \
    -e 's|http://security.ubuntu.com/ubuntu/|http://mirrors.tuna.tsinghua.edu.cn/ubuntu/|g' \
    -e 's|http://deb.debian.org/debian|http://mirrors.tuna.tsinghua.edu.cn/debian|g' \
    -e 's|http://security.debian.org/debian-security|http://mirrors.tuna.tsinghua.edu.cn/debian-security|g' \
    /etc/apt/sources.list || true
fi

apt-get update
apt-get install -y --no-install-recommends \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  software-properties-common \
  git

if ! command -v docker >/dev/null 2>&1; then
  log "Installing Docker Engine"
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --batch --yes --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] ${DOCKER_APT_MIRROR_URL} \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    >/etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y --no-install-recommends docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

if ! command -v nvidia-smi >/dev/null 2>&1; then
  if [[ "$INSTALL_NVIDIA_DRIVER" == "1" ]]; then
    log "Installing NVIDIA driver via ubuntu-drivers"
    apt-get install -y --no-install-recommends ubuntu-drivers-common
    ubuntu-drivers install
    log "NVIDIA driver installed. Reboot is usually required before continuing."
    touch /var/run/llm-pipeline-needs-reboot
    exit 0
  fi
  log "nvidia-smi is missing. Re-run with INSTALL_NVIDIA_DRIVER=1 to install the GPU driver."
  exit 1
fi

distribution=$(. /etc/os-release; echo "${ID}${VERSION_ID}")
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --batch --yes --dearmor -o /etc/apt/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL "https://nvidia.github.io/libnvidia-container/${distribution}/libnvidia-container.list" \
  | sed 's#deb https://#deb [signed-by=/etc/apt/keyrings/nvidia-container-toolkit-keyring.gpg] https://#' \
  >/etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update
apt-get install -y --no-install-recommends nvidia-container-toolkit
nvidia-ctk runtime configure --runtime=docker
systemctl restart docker

if id -nG ubuntu | tr ' ' '\n' | rg '^docker$' >/dev/null 2>&1; then
  :
else
  usermod -aG docker ubuntu || true
fi

if [[ -n "$RAY_CLUSTER_JOIN_ADDRESS" ]]; then
  cat >/etc/profile.d/llm-pipeline-ray-worker.sh <<EOF
export RAY_CLUSTER_JOIN_ADDRESS="${RAY_CLUSTER_JOIN_ADDRESS}"
export RAY_WORKER_NODE_IP="${RAY_WORKER_NODE_IP}"
EOF
  chmod 0644 /etc/profile.d/llm-pipeline-ray-worker.sh
fi

mkdir -p "$WORKSPACE_DIR"
chown -R ubuntu:ubuntu "$WORKSPACE_DIR"

log "Bootstrap completed"
