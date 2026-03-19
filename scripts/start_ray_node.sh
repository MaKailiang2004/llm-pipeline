#!/usr/bin/env bash

set -euo pipefail

detect_node_ip() {
  local detected
  detected="$(hostname -I 2>/dev/null | awk '{print $1}')"
  if [[ -n "$detected" ]]; then
    printf '%s\n' "$detected"
    return 0
  fi
  printf '127.0.0.1\n'
}

RAY_NODE_ROLE="${RAY_NODE_ROLE:-worker}"
RAY_NODE_IP="${RAY_NODE_IP:-$(detect_node_ip)}"
RAY_HEAD_PORT="${RAY_HEAD_PORT:-6379}"
RAY_HEAD_CLIENT_PORT="${RAY_HEAD_CLIENT_PORT:-10001}"
RAY_DASHBOARD_PORT="${RAY_DASHBOARD_PORT:-8265}"
RAY_CLUSTER_JOIN_ADDRESS="${RAY_CLUSTER_JOIN_ADDRESS:-127.0.0.1:${RAY_HEAD_PORT}}"

# Ensure Hadoop/JVM native libraries are visible for libhdfs at runtime.
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
ARROW_LIBHDFS_DIR="${ARROW_LIBHDFS_DIR:-${HADOOP_HOME}/lib/native}"
LD_LIBRARY_PATH="${ARROW_LIBHDFS_DIR}:${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH:-}"
HDFS_HOST="${HDFS_HOST:-namenode}"
HDFS_PORT="${HDFS_PORT:-9000}"
HDFS_CLIENT_USE_DATANODE_HOSTNAME="${HDFS_CLIENT_USE_DATANODE_HOSTNAME:-true}"
case "${HDFS_CLIENT_USE_DATANODE_HOSTNAME,,}" in
  1|true|yes|y|on) HDFS_CLIENT_USE_DATANODE_HOSTNAME="true" ;;
  0|false|no|n|off) HDFS_CLIENT_USE_DATANODE_HOSTNAME="false" ;;
  *) HDFS_CLIENT_USE_DATANODE_HOSTNAME="true" ;;
esac
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/workspace/.runtime/hadoop-conf}"
mkdir -p "$HADOOP_CONF_DIR"

cat > "${HADOOP_CONF_DIR}/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${HDFS_HOST}:${HDFS_PORT}</value>
  </property>
</configuration>
EOF

cat > "${HADOOP_CONF_DIR}/hdfs-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>__DFS_CLIENT_USE_DATANODE_HOSTNAME__</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>__DFS_CLIENT_USE_DATANODE_HOSTNAME__</value>
  </property>
</configuration>
EOF
sed -i "s/__DFS_CLIENT_USE_DATANODE_HOSTNAME__/${HDFS_CLIENT_USE_DATANODE_HOSTNAME}/g" "${HADOOP_CONF_DIR}/hdfs-site.xml"

# Also write into default Hadoop conf dir so all Java entrypoints (incl. pyarrow JNI)
# resolve the same values even if they don't honor runtime-exported env vars.
mkdir -p "${HADOOP_HOME}/etc/hadoop"
cp -f "${HADOOP_CONF_DIR}/core-site.xml" "${HADOOP_HOME}/etc/hadoop/core-site.xml"
cp -f "${HADOOP_CONF_DIR}/hdfs-site.xml" "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml"

# Ensure libhdfs JVM picks our conf first.
BASE_CLASSPATH="${CLASSPATH:-${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*}"
CLASSPATH="${HADOOP_CONF_DIR}:${HADOOP_HOME}/etc/hadoop:${BASE_CLASSPATH}"

export JAVA_HOME HADOOP_HOME ARROW_LIBHDFS_DIR LD_LIBRARY_PATH HADOOP_CONF_DIR HDFS_HOST HDFS_PORT HDFS_CLIENT_USE_DATANODE_HOSTNAME CLASSPATH

if [[ "$RAY_NODE_ROLE" == "head" ]]; then
  exec ray start \
    --head \
    --node-ip-address="$RAY_NODE_IP" \
    --port="$RAY_HEAD_PORT" \
    --ray-client-server-port="$RAY_HEAD_CLIENT_PORT" \
    --dashboard-host=0.0.0.0 \
    --dashboard-port="$RAY_DASHBOARD_PORT" \
    --block
fi

exec ray start \
  --address="$RAY_CLUSTER_JOIN_ADDRESS" \
  --node-ip-address="$RAY_NODE_IP" \
  --block
