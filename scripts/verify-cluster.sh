#!/usr/bin/env bash
# =============================================================================
#  BDSS Dev Cluster - configuration and health check
#  Run inside the dev container:   bash /opt/bdss-scripts/verify-cluster.sh
#  Exit code 0 = no failures (warnings are allowed), 1 = at least one failure.
#  Read-only: nothing is changed in the cluster (it writes and deletes one tiny temp file in HDFS).
# =============================================================================
set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()      { echo "  [ OK ]  $*"; PASS=$((PASS + 1)); }
bad()     { echo "  [FAIL]  $*"; FAIL=$((FAIL + 1)); }
warn()    { echo "  [WARN]  $*"; WARN=$((WARN + 1)); }
section() { echo; echo "== $* =="; }

port_open() { nc -z -w 3 "$1" "$2" >/dev/null 2>&1; }

section "Client configuration"
echo "  user: $(id -un)   HADOOP_USER_NAME=${HADOOP_USER_NAME:-unset}"
if DEFAULT_FS="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null)"; then
  ok "fs.defaultFS = ${DEFAULT_FS}"
  case "$DEFAULT_FS" in
    hdfs://*) echo "         -> a path such as /data/x.csv means HDFS. Use file:///data/x.csv for the bind-mounted copy." ;;
    file:*)   warn "default filesystem is the local disk, not HDFS" ;;
  esac
else
  bad "cannot read fs.defaultFS (is /opt/hadoop/etc/hadoop present?)"
fi

# Ask every DataNode where it stores blocks (JMX on port 9864). Asking the local client would be wrong:
# the default folder depends on the OS user that runs the DataNode, not on the one running this script.
EXPECTED_TMP="${HADOOP_TMP_DIR:-/tmp/hadoop-user}"
for w in worker1 worker2 worker3 worker4; do
  VOL="$(curl -s -m 5 "http://${w}:9864/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo" 2>/dev/null | python3 -c '
import json, sys
try:
    info = json.load(sys.stdin)["beans"][0]["VolumeInfo"]
    print(" ".join(json.loads(info).keys()))
except Exception:
    pass' 2>/dev/null)"
  if [ -z "$VOL" ]; then
    warn "${w}: DataNode volume info not available (DataNode not running, or port 9864 closed)"
  else
    case "$VOL" in
      "$EXPECTED_TMP"/*) ok "${w}: blocks stored in ${VOL} (persisted volume)" ;;
      *) bad "${w}: blocks stored in ${VOL}, outside the persisted volume ${EXPECTED_TMP}. Set HADOOP_TMP_DIR in .env, then: docker compose up -d" ;;
    esac
  fi
done

section "HDFS identity"
HDFS_ME="${HADOOP_USER_NAME:-$(id -un)}"
HDFS_OWNER="$(hdfs dfs -stat '%u' /user 2>/dev/null || true)"
if [ -z "$HDFS_OWNER" ]; then
  warn "/user does not exist in HDFS yet (the init container creates it)"
elif [ "$HDFS_OWNER" = "$HDFS_ME" ]; then
  ok "HDFS user '${HDFS_ME}' owns /user - Spark on YARN can create /user/${HDFS_ME}/.sparkStaging"
else
  bad "HDFS user '${HDFS_ME}' cannot write /user (owner is '${HDFS_OWNER}'). Set HADOOP_USER_NAME=${HDFS_OWNER} in .env, then: docker compose up -d"
fi

section "HDFS high availability"
if STATES="$(hdfs haadmin -getAllServiceState 2>/dev/null)" && [ -n "$STATES" ]; then
  echo "$STATES" | sed 's/^/         /'
  ACTIVE="$(echo "$STATES" | grep -c 'active')"
  STANDBY="$(echo "$STATES" | grep -c 'standby')"
  [ "$ACTIVE" -eq 1 ]  && ok "exactly one active NameNode"  || bad "expected 1 active NameNode, found ${ACTIVE}"
  [ "$STANDBY" -eq 1 ] && ok "one standby NameNode"         || bad "expected 1 standby NameNode, found ${STANDBY}"
else
  bad "haadmin could not reach the NameNodes"
fi

section "HDFS data nodes and write test"
if REPORT="$(hdfs dfsadmin -report 2>/dev/null)"; then
  LIVE="$(echo "$REPORT" | grep -i -m1 'Live datanodes' | grep -o '[0-9]\+' | head -n1)"
  [ "${LIVE:-0}" -eq 4 ] && ok "4 live DataNodes" || bad "expected 4 live DataNodes, found ${LIVE:-0}"
else
  warn "dfsadmin -report needs the HDFS superuser (HADOOP_USER_NAME=${HADOOP_USER_NAME:-unset})"
fi
TMP="/tmp/bdss-verify-$$"
if echo "bdss" | hdfs dfs -put - "$TMP" >/dev/null 2>&1 && [ "$(hdfs dfs -cat "$TMP" 2>/dev/null)" = "bdss" ]; then
  ok "HDFS write/read round trip"
else
  bad "HDFS write/read round trip failed"
fi
hdfs dfs -rm -f -skipTrash "$TMP" >/dev/null 2>&1 || true

section "YARN"
if NODES="$(yarn node -list 2>/dev/null)"; then
  RUNNING="$(echo "$NODES" | grep -c 'RUNNING')"
  [ "$RUNNING" -eq 4 ] && ok "4 NodeManagers RUNNING" || bad "expected 4 RUNNING NodeManagers, found ${RUNNING}"
else
  bad "cannot reach the ResourceManager"
fi

section "Shared folders (bind mounts)"
[ -d /data ]      && [ -n "$(ls -A /data 2>/dev/null)" ]      && ok "/data is mounted and not empty"  || bad "/data is missing or empty"
[ -d /workspace ] && ok "/workspace is mounted" || bad "/workspace is missing"
echo "  (the Spark smoke test proves the SAME folders are visible on the YARN workers)"

section "Service ports"
while read -r host port label; do
  if port_open "$host" "$port"; then ok "${label} (${host}:${port})"; else warn "${label} not reachable at ${host}:${port} yet (still starting?)"; fi
done <<'EOF_PORTS'
zk 2181 ZooKeeper
metastore-db 5432 Hive-metastore-PostgreSQL
hive 10002 HiveServer2-UI
kafka 9092 Kafka
worker4 8000 Jupyter-Notebook
worker4 18080 Spark-UI-history-server
EOF_PORTS

section "Spark client configuration"
if grep -q '^spark.executorEnv.PYTHONPATH' "${SPARK_HOME:-/opt/spark}/conf/spark-defaults.conf" 2>/dev/null; then
  ok "executors get PYTHONPATH from spark-defaults.conf (Python code can run on YARN)"
else
  bad "spark.executorEnv.PYTHONPATH is missing from spark-defaults.conf: Python lambdas/UDFs fail on YARN executors. Rebuild: docker compose build dev"
fi

section "Toolchain in this container"
java -version >/dev/null 2>&1 && ok "Java: $(java -version 2>&1 | head -n1)" || bad "java not found"
command -v javac >/dev/null    && ok "javac present (Maven builds possible)" || warn "javac missing"
command -v mvn >/dev/null      && ok "Maven present" || warn "mvn missing"
SPARK_V="$(spark-submit --version 2>&1 | grep -m1 -o 'version [0-9.]*' | cut -d' ' -f2)"
[ "$SPARK_V" = "4.1.1" ] && ok "Spark ${SPARK_V}" || warn "Spark version is '${SPARK_V:-unknown}', the cluster uses 4.1.1"
python --version >/dev/null 2>&1 && ok "$(python --version 2>&1)"

echo
echo "Summary: ${PASS} ok, ${WARN} warning(s), ${FAIL} failure(s)"
[ "$FAIL" -eq 0 ]
