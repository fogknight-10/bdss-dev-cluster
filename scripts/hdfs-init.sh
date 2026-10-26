#!/usr/bin/env bash
# =============================================================================
#  BDSS Dev Cluster - one-shot HDFS initialisation (run by the "init" service)
#  Waits for an active NameNode, leaves safe mode, creates the HDFS folders the
#  labs expect and - if HDFS_SYNC_ON_START=true - mirrors ./data into hdfs:///data.
#  Safe to re-run.
# =============================================================================
set -euo pipefail

USER_NAME="${HADOOP_USER_NAME:-user}"
log() { echo "[hdfs-init] $*"; }

log "waiting for an active NameNode (up to 10 minutes)..."
ready=false
for _ in $(seq 1 120); do
  if hdfs dfs -ls / >/dev/null 2>&1; then ready=true; break; fi
  sleep 5
done
if [ "$ready" != true ]; then
  log "ERROR: no active NameNode answered. Check:  docker compose logs master1 master2"
  exit 1
fi

log "waiting for the NameNode to leave safe mode..."
hdfs dfsadmin -safemode wait || log "WARN: could not query safe mode (not a superuser?) - continuing"

OWNER="$(hdfs dfs -stat '%u' /user 2>/dev/null || true)"
if [ -n "$OWNER" ] && [ "$OWNER" != "$USER_NAME" ]; then
  log "ERROR: /user in HDFS is owned by '${OWNER}', not '${USER_NAME}', so '${USER_NAME}' cannot create /user/${USER_NAME}."
  log "       Set HADOOP_USER_NAME=${OWNER} in .env and run: docker compose up -d"
  exit 1
fi

log "creating HDFS folders for user '${USER_NAME}'"
hdfs dfs -mkdir -p "/user/${USER_NAME}" /tmp /data
hdfs dfs -chmod 1777 /tmp || true
hdfs dfs -chmod 777 /data || true

if [ "${HDFS_SYNC_ON_START:-false}" = "true" ]; then
  log "HDFS_SYNC_ON_START=true -> mirroring /data into hdfs:///data"
  bash /opt/bdss-scripts/hdfs-sync.sh /data /data
fi

log "done. fs.defaultFS = $(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo unknown)"
