#!/usr/bin/env bash
# =============================================================================
#  BDSS Dev Cluster - copy a local folder into HDFS (idempotent, overwrites)
#
#  Usage (inside the dev container):
#      bash /opt/bdss-scripts/hdfs-sync.sh [LOCAL_DIR] [HDFS_DIR]
#      bash /opt/bdss-scripts/hdfs-sync.sh /data /data      # default
#
#  After this, Spark can read the same files two ways:
#      file:///data/orders.csv         (bind mount, no copy needed)
#      hdfs:///data/orders.csv         (distributed copy inside HDFS)
# =============================================================================
set -euo pipefail

SRC="${1:-/data}"
DST="${2:-/data}"

[ -d "$SRC" ] || { echo "Local folder not found: $SRC" >&2; exit 1; }

hdfs dfs -mkdir -p "$DST"
count=0
shopt -s nullglob
for item in "$SRC"/*; do
  hdfs dfs -put -f "$item" "$DST/"
  count=$((count + 1))
done
echo "Copied $count item(s) from $SRC to hdfs:$DST"
hdfs dfs -ls "$DST"
