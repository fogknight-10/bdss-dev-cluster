# syntax=docker/dockerfile:1
# =============================================================================
#  BDSS Dev Cluster - VS Code client image
#  Built by Mohamed (github.com/fogknight-10) - https://github.com/fogknight-10/bdss-dev-cluster
#  using the original cluster by usf-etman (https://github.com/usf-etman/bdss-cluster).
#
#  Hadoop 3.4.2 and Spark 4.1.1 (binaries + configuration) are copied from the
#  cluster's own image, so this client always matches the cluster.
# =============================================================================
ARG CLUSTER_SPARK_IMAGE=ysfetman/bdss-hadoop-spark:4.1.1
FROM ${CLUSTER_SPARK_IMAGE} AS cluster

FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

ARG AUTHOR_NAME="Mohamed"
LABEL org.opencontainers.image.title="BDSS Dev Cluster - VS Code client" \
      org.opencontainers.image.authors="${AUTHOR_NAME}" \
      org.opencontainers.image.description="Hadoop 3.4.2 / Spark 4.1.1 client with JDK 17, Maven and Python 3.12 for the BDSS Dev Cluster" \
      org.opencontainers.image.base.name="ysfetman/bdss-hadoop-spark:4.1.1" \
      org.opencontainers.image.source="https://github.com/fogknight-10/bdss-dev-cluster" \
      org.opencontainers.image.url="https://github.com/fogknight-10/bdss-dev-cluster" \
      org.opencontainers.image.documentation="https://github.com/fogknight-10/bdss-dev-cluster#readme" \
      io.bdss.based-on="https://github.com/usf-etman/bdss-cluster"

# JDK 17 (not just the JRE: Maven builds MapReduce jars), Maven and small network/diagnostic tools.
# The base image ships a Yarn apt source whose signing key has expired; Yarn is not needed, so drop it first.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
 && (grep -rl yarnpkg /etc/apt/sources.list.d/ 2>/dev/null | xargs -r rm -f) \
 && apt-get update && export DEBIAN_FRONTEND=noninteractive \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless maven git curl netcat-openbsd procps iputils-ping dnsutils less nano unzip \
 && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /usr/lib/jvm/java-17 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Hadoop and Spark exactly as in the cluster (same version, same *-site.xml / spark-defaults.conf)
COPY --from=cluster /opt/hadoop /opt/hadoop
COPY --from=cluster /opt/spark  /opt/spark

# py4j is versioned inside its file name; a stable link keeps PYTHONPATH independent of the version.
# The build fails here, with a clear reason, if the cluster image does not have the expected layout.
RUN ln -sfn "$(ls /opt/spark/python/lib/py4j-*-src.zip | head -n 1)" /opt/spark/python/lib/py4j-src.zip \
 && test -d /opt/hadoop/etc/hadoop \
 && test -x /opt/spark/bin/spark-submit

# YARN containers do not inherit the NodeManager's environment, so Python code on executors (RDD lambdas, UDFs)
# fails with "No module named 'pyspark'". Point executors and the application master at the Spark install that
# every worker already has. This edits only this client's copy of spark-defaults.conf; the cluster is unchanged.
RUN PY4J="$(basename "$(ls /opt/spark/python/lib/py4j-*-src.zip | head -n 1)")" \
 && { echo ""; \
      echo "# --- added by the BDSS dev container (client side only) ---"; \
      echo "spark.executorEnv.PYTHONPATH        /opt/spark/python:/opt/spark/python/lib/${PY4J}"; \
      echo "spark.yarn.appMasterEnv.PYTHONPATH  /opt/spark/python:/opt/spark/python/lib/${PY4J}"; \
    } >> /opt/spark/conf/spark-defaults.conf

ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop \
    LD_LIBRARY_PATH=/opt/hadoop/lib/native \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin:${PATH}

# Python packages are baked into the image so they survive container re-creation
COPY .devcontainer/requirements.txt /tmp/pip-requirements.txt
RUN pip install --no-cache-dir -r /tmp/pip-requirements.txt

# VS Code Server and its extensions live in a named volume (see docker-compose.yml) so that a re-created container
# does not download them again. The folder must exist in the image, owned by vscode, so the new volume inherits it.
RUN mkdir -p /home/vscode/.vscode-server && chown -R vscode:vscode /home/vscode/.vscode-server

# Files written to /data and /workspace by this user and by the cluster (root) must stay writable for both
RUN echo "umask 000" >> /etc/bash.bashrc && echo "umask 000" > /etc/profile.d/umask.sh

# Signature banner shown in interactive shells
RUN printf "%s\n" \
  "  BDSS Dev Cluster  -  built by ${AUTHOR_NAME}" \
  "  github.com/fogknight-10/bdss-dev-cluster  -  built using the original cluster by usf-etman (github.com/usf-etman/bdss-cluster)" \
  "  Hadoop 3.4.2 | Spark 4.1.1 on YARN | Hive 4.1.0 | Kafka 4.2.0 | Python 3.12 | Java 17" \
  "  Check the cluster:  bash /opt/bdss-scripts/verify-cluster.sh" > /etc/bdss-banner \
 && echo '[ -n "$PS1" ] && [ -z "$BDSS_BANNER_SHOWN" ] && export BDSS_BANNER_SHOWN=1 && cat /etc/bdss-banner' >> /etc/bash.bashrc

WORKDIR /workspace
