#!/usr/bin/env bash
# Launch the fraud detector locally using the pyspark bundled with the venv.
# Prereqs: docker stack up (kafka 9092, postgres 5433), venv installed.
set -euo pipefail
cd "$(dirname "$0")"

VENV="$PWD/.venv"
export SPARK_HOME="$VENV/lib/python3.12/site-packages/pyspark"
export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"

# Pass the Kafka connector jars directly (lz4/snappy/slf4j/hadoop are already in
# the pyspark bundle). This avoids ivy resolving the huge hadoop transitive tree.
# Re-fetch these into jars/ with: ./fetch_jars.sh
JARS=$(ls "$PWD"/jars/*.jar | paste -sd,)

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[*]" \
  --jars "$JARS" \
  --py-files events_pb2.py,config.py,sinks.py \
  fraud_detector.py
