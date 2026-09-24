#!/usr/bin/env bash
# Download the Kafka connector jars the fraud detector needs into ./jars.
# We pass these via spark-submit --jars instead of --packages so ivy does not
# resolve the huge hadoop transitive tree (pyspark already bundles hadoop).
# Run once after cloning.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p jars && cd jars

BASE=https://repo1.maven.org/maven2
fetch() { echo "fetching $2"; curl -sSL -o "$2" "$BASE/$1/$2"; }

fetch org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3            spark-sql-kafka-0-10_2.12-3.5.3.jar
fetch org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3 spark-token-provider-kafka-0-10_2.12-3.5.3.jar
fetch org/apache/kafka/kafka-clients/3.4.1                        kafka-clients-3.4.1.jar
fetch org/apache/commons/commons-pool2/2.11.1                     commons-pool2-2.11.1.jar

echo "done:"; ls -1 .
