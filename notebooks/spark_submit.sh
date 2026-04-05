#!/usr/bin/env bash
# Spark 3.5.x: Kafka source needs --packages; image already includes Iceberg — do not add a second Iceberg JAR.
set -euo pipefail
PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4"
exec spark-submit --packages "$PKG" "$@"
