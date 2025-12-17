#!/usr/bin/env bash
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic reviews_raw --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic reviews_enriched --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
