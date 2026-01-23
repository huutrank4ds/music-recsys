#!/bin/bash
# start_streams.sh

echo "🚀 Starting Spark Streaming Jobs..."

# 1. Job Real-time Analytics (Ghi MongoDB) - Chạy ngầm (-d)
docker exec -d spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 800m \
  --total-executor-cores 1 \
  --conf "spark.kafka.consumer.cache.enabled=false" \
  /opt/src/jobs/stream_to_mongo.py

echo "✅ Job MongoDB Started."

# 2. Job Archiver (Ghi MinIO) - Chạy ngầm (-d)
docker exec -d spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 800m \
  --total-executor-cores 1 \
  --conf "spark.kafka.consumer.cache.enabled=false" \
  /opt/src/jobs/stream_to_minio.py

echo "✅ Job MinIO Started."