---
description: Chạy pipeline hoàn chỉnh từ đầu đến cuối
---
# 🎵 Music Recommendation System - Pipeline

## 🚀 QUICK START

### BƯỚC 0: Khởi động Infrastructure
```bash
docker-compose up -d
# Đợi 2-3 phút
```

### BƯỚC 1: Cài đặt Dependencies
```bash
docker exec spark-master pip install python-dotenv
```

### BƯỚC 2: Download Data (~5 phút)
```bash
docker exec -it spark-master python3 /opt/src/scripts/download_data.py
```

### BƯỚC 3: Clean Data (~3 phút)
```bash
docker exec -it spark-master python3 /opt/src/scripts/fix_format.py
```

### BƯỚC 4: Sort Data (~10 phút)
```bash
docker exec -it spark-master spark-submit /opt/src/scripts/preprocess_sort.py
```

### BƯỚC 5: Streaming vào MinIO (2 TERMINALS, ~30 phút)

**Tạo topic Kafka trước:**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic music_log --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Terminal 1 - Spark Streaming:**
```bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/ingestion/stream_to_minio.py
```

**Terminal 2 - Producer:**
```bash
docker exec -it spark-master python3 /opt/src/ingestion/producer.py --speed 20000
```

> Đợi Producer xong → Ctrl+C cả 2 terminals

### BƯỚC 6: ETL Songs → MongoDB (~5-10 phút)
```bash
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_master_data.py
```

**Kiểm tra log:**
```bash
docker exec spark-master cat /tmp/etl_songs.log
```

### BƯỚC 7: ETL Users → MongoDB (~2 phút)
```bash
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_users.py
```

**Kiểm tra log:**
```bash
docker exec spark-master cat /tmp/etl_users.log
```

### BƯỚC 8: Kiểm tra MongoDB
```bash
docker exec mongodb mongosh music_recsys --eval "db.songs.countDocuments()"
docker exec mongodb mongosh music_recsys --eval "db.users.countDocuments()"
```

### BƯỚC 9: Train ALS Model (~30 phút)
```bash
docker exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/modeling/train_als_model.py
```

### BƯỚC 10 (Optional): Enrich Lyrics (~11-12 giờ)
```bash
# Cần mount folder src/enrichment vào container trước
docker exec -it spark-master python3 /opt/src/enrichment/fetch_lyrics_lrclib.py
```

### BƯỚC 11 (Optional): Create Lyrics Embeddings (~1 giờ)
```bash
docker exec spark-master pip install sentence-transformers
docker exec spark-master python3 /opt/src/modeling/create_lyrics_embeddings.py
```

---

## 📋 COPY-PASTE COMMANDS

```bash
# Infrastructure
docker-compose up -d

# Dependencies
docker exec spark-master pip install python-dotenv

# Data Preparation
docker exec -it spark-master python3 /opt/src/scripts/download_data.py
docker exec -it spark-master python3 /opt/src/scripts/fix_format.py
docker exec -it spark-master spark-submit /opt/src/scripts/preprocess_sort.py

# Create Kafka topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic music_log --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Streaming (2 terminals)
# Terminal 1:
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/ingestion/stream_to_minio.py
# Terminal 2:
docker exec -it spark-master python3 /opt/src/ingestion/producer.py --speed 20000

# ETL (sau streaming xong)
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_master_data.py
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_users.py

# Verify
docker exec mongodb mongosh music_recsys --eval "db.songs.countDocuments()"
docker exec mongodb mongosh music_recsys --eval "db.users.countDocuments()"

# Train ALS
docker exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/modeling/train_als_model.py
```

---

## 🔧 Troubleshooting

### Xem log ETL
```bash
docker exec spark-master cat /tmp/etl_songs.log
docker exec spark-master cat /tmp/etl_users.log
```

### Lỗi "No module named 'dotenv'"
```bash
docker exec spark-master pip install python-dotenv
```

### Lỗi Kafka topic không tồn tại
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic music_log --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### MongoDB connector version
- Dùng `mongo-spark-connector_2.12:3.0.1` với `format("mongo")`
- KHÔNG dùng version 10.x (không tương thích)

---

## 📁 Mapping Đường Dẫn

| Local | Container |
|:------|:----------|
| `data_pipeline/` | `/opt/src/` |
| `data/` | `/opt/data/` |
| `common/` | `/opt/src/common/` |
