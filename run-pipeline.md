---
description: Chạy pipeline hoàn chỉnh từ đầu đến cuối
---
# 🎵 Music Recommendation System - Pipeline

## 🚀 QUICK START

### BƯỚC 0: Khởi động Infrastructure
```bash
docker-compose up -d
# Đợi 2-3 phút cho các services khởi động
```

# ================================================================
# PHASE 1: DATA PIPELINE (ETL & MODELING)
# ================================================================

### BƯỚC 1: Cài đặt Dependencies (Spark)
```bash
docker exec spark-master pip install python-dotenv sentence-transformers pymilvus tqdm aiohttp
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

### BƯỚC 6: ETL Songs & Users (~10 phút)
```bash
# ETL Songs
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_master_data.py

# ETL Users
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_users.py
```

### BƯỚC 7: Train ALS Model (Collaborative Filtering) (~30 phút)
```bash
docker exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/modeling/train_als_model.py
```

### BƯỚC 8: Enrich Lyrics (Content-Based Data) (~10-15 giờ)
*Bước này quan trọng cho tính năng Hybrid Recommendation. Có thể chạy qua đêm.*
```bash
docker exec -e MONGO_URI=mongodb://mongodb:27017 spark-master python3 -u /opt/src/scripts/fetch_lyrics_lrclib.py
```
*Note: Script hỗ trợ resume (bỏ qua bài đã fetch).*

### BƯỚC 9: Create Lyrics Embeddings (~1-2 giờ)
*Tạo vector từ lyrics và lưu vào Milvus.*
```bash
# Đảm bảo Milvus (milvus-standalone) đang chạy
docker start milvus-standalone
Start-Sleep -Seconds 30

docker exec -e MONGO_URI=mongodb://mongodb:27017 spark-master python3 -u /opt/src/modeling/create_lyrics_embeddings.py
```

# ================================================================
# PHASE 2: SERVING LAYEER (BACKEND API)
# ================================================================

### BƯỚC 10: Setup Backend
Backend cần một số thư viện bổ sung.
```bash
# Rebuild nếu cần (Recommended)
docker-compose build backend
docker-compose up -d backend

# Hoặc cài nhanh (Temporary):
docker exec music_backend pip install confluent-kafka marshmallow
docker restart music_backend
```

### BƯỚC 11: Test API Verification
Kiểm tra xem API có hoạt động đúng logic Hybrid không.
```bash
# Copy script test vào container
docker cp verify_hybrid_api.py music_backend:/app/verify_hybrid_api.py

# Chạy test
docker exec music_backend python verify_hybrid_api.py
```

---

## 🔧 Troubleshooting

### Milvus Connection Error
Nếu gặp lỗi kết nối Milvus từ Spark hoặc Backend:
1. Đảm bảo container `milvus-standalone` đang chạy.
2. Kiểm tra log: `docker logs milvus-standalone --tail 50`.
3. Kiểm tra network: Cả `spark-master`, `music_backend` và `milvus-standalone` phải cùng trong network `bigdata-net`.

### Backend "Connection Refused"
1. Kiểm tra Backend log: `docker logs music_backend --tail 50`.
2. Nếu lỗi "ModuleNotFoundError", hãy cài lại thư viện thiếu.
3. Nếu lỗi DB connection, hãy restart Backend: `docker restart music_backend`.

### Lyrics Enrichment chạy chậm?
Script đã được tối ưu Concurrent (Async). Nếu vẫn chậm, kiểm tra mạng internet hoặc giảm `REQUEST_DELAY` trong code.

---

## 📁 Mapping Đường Dẫn

| Local | Container |
|:------|:----------|
| `data_pipeline/` | `/opt/src/` |
| `data/` | `/opt/data/` |
| `common/` | `/opt/src/common/` |
