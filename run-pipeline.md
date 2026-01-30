---
description: Chạy pipeline hoàn chỉnh từ đầu đến cuối (Updated Schema)
---
# 🎵 Music Recommendation System - Pipeline (Updated)

Hướng dẫn chạy toàn bộ hệ thống xử lý dữ liệu, huấn luyện mô hình và kiểm thử.
**Lưu ý:** Codebase đã được cập nhật để sử dụng Schema chuẩn (`track_name`, `artist_name`, `plays_` as String, `duration` as String).

## 🚀 QUICK START (Start Infrastructure)

### BƯỚC 0: Khởi động Docker
```bash
docker-compose up -d
# Đợi 2-3 phút cho các services (MongoDB, MinIO, Spark, Milvus, Kafka) khởi động hoàn tất
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

### BƯỚC 3: Clean Data Format (~3 phút)
```bash
docker exec -it spark-master python3 /opt/src/scripts/fix_format.py
```

### BƯỚC 4: Sort Data (~10 phút)
```bash
docker exec -it spark-master spark-submit /opt/src/scripts/preprocess_sort.py
```

### BƯỚC 5: Streaming vào MinIO (2 TERMINALS, ~30 phút)

**Terminal 1 - Spark Streaming:**
```bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/ingestion/stream_to_minio.py
```

**Terminal 2 - Producer:**
*(Mở terminal mới)*
```bash
docker exec -it spark-master python3 /opt/src/ingestion/producer.py --speed 20000
```
> Đợi Producer chạy xong → Ctrl+C cả 2 terminals.

### BƯỚC 6: ETL Songs & Users (~10 phút)
*Tạo bảng `songs` và `users` trong MongoDB. Code đã được update để dùng cột `track_name`, `artist_name` và ép kiểu String.*
```bash
# ETL Songs
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_master_data.py

# ETL Users
docker exec spark-master spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/src/batch/etl_users.py
```

### BƯỚC 7: Train ALS Model (Collaborative Filtering) (~30 phút)
*Tạo vector sở thích người dùng. Lưu ý: Sử dụng RAM 2G cho Driver để tránh OOM.*
```bash
docker exec spark-master spark-submit --driver-memory 2g --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/modeling/train_als_model.py
```

### BƯỚC 8: Enrich Lyrics (Fetch Data) (~10-15 giờ)
*Tải lời bài hát từ LRCLIB. Code đã update để tự động điền `duration=300` và `release_date=null` nếu thiếu.*
```bash
docker exec -e MONGO_URI=mongodb://mongodb:27017 spark-master python3 -u /opt/src/scripts/fetch_lyrics_lrclib.py
```

### BƯỚC 9: Clean Data (Bắt buộc)
*Xóa các bài hát không tìm thấy lyric để chuẩn bị tạo Vector (tránh lỗi).*
```bash
docker exec -it -e MONGO_URI="mongodb://mongodb:27017" -e MILVUS_HOST="milvus-standalone" spark-master python3 /opt/src/scripts/clean_and_sync_data.py --yes
```

### BƯỚC 10: Create Lyrics Embeddings (~1-2 giờ)
*Tạo vector nội dung từ lời bài hát và đẩy vào Milvus.*
```bash
# Đảm bảo Milvus đang chạy
docker start milvus-standalone
timeout 30

docker exec -e MONGO_URI=mongodb://mongodb:27017 spark-master python3 -u /opt/src/modeling/create_lyrics_embeddings.py
```

# ================================================================
# PHASE 2: EXPORT & SERVING
# ================================================================

### BƯỚC 11: Export Clean Data (Optional)
*Xuất dữ liệu `users` và `songs` ra file JSONL chuẩn để kiểm tra hoặc backup.*
```bash
# Export Songs
docker exec spark-master python3 /opt/src/scripts/export_clean_dataset.py
docker cp spark-master:/opt/data/songs_clean.jsonl ./songs_clean.jsonl

# Export Users
docker exec mongodb mongoexport --db music_recsys --collection users --out /data/db/users.jsonl
docker cp mongodb:/data/db/users.jsonl ./users.jsonl
```

### BƯỚC 12: Setup Backend & Test API
```bash
# Start Backend
docker-compose up -d backend

# Verify Logic
docker cp verify_hybrid_api.py music_backend:/app/verify_hybrid_api.py
docker exec music_backend python verify_hybrid_api.py
```

---

## 🔧 Mapping Đường Dẫn
| Local | Container |
|:------|:----------|
| `data_pipeline/` | `/opt/src/` |
| `data/` | `/opt/data/` |
