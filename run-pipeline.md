---
description: Chạy pipeline streaming từ đầu đến cuối (Download → Clean → Sort → Kafka → Spark → MinIO → MongoDB → Milvus)
---
# Music Recommendation System - Full Pipeline

Pipeline xử lý data streaming từ HuggingFace đến MinIO Data Lake, MongoDB và Milvus.

## Kiến trúc Pipeline

```
HuggingFace Dataset (LastFM-1K)
        ↓
   [Download]
        ↓
  data/raw/*.parquet
        ↓
   [Clean/Fix Format]
        ↓
  data/data_clean/*.parquet
        ↓
   [Sort by Timestamp]
        ↓
  data/processed_sorted/*.parquet
        ↓
   [Producer TURBO] ──→ Kafka Topic "music_log"
                             ↓
               [Spark Streaming TURBO ETL]
                             ↓
               MinIO (s3a://datalake/raw/music_logs/)
                             ↓
               [ETL Master Data]
                             ↓
               MongoDB (music_recsys.songs)
                             ↓
               [ETL Users]
                             ↓
               MongoDB (music_recsys.users)
                             ↓
               [Train ALS Model]
                    ↓         ↓
           MongoDB        Milvus
        (user vectors)  (item vectors)
```

---

## BƯỚC 0: Khởi động Infrastructure

```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra services đã sẵn sàng
docker-compose ps
```

Đợi đến khi tất cả services healthy (khoảng 2-3 phút cho Milvus).

**Các services cần chạy:**

- `kafka` (Healthy)
- `kafka-ui`
- `spark-master`
- `spark-worker`
- `minio`
- `mongodb`
- `milvus-etcd`
- `milvus-standalone`

**Kiểm tra Milvus đã sẵn sàng:**

```bash
docker logs milvus-standalone 2>&1 | tail -5
# Nếu thấy "Milvus Proxy successfully initialized" là OK
```

---

## BƯỚC 1: Download Data từ HuggingFace

// turbo

```bash
docker exec -it spark-master python3 /opt/src/prepare-data/download_data.py
```

**Output mong đợi:**

```
Đang download dataset từ HuggingFace: matthewfranglen/lastfm-1k...
Xử lý split: train
  - Số dòng: xxx
  - Đã lưu: /opt/data/raw/train.parquet
...
HOÀN THÀNH!
```

**Kiểm tra:**

```bash
docker exec spark-master ls -lh /opt/data/raw/
```

---

## 🧹 BƯỚC 2: Clean Data (Fix Format)

// turbo

```bash
docker exec -it spark-master python3 /opt/src/prepare-data/fix_format.py
```

**Output mong đợi:**

```
 BẮT ĐẦU QUY TRÌNH TÁI SINH DỮ LIỆU
 Xử lý: train.parquet
   Đã tái sinh thành công
...
```

**Kiểm tra:**

```bash
docker exec spark-master ls -lh /opt/data/data_clean/
```

---

## BƯỚC 3: Sort Data theo Timestamp

// turbo

```bash
docker exec -it spark-master spark-submit /opt/src/prepare-data/etl_sort.py
```

**Output mong đợi:**

```
Khởi động Spark Session...
Đang đọc dữ liệu sạch từ: /opt/data/data_clean/*.parquet
Đang sắp xếp theo thời gian...
Đang ghi dữ liệu đã sắp xếp ra: /opt/data/processed_sorted
THÀNH CÔNG!
```

**Kiểm tra:**

```bash
docker exec spark-master ls -lh /opt/data/processed_sorted/
```

---

## BƯỚC 4: Khởi động Spark Streaming TURBO ETL (Terminal 1)

**Mở Terminal mới** và chạy:

```bash
docker exec -it spark-master bash -c "cd /opt/src/pipelines/ingestion && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 stream_to_minio_turbo.py"
```

> ⚡ **TURBO Mode**: Trigger mỗi 10 giây (thay vì 1 phút), fetch size lớn hơn, tối ưu S3A upload.

**Output mong đợi:**

```
🚀 Khởi động Spark Streaming TURBO ETL...
⚡ Đang lắng nghe Kafka Topic 'music_log' với TURBO settings...
💾 Đang ghi xuống MinIO (Parquet) - TURBO MODE (10s trigger)...
```

**QUAN TRỌNG: Giữ terminal này mở, đừng tắt!**

---

## BƯỚC 5: Chạy Producer (Terminal 2)

**Mở Terminal mới** và chọn 1 trong 3 mode:

### 🎯 Option A: BALANCED Mode (Khuyến nghị)

```bash
docker exec -it spark-master python3 /opt/src/pipelines/ingestion/producer_balanced.py
```

> ⚖️ **BALANCED Mode**: Cân bằng giữa tốc độ và tính realtime
> - Giữ timestamp gốc (quan trọng cho time-series analytics)
> - Tăng tốc x1000 (1 giờ data = 3.6 giây thực)
> - Nhảy qua khoảng trống > 5 phút
> - Tốc độ: ~1,000-5,000 msg/s

**Output mong đợi:**

```
⚡ BALANCED MODE: x1000.0 speed, batch 500
   Skip gaps > 5 minutes
📖 Đọc file: part-00000-xxx.parquet
📊 Sent: 5,000 | Rate: 2,345 msg/s | Elapsed: 2.1s
⏩ Skip gap 45.2 phút
📊 Sent: 10,000 | Rate: 2,100 msg/s | Elapsed: 4.8s
...
🎉 DONE: xxx messages in Xs
```

### ⚡ Option B: TURBO Mode (Nhanh nhất)

```bash
docker exec -it spark-master python3 /opt/src/pipelines/ingestion/producer_turbo.py
```

> ⚡ **TURBO Mode**: Gửi data tốc độ TỐI ĐA
> - Không giữ timestamp gốc (ghi đè bằng thời gian hiện tại)
> - Không delay, không giả lập realtime
> - Tốc độ: ~10,000+ msg/s

### 🐢 Option C: Normal Mode (Realtime simulation)

```bash
docker exec -it spark-master python3 /opt/src/pipelines/ingestion/producer.py
```

> 🐢 **Normal Mode**: Giả lập thời gian thực
> - Giữ timestamp gốc
> - Chậm, phù hợp demo realtime
> - Tốc độ: ~5 msg/s (với x200 speed factor)

---

### 📊 So sánh 3 modes:

| Mode | Tốc độ | Thời gian 1M msg | Giữ timestamp | Use case |
|------|--------|------------------|---------------|----------|
| **producer.py** | ~5 msg/s | ~55 giờ | ✅ | Demo realtime |
| **producer_balanced.py** ⭐ | ~2,000 msg/s | ~8 phút | ✅ | **Dev/Test** |
| **producer_turbo.py** | ~10,000+ msg/s | ~2 phút | ❌ | Load data nhanh |

**Đợi Producer chạy xong** hoặc nhấn `Ctrl+C` khi đủ data.

---

## BƯỚC 6: Kiểm tra Kết quả trong MinIO

```bash
# Kiểm tra data đã được ghi
docker exec minio mc ls local/datalake/raw/music_logs/ --recursive --summarize
```

**Hoặc truy cập MinIO Console:**

- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`
- Navigate to: `datalake` → `raw` → `music_logs`

---

## BƯỚC 7: ETL Master Data (MinIO → MongoDB songs)

**Dừng Spark Streaming** (Terminal 1) bằng `Ctrl+C`, sau đó restart Spark cluster:

```bash
# Restart Spark để giải phóng resources
docker restart spark-master spark-worker

# Đợi 15-20 giây cho cluster sẵn sàng
```

**Chạy ETL Master Data:**

```bash
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/src/pipelines/batch/etl_master_data.py
```

**Output mong đợi:**

```
Bắt đầu ETL Master Data (Collection: songs)...
>>> Đang đọc dữ liệu từ MinIO...
>>> Đang lọc bài hát duy nhất...
>>> Đang ghi vào MongoDB...
THÀNH CÔNG! Đã lưu xxx bài hát vào MongoDB.
```

---

## BƯỚC 8: ETL Users (MinIO → MongoDB users)

```bash
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/src/pipelines/batch/etl_users.py
```

**Output mong đợi:**

```
🎵 ETL Users Collection (MongoDB)
>>> Đang đọc dữ liệu từ MinIO...
>>> Đang lọc users duy nhất...
>>> Đang ghi vào MongoDB...
✅ THÀNH CÔNG! Đã lưu xxx users vào MongoDB.
```

---

## BƯỚC 9: Train ALS & Sync Vectors (MongoDB + Milvus)

**Chạy Training:**

```bash
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/pipelines/batch/train_als_model.py
```

**Output mong đợi:**

```
============================================================
🎵 MUSIC RECOMMENDATION - ALS BATCH TRAINING
   Started at: 2026-01-18 05:00:00
============================================================
>>> Loading data from MinIO (Last 90 days)...
>>> Preparing data for ALS...
>>> Training ALS Model...
    Rank: 64, MaxIter: 15, RegParam: 0.1
>>> ALS Model trained successfully!
>>> Syncing User Factors to MongoDB...
>>> MongoDB: Upserted xxx users
>>> Syncing Item Factors to Milvus...
>>> Setting up Milvus collection 'music_collection'...
>>> Milvus collection created with dimension=64
    Inserted batch 1: 1000 items
    Inserted batch 2: 1000 items
    ...
>>> Milvus: Inserted xxx item embeddings
============================================================
✅ TRAINING COMPLETED SUCCESSFULLY!
   Users synced to MongoDB: xxx
   Items synced to Milvus: xxx
============================================================
```

---

## BƯỚC 10: Kiểm tra Kết quả

### MongoDB

```bash
# Đếm số bài hát
docker exec mongodb mongo music_recsys --eval "db.songs.count()"

# Xem 1 bài hát mẫu
docker exec mongodb mongo music_recsys --eval "db.songs.findOne()"

# Đếm số users
docker exec mongodb mongo music_recsys --eval "db.users.count()"

# Xem user với latent_vector (kiểm tra đã có vector chưa)
docker exec mongodb mongo music_recsys --quiet --eval "var u = db.users.findOne(); print('User:', u._id); print('Vector length:', u.latent_vector ? u.latent_vector.length : 0)"
```

### Milvus

```bash
# Kiểm tra Milvus collection
docker exec spark-master python3 -c "
from pymilvus import connections, Collection, utility
connections.connect(host='milvus', port=19530)
print('Collections:', utility.list_collections())
if 'music_collection' in utility.list_collections():
    c = Collection('music_collection')
    print('Entities:', c.num_entities)
    print('Schema:', c.schema)
connections.disconnect('default')
"
```

---

## Monitoring & Debug

### Kafka UI

- URL: http://localhost:8080
- Xem topic `music_log` và số messages

### Spark Master UI

- URL: http://localhost:9090
- Xem running applications và jobs

### MinIO Console

- URL: http://localhost:9001
- Xem data đã được ghi

### Milvus (Check via logs)

```bash
docker logs milvus-standalone --tail 20
```

---

## Dừng Pipeline

```bash
# Terminal chạy Producer: Ctrl+C
# Terminal chạy Spark Streaming: Ctrl+C

# Tắt tất cả services
docker-compose down
```

---

## Chạy lại từ đầu (Reset)

```bash
# 1. Xóa data cũ trên máy local
rm -rf data/raw data/data_clean data/processed_sorted

# 2. Xóa MinIO data (trong container)
docker exec minio mc rm local/datalake/checkpoints/ --recursive --force
docker exec minio mc rm local/datalake/raw/ --recursive --force

# 3. Xóa MongoDB data
docker exec mongodb mongo music_recsys --eval "db.songs.drop()"
docker exec mongodb mongo music_recsys --eval "db.users.drop()"

# 4. Xóa Milvus data (restart container)
docker-compose restart milvus-standalone

# 5. Chạy lại từ BƯỚC 1
```

---

## Cấu hình quan trọng

| Config             | File                                           | Giá trị                     |
| ------------------ | ---------------------------------------------- | --------------------------- |
| Kafka Bootstrap    | `producer*.py`, `stream_to_minio*.py`          | `kafka:9092`                |
| MinIO Endpoint     | `stream_to_minio*.py`, `etl_master_data.py`    | `http://minio:9000`         |
| MongoDB URI        | `etl_master_data.py`, `train_als_model.py`     | `mongodb://mongodb:27017`   |
| **Milvus Host**    | `train_als_model.py`                           | `milvus:19530`              |
| Spark Master       | `etl_master_data.py`                           | `spark://spark-master:7077` |
| Processing Trigger | `stream_to_minio.py`                           | `1 minute`                  |
| Processing Trigger | `stream_to_minio_turbo.py` ⚡                  | `10 seconds`                |
| Producer Speed     | `producer.py`                                  | `x200` (realtime simulation)|
| Producer Speed     | `producer_balanced.py` ⭐                      | `x1000` (fast + timestamp)  |
| Producer Speed     | `producer_turbo.py` ⚡                         | `MAX` (no delay)            |
| **ALS Rank**       | `train_als_model.py`                           | `64` (vector dimension)     |
| **Sliding Window** | `train_als_model.py`                           | `90 days`                   |

---

## Troubleshooting

### 1. Spark job báo "Initial job has not accepted any resources"

**Nguyên nhân:** Spark Worker chưa register hoặc đang bận với job khác.

**Giải pháp:**

```bash
# Restart Spark cluster
docker restart spark-master spark-worker

# Đợi 15-20 giây rồi chạy lại
```

### 2. MongoDB không khởi động được (Exit code 62)

**Nguyên nhân:** Phiên bản MongoDB mới yêu cầu CPU hỗ trợ AVX.

**Giải pháp:** Sử dụng MongoDB 4.4 trong file `.env`:

```
MONGO_IMAGE=mongo:4.4
```

### 3. Docker Desktop không chạy

**Nguyên nhân:** Docker Desktop chưa start.

**Giải pháp:**

```bash
# Windows: Mở Docker Desktop từ Start Menu
# Hoặc chạy:
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"

# Đợi Docker ready rồi kiểm tra:
docker info
```

### 4. Producer không gửi được message

**Nguyên nhân:** Kafka chưa healthy.

**Giải pháp:**

```bash
# Kiểm tra Kafka status
docker-compose ps kafka

# Đợi đến khi Kafka healthy rồi chạy lại
```

### 5. Milvus không khởi động

**Nguyên nhân:** etcd hoặc MinIO chưa sẵn sàng.

**Giải pháp:**

```bash
# Kiểm tra etcd
docker logs milvus-etcd

# Restart Milvus
docker-compose restart milvus-standalone

# Đợi 1-2 phút cho Milvus khởi động
```

### 6. Lỗi "Connection refused" khi sync Milvus

**Nguyên nhân:** Milvus chưa fully started.

**Giải pháp:**

```bash
# Kiểm tra Milvus health
docker logs milvus-standalone 2>&1 | grep -i "successfully"

# Đợi thấy dòng "Milvus Proxy successfully initialized" rồi chạy lại
```

### 7. Lỗi "'list' object has no attribute 'toArray'"

**Nguyên nhân:** Spark 3.5 ALS trả về features dạng list thay vì DenseVector.

**Giải pháp:** Đã được fix trong `train_als_model.py` - sử dụng hàm `convert_vector()` để handle cả 2 trường hợp.

---

## 📁 Cấu trúc Project

```
music-recsys/
├── data/
│   ├── raw/                    # Data download từ HuggingFace
│   ├── data_clean/             # Data đã clean
│   └── processed_sorted/       # Data đã sort theo timestamp
├── src/
│   ├── prepare-data/
│   │   ├── download_data.py    # Download từ HuggingFace
│   │   ├── fix_format.py       # Clean data
│   │   └── etl_sort.py         # Sort theo timestamp
│   └── pipelines/
│       ├── ingestion/
│       │   ├── producer.py         # 🐢 Gửi data (chậm, simulate realtime x200)
│       │   ├── producer_balanced.py # ⭐ Cân bằng speed/realtime (x1000)
│       │   ├── producer_turbo.py   # ⚡ Gửi data tốc độ MAX
│       │   ├── stream_to_minio.py  # Spark Streaming: Kafka → MinIO (1 min trigger)
│       │   └── stream_to_minio_turbo.py  # ⚡ Turbo mode (10s trigger)
│       └── batch/
│           ├── etl_master_data.py  # ETL: MinIO → MongoDB (songs)
│           ├── etl_users.py        # ETL: MinIO → MongoDB (users)
│           └── train_als_model.py  # ALS Training → MongoDB + Milvus
├── docker-compose.yml
├── spark.Dockerfile
├── .env
└── run-pipeline.md             # File hướng dẫn này
```

---

## 📊 Kết quả Pipeline

Sau khi chạy xong toàn bộ pipeline:

| Database    | Collection         | Nội dung                                          |
| :---------- | :----------------- | :------------------------------------------------ |
| **MongoDB** | `songs`            | Metadata bài hát (id, title, artist, track_index) |
| **MongoDB** | `users`            | User profile + latent_vector (64-dim)             |
| **Milvus**  | `music_collection` | Item embeddings (64-dim) với Index IVF_FLAT       |

### Sử dụng cho Recommendation:

1. **Home Page (User-based):** Query MongoDB `users.latent_vector` → Search Milvus `music_collection` → Top-K songs
2. **Next Song (Item-based):** Lấy embedding của bài đang nghe từ Milvus → Search similar → Top-K songs

---

## ⚡ Quick Start (Khuyến nghị)

Nếu bạn đã có data trong `data/processed_sorted/`, chạy nhanh:

```bash
# Terminal 1: Streaming
docker exec -it spark-master bash -c "cd /opt/src/pipelines/ingestion && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 stream_to_minio_turbo.py"

# Terminal 2: Producer BALANCED (mở terminal mới) - ⭐ Khuyến nghị
docker exec -it spark-master python3 /opt/src/pipelines/ingestion/producer_balanced.py

# Hoặc dùng TURBO nếu muốn nhanh nhất (không giữ timestamp gốc):
# docker exec -it spark-master python3 /opt/src/pipelines/ingestion/producer_turbo.py

# Sau khi xong, Ctrl+C cả 2 terminal, restart spark rồi chạy:
docker restart spark-master spark-worker

# ETL + Training (chờ 15-20s sau restart)
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/src/pipelines/batch/etl_master_data.py

docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/src/pipelines/batch/etl_users.py

docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 /opt/src/pipelines/batch/train_als_model.py
```

