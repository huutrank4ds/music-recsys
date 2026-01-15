---
description: Chạy pipeline streaming từ đầu đến cuối (Download → Clean → Sort → Kafka → Spark → MinIO)
---

# 🚀 Music Recommendation System - Full Pipeline

Pipeline xử lý data streaming từ HuggingFace đến MinIO Data Lake.

## 📋 Kiến trúc Pipeline

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
   [Producer] ──→ Kafka Topic "music_log"
                         ↓
               [Spark Streaming ETL]
                         ↓
               MinIO (s3a://datalake/raw/music_logs/)
```

---

## 🔧 BƯỚC 0: Khởi động Infrastructure

```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra services đã sẵn sàng
docker-compose ps
```

Đợi đến khi tất cả services healthy (khoảng 1-2 phút).

---

## 📥 BƯỚC 1: Download Data từ HuggingFace

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
🔥 BẮT ĐẦU QUY TRÌNH TÁI SINH DỮ LIỆU
👉 Xử lý: train.parquet
   ✅ Đã tái sinh thành công
...
```

**Kiểm tra:**
```bash
docker exec spark-master ls -lh /opt/data/data_clean/
```

---

## 📊 BƯỚC 3: Sort Data theo Timestamp

// turbo
```bash
docker exec -it spark-master spark-submit /opt/src/prepare-data/etl_sort.py
```

**Output mong đợi:**
```
🚀 Khởi động Spark Session...
⏳ Đang đọc dữ liệu sạch từ: /opt/data/data_clean/*.parquet
⏳ Đang sắp xếp theo thời gian...
💾 Đang ghi dữ liệu đã sắp xếp ra: /opt/data/processed_sorted
✅ THÀNH CÔNG!
```

**Kiểm tra:**
```bash
docker exec spark-master ls -lh /opt/data/processed_sorted/
```

---

## 🎧 BƯỚC 4: Khởi động Spark Streaming ETL (Terminal 1)

**Mở Terminal mới** và chạy:

```bash
docker exec -it spark-master bash
cd /opt/src
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  stream_etl.py
```

**Output mong đợi:**
```
Khởi động Spark Streaming ETL...
Đang lắng nghe Kafka Topic 'music_log'...
Đang ghi xuống MinIO (Parquet)...
```

**⚠️ QUAN TRỌNG: Giữ terminal này mở, đừng tắt!**

---

## 🚀 BƯỚC 5: Chạy Producer (Terminal 2)

**Mở Terminal mới** và chạy:

// turbo
```bash
docker exec -it spark-master python3 /opt/src/ingestion/producer.py
```

**Output mong đợi:**
```
🔧 Đang kiểm tra Topic 'music_log'...
✅ Topic 'music_log' đã tồn tại.
🔌 Khởi tạo Producer...
🚀 Bắt đầu Replay với tốc độ: x200.0
📖 Đọc file: ...
✅ Sent: 100 | Time: ...
✅ Sent: 200 | Time: ...
```

---

## ✅ BƯỚC 6: Kiểm tra Kết quả trong MinIO

```bash
# Kiểm tra data đã được ghi
docker exec minio mc ls local/datalake/raw/music_logs/ --recursive
```

**Hoặc truy cập MinIO Console:**
- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`
- Navigate to: `datalake` → `raw` → `music_logs`

---

## 📊 Monitoring & Debug

### Kafka UI
- URL: http://localhost:8080
- Xem topic `music_log` và số messages

### Spark Master UI
- URL: http://localhost:8090
- Xem running applications và jobs

### MinIO Console
- URL: http://localhost:9001
- Xem data đã được ghi

---

## 🛑 Dừng Pipeline

```bash
# Terminal chạy Producer: Ctrl+C
# Terminal chạy Spark Streaming: Ctrl+C

# Tắt tất cả services
docker-compose down
```

---

## 🔄 Chạy lại từ đầu (Reset)

```bash
# Xóa data cũ
rm -rf data/raw data/data_clean data/processed_sorted

# Xóa MinIO checkpoints (trong container)
docker exec minio mc rm local/datalake/checkpoints/ --recursive --force
docker exec minio mc rm local/datalake/raw/ --recursive --force

# Chạy lại từ BƯỚC 1
```

---

## 📝 Cấu hình quan trọng

| Config | File | Giá trị |
|--------|------|---------|
| Kafka Bootstrap | `producer.py`, `stream_etl.py` | `kafka:9092` |
| MinIO Endpoint | `stream_etl.py` | `http://minio:9000` |
| Spark Master | `stream_etl.py` | `spark://spark-master:7077` |
| Processing Trigger | `stream_etl.py` | `1 minute` |
| Producer Speed | `producer.py` | `x200` |
