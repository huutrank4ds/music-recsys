---
description: Chạy pipeline streaming từ đầu đến cuối (Download → Clean → Sort → Kafka → Spark → MinIO → MongoDB)
---
# Music Recommendation System - Full Pipeline

Pipeline xử lý data streaming từ HuggingFace đến MinIO Data Lake và MongoDB.

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
   [Producer] ──→ Kafka Topic "music_log"
                         ↓
               [Spark Streaming ETL]
                         ↓
               MinIO (s3a://datalake/raw/music_logs/)
                         ↓
               [ETL Master Data]
                         ↓
               MongoDB (music_recsys.songs)
```

---

## BƯỚC 0: Khởi động Infrastructure

```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra services đã sẵn sàng
docker-compose ps
```

Đợi đến khi tất cả services healthy (khoảng 1-2 phút).

**Các services cần chạy:**

- `kafka` (Healthy)
- `kafka-ui`
- `spark-master`
- `spark-worker`
- `minio`
- `mongodb`

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

## BƯỚC 4: Khởi động Spark Streaming ETL (Terminal 1)

**Mở Terminal mới** và chạy:

```bash
docker exec -it spark-master bash -c "cd /opt/src/ingestion && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 stream_to_minio.py"
```

**Output mong đợi:**

```
Khởi động Spark Streaming ETL...
Đang lắng nghe Kafka Topic 'music_log'...
Đang ghi xuống MinIO (Parquet)...
```

**QUAN TRỌNG: Giữ terminal này mở, đừng tắt!**

---

## BƯỚC 5: Chạy Producer (Terminal 2)

**Mở Terminal mới** và chạy:

```bash
docker exec -it spark-master python3 /opt/src/ingestion/producer.py
```

**Output mong đợi:**

```
Đang kiểm tra Topic 'music_log'...
Topic 'music_log' đã tồn tại.
Khởi tạo Producer...
Bắt đầu Replay với tốc độ: x200.0
Đọc file: ...
Sent: 100 | Time: ...
Sent: 200 | Time: ...
```

 **Đợi khoảng 1-2 phút** để có đủ data ghi vào MinIO, sau đó nhấn `Ctrl+C` để dừng Producer.

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

## BƯỚC 7: ETL Master Data (MinIO → MongoDB)

**Dừng Spark Streaming** (Terminal 1) bằng `Ctrl+C`, sau đó restart Spark cluster:

```bash
# Restart Spark để giải phóng resources
docker restart spark-master spark-worker

# Đợi 15-20 giây cho cluster sẵn sàng
```

**Chạy ETL Master Data:**

```bash
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /opt/src/processing/etl_master_data.py
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

## BƯỚC 8: Kiểm tra MongoDB

```bash
# Đếm số bài hát
docker exec mongodb mongo music_recsys --eval "db.songs.count()"

# Xem 5 bài hát đầu tiên
docker exec mongodb mongo music_recsys --eval "db.songs.find().limit(5).pretty()"
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

# 4. Chạy lại từ BƯỚC 1
```

---

## Cấu hình quan trọng

| Config             | File                                           | Giá trị                     |
| ------------------ | ---------------------------------------------- | ----------------------------- |
| Kafka Bootstrap    | `producer.py`, `stream_to_minio.py`        | `kafka:9092`                |
| MinIO Endpoint     | `stream_to_minio.py`, `etl_master_data.py` | `http://minio:9000`         |
| MongoDB URI        | `etl_master_data.py`                         | `mongodb://mongodb:27017`   |
| Spark Master       | `etl_master_data.py`                         | `spark://spark-master:7077` |
| Processing Trigger | `stream_to_minio.py`                         | `1 minute`                  |
| Producer Speed     | `producer.py`                                | `x200`                      |

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

### 3. MinIO Console không truy cập được

**Nguyên nhân:** Docker Desktop chưa chạy hoặc container chưa start.

**Giải pháp:**

```bash
# Kiểm tra Docker đang chạy
docker ps

# Khởi động lại services
docker-compose up -d
```

### 4. Producer không gửi được message

**Nguyên nhân:** Kafka chưa healthy.

**Giải pháp:**

```bash
# Kiểm tra Kafka status
docker-compose ps kafka

# Đợi đến khi Kafka healthy rồi chạy lại
```

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
│   ├── ingestion/
│   │   ├── producer.py         # Gửi data vào Kafka
│   │   └── stream_to_minio.py  # Spark Streaming: Kafka → MinIO
│   └── processing/
│       └── etl_master_data.py  # ETL: MinIO → MongoDB
├── docker-compose.yml
├── .env
└── run-pipeline.md             # File hướng dẫn này
```
