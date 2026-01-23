
# 🎵 Music Recommendation System Design

**Project:** Big Data End-term Project
**Architecture:** Lambda Architecture (Spark + Kafka + MongoDB + MinIO)

---

## 📖 System Overview (Tổng quan hệ thống)

Hệ thống được thiết kế để cung cấp trải nghiệm cá nhân hóa cho người dùng nghe nhạc, sử dụng các công nghệ Big Data để xử lý dữ liệu lớn. Hệ thống bao gồm hai tính năng cốt lõi:

1. **Home Page Recommendations:** Gợi ý danh sách bài hát phù hợp với "gu" của người dùng mỗi khi họ truy cập (Batch Processing).
2. **Next Song Prediction:** Tự động đề xuất bài hát tiếp theo dựa trên bài hát đang nghe (Real-time Context / Item-based Filtering).

---

## 📂 Project Structure (Cấu trúc dự án)

Tổ chức mã nguồn và dữ liệu được phân chia rõ ràng theo các tầng xử lý:

```text
music-recsys/
├── docker-compose.yml           # Quản lý hạ tầng
├── .env
├── .gitignore
├── README.md
├── backend/
│   ├── api.Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py
│       ├── api/
│       │   ├── recs.py
│       │   └── search.py
│       ├── core/
│       │   └── database.py
│       └── services/
│           ├── music.py
│           └── recommender.py
├── common/
│   ├── schemas.py
│   └── logger.py
├── data/                        # Dữ liệu (Mounted Volume - Máy Host)
│   ├── raw/                     # Dữ liệu thô (Logs)
│   ├── processed_sorted/        # Dữ liệu Parquet đã làm sạch (Input cho Model)
│   ├── songs_master_list/       # File JSON danh sách bài hát (Output bước ETL)
│   └── checkpoints/             # Spark Streaming Checkpoints
└── data_pipeline/                      
    ├── config.py
    ├── spark.Dockerfile.py
    ├── utils.py
    ├── requirements.txt
    ├── batch/              
    │   ├── etl_master_data.py
    │   ├── etl_users.py  
    │   └── import_master_songs.py
    ├── ingestion/
    │   ├── producer.py
    │   ├── stream_to_mongo.py     
    │   └── stream_to_minio.py
    ├── modeling/
    │   └── train_als_model.py
    └── scripts/ 
        ├── download_data.py
        ├── preprocess_sort.py
        ├── fix_format.py
        └── train_als_model.py

```

## 🗄️ Database Schema Design

Hệ thống sử dụng mô hình lưu trữ lai (Polyglot Persistence): **MongoDB** cho dữ liệu định danh/metadata và **Milvus** cho dữ liệu Vector đặc trưng.

### Phase 0. MinIO (Data Lake - Raw Logs)

> Lưu trữ nhật ký hành vi người dùng (User Logs) dưới dạng **Parquet**, được phân vùng (partition) theo ngày để tối ưu hóa tốc độ truy vấn của Spark.

* **Bucket:** `datalake`
* **Storage Path:** `raw/logs/date=YYYY-MM-DD/part-*.parquet`
* **Format:** Apache Parquet (Snappy Compression)

| Field | Type | Description |
| :--- | :--- | :--- |
| `user_id` | String | ID người dùng (Khóa ngoại tham chiếu `users`). |
| `track_id` | String | ID bài hát (Khóa ngoại tham chiếu `songs`). |
| `timestamp` | **Long** | Thời điểm tương tác (**Epoch Milliseconds**). |
| `action` | String | Loại hành vi: `listen`, `skip`, `complete`. |
| `duration` | **Int** | Thời gian bài hát được nghe |
| `total_duration` | **Int** | Tổng thời lượng bài hát |
| `source` | String | Nguồn dữ liệu: `simulation` (Tool giả lập) hoặc `real_user` (Web App). |

### Phase 1. MongoDB (Metadata & User Profile)

#### Collection: `songs`

> Lưu trữ thông tin hiển thị (Metadata).

| Field         | Type   | Description                   |
| :------------ | :----- | :---------------------------- |
| `_id`       | String | **PK**. Track ID (UUID) |
| `title`     | String | Tên bài hát                |
| `artist`    | String | Tên nghệ sĩ                |
| `artist_id` | String | Mã định danh nghệ sĩ     |
| `duration_ms` | Int | Thời lượng bài hát |
| `image_url` | String | Đường dẫn ảnh đại diện bài hát |
| `url` | String | Đường dẫn đến dữ liệu bài hát |
| `plays_7d` | Int | Số lượt nghe bài hát trong 7 ngày gần nhất |
| `plays_cumulative` | Int | Số lượt nghe bài hát từ khi khởi tạo. |
| `release_date` | String | Ngày khởi tạo bài hát |

#### Collection: `users`

> Lưu trữ vector sở thích người dùng (cập nhật hàng đêm).

| Field             | Type             | Description                              |
| :---------------- | :--------------- | :--------------------------------------- |
| `_id`           | String           | **PK**. User ID                    |
| `username`      | String           | Tên hiển thị                          |
| `latent_vector` | Array`<Float>` | Vector đặc trưng `[0.1, -0.5, ...]` |
| `last_updated`  | Date             | Thời gian chạy model gần nhất        |

---

### Phase 2. Milvus (Vector Database)

#### Collection: `music_collection`

> Lưu trữ vector đặc trưng của bài hát để tìm kiếm tương đồng.

* **Metric Type:** `IP` (Inner Product) - *Tương thích với thuật toán ALS.*
* **Index Type:** `IVF_FLAT` hoặc `HNSW`.

| Field         | Type              | Description                               |
| :------------ | :---------------- | :---------------------------------------- |
| `id`        | String            | **PK**. Track ID (Map với MongoDB) |
| `embedding` | Vector`<Float>` | Item Factors từ Spark ALS                |

## 🔄 Operational Workflow

### 🔹 Phase 1: Ingestion (Real-time Data Lake)

1. **Event:** User nghe nhạc -> Web App gửi log.
2. **Transport:** Kafka topic `music_log` nhận message.
3. **Storage:** Spark Streaming đọc Kafka -> Ghi xuống **MinIO** (Parquet) phân vùng theo ngày.

### 🔹 Phase 2: Batch Training (Nightly Job)

*Chiến lược Sliding Window: Dùng dữ liệu 90 ngày gần nhất.*

1. **Load:** Spark đọc Parquet từ MinIO (Filter: `timestamp >= NOW - 90 days`).
2. **Train:** Chạy thuật toán **ALS (Alternating Least Squares)**.
3. **Sync User:** Lấy `userFactors` -> Update vào MongoDB collection `users` (`latent_vector`).
4. **Sync Item:** Lấy `itemFactors` -> Insert/Replace vào Milvus collection `music_collection`.

### 🔹 Phase 3: Serving (Hybrid Vector Search)

#### Scenario A: Trang chủ (Home Page)

*Mục tiêu: Gợi ý theo sở thích dài hạn.*

1. **Backend:** Lấy `user_vector` từ Redis hoặc từ MongoDB (theo User ID).
2. **Search:** Gửi `user_vector` sang Milvus.
3. **Query:** `Milvus.search(data=[user_vector], limit=10, metric="IP")`.
4. **Result:** Join ID kết quả với MongoDB `songs` -> Trả về Frontend.

#### Scenario B: Bài tiếp theo (Next Song / Smart Session)

*Mục tiêu: Gợi ý theo Mood hiện tại + Sở thích gốc.*

1. **Context:** User vừa nghe bài hát **X**.
2. **Backend:**
   * Lấy `user_vector` (Sở thích gốc) từ Redis hoặc MongoDB.
   * Lấy `song_vector_X` (Mood hiện tại) từ Redis.
3. **Calculation:** Tính Vector Phiên (Session Vector):
   $$
   V_{session} = (0.7 \times V_{user}) + (0.3 \times V_{song\_X})
   $$
4. **Search:** Gửi $V_{session}$ sang Milvus để tìm các bài hát gần nhất.

## ✅ Implementation Checklist (Tiến độ thực hiện)

Dưới đây là danh sách các hạng mục công việc cần hoàn thành để vận hành hệ thống.

### 1. 🏗️ Infrastructure (Hạ tầng)

> Mục tiêu: Dựng môi trường container ổn định.

- [X] **Docker Compose Setup**
  - [X] Cấu hình Apache Spark (Master & Worker).
  - [X] Cấu hình Kafka KRAFT.
  - [X] Cấu hình MinIO (S3 Compatible Storage).
  - [X] Cấu hình MongoDB (NoSQL Database).
  - [X] **[New]** Cấu hình Milvus (Vector Database - Standalone).
  - [X] **[New]** Cấu hình Attu (Dashboard quản lý Milvus).
- [X] **Networking:** Đảm bảo các container thông nhau (Bridge Network).
- [X] **Volume Persistence:** Mount volume cho DB để tránh mất dữ liệu.

### 2. 📥 Data Ingestion (Thu thập dữ liệu)

> Mục tiêu: Đưa dữ liệu hành vi người dùng vào Data Lake.

- [X] **Fake Data Producer**
  - [X] Script Python giả lập hành vi nghe nhạc của người dùng.
  - [X] Đẩy message vào Kafka topic `music_log`.
- [X] **Streaming Pipeline**
  - [X] Spark Structured Streaming đọc từ Kafka.
  - [X] Sink dữ liệu xuống MinIO dưới dạng file `.parquet`.
  - [X] Partition dữ liệu theo ngày (`date=YYYY-MM-DD`).
  - [X] Update lượt nghe xuống MongoDB.

### 3. 🧹 ETL & Master Data (Làm sạch & Metadata)

> Mục tiêu: Đồng bộ danh sách bài hát chuẩn vào Database.

- [X] **Song Metadata Sync Script**
  - [X] Đọc Log hoặc Dataset gốc.
  - [X] **Mapping:** Chuẩn hóa tên trường (`track_name` -> `title`, `artist_name` -> `artist`).
  - [X] **Filtering:** Lọc bỏ bản ghi lỗi/rác.
  - [X] **Upsert:** Lưu vào MongoDB collection `songs`.
  - [X] **Indexing:** Đánh index cho `artist_id` và `title`.

### 4. 🧠 AI & Model Training (Batch Processing)

> Mục tiêu: Học thói quen người dùng & Sinh Vector đặc trưng.

- [X] **Environment Setup**
  - [X] Cài đặt `mongo-spark-connector`, `pymongo`, `pymilvus` trên Spark Worker.
- [X] **Training Job (`train_als_vector.py`)**
  - [X] **Sliding Window:** Chỉ load dữ liệu Parquet 90 ngày gần nhất.
  - [X] **Training:** Huấn luyện mô hình ALS (Alternating Least Squares).
  - [X] **Export Users:** Lưu `userFactors` vào MongoDB (`users` collection).
  - [X] **Export Items:** Lưu `itemFactors` vào Milvus (`music_collection`).
  - [X] **Index Building:** Build Index (IVF_FLAT/HNSW) cho Milvus.

### 5. 🔌 Backend API (Serving Layer)

> Mục tiêu: API phục vụ Frontend & Tính toán Vector.

- [X] **Core Logic**
  - [X] Module kết nối MongoDB & Milvus.
  - [X] Hàm `vector_search(vector, top_k)`.
  - [X] Hàm tính toán `session_vector` (Weighted Average).
- [X] **API Endpoints**
  - [X] `GET /songs`: Danh sách bài hát (Pagination).
  - [X] `GET /recommend/home`: Gợi ý trang chủ (User Vector -> Milvus).
  - [X] `POST /recommend/next`: Gợi ý bài tiếp theo (Session Vector -> Milvus).

### 6. 💻 Frontend (Web App)

> Mục tiêu: Giao diện người dùng cuối.

- [ ] **Home Page:** Hiển thị danh sách gợi ý cá nhân hóa.
- [ ] **Music Player:** Phát nhạc từ URL MinIO.
- [ ] **Smart Queue:** Tự động fetch bài hát tiếp theo từ API `/next`.

---

## 🚀 Hướng dẫn chạy (Quick Start)

### 1. Khởi động Hạ tầng

```bash
docker-compose up -d
```
