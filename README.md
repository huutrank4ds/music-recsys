# 🎵 Music Recommendation System Design

**Project:** Big Data End-term Project  
**Architecture:** Lambda Architecture (Spark + Kafka + MongoDB + MinIO)

---

## 📖 1. System Overview (Tổng quan hệ thống)

Hệ thống được thiết kế để cung cấp trải nghiệm cá nhân hóa cho người dùng nghe nhạc, sử dụng các công nghệ Big Data để xử lý dữ liệu lớn. Hệ thống bao gồm hai tính năng cốt lõi:

1.  **Home Page Recommendations:** Gợi ý danh sách bài hát phù hợp với "gu" của người dùng mỗi khi họ truy cập (Batch Processing).
2.  **Next Song Prediction:** Tự động đề xuất bài hát tiếp theo dựa trên bài hát đang nghe (Real-time Context / Item-based Filtering).

---

## 📂 2. Project Structure (Cấu trúc dự án)

Tổ chức mã nguồn và dữ liệu được phân chia rõ ràng theo các tầng xử lý:

```text
music-recsys/
├── docker-compose.yml           # Quản lý hạ tầng (Spark, Kafka, Mongo, MinIO)
├── configs/                     # Các file cấu hình môi trường
│   └── spark-defaults.conf
├── data/                        # Dữ liệu (Mounted Volume - Máy Host)
│   ├── raw/                     # Dữ liệu thô (Logs)
│   ├── processed_sorted/        # Dữ liệu Parquet đã làm sạch (Input cho Model)
│   ├── songs_master_list/       # File JSON danh sách bài hát (Output bước ETL)
│   └── checkpoints/             # Spark Streaming Checkpoints
├── src/                         # Mã nguồn chính
│   ├── 1_ingestion/             # Tầng thu thập dữ liệu
│   │   ├── producer.py          # Giả lập hành vi User -> Kafka (Time Travel logic)
│   │   └── stream_to_minio.py   # Spark Streaming: Đọc Kafka -> Ghi MinIO
│   ├── 2_processing/            # Tầng xử lý Batch & AI
│   │   ├── etl_master_data.py   # Trích xuất danh sách bài hát -> MongoDB
│   │   └── train_als_model.py   # Huấn luyện ALS -> MongoDB (UserRecs & ItemSims)
│   └── 3_serving/               # Tầng phục vụ (Backend)
│       └── api_server.py        # API Query MongoDB trả về Frontend
└── notebooks/                   # Jupyter Notebook (Dùng để kiểm thử nhanh)
```

## 🗄️ 3. Database Design (MongoDB Schema)

Hệ thống sử dụng **MongoDB** làm **Serving Layer** để đảm bảo độ trễ thấp (<50ms) cho người dùng cuối. Cần tạo database tên `music_recsys`.

### 3.1. Collection: `songs` (Master Data)
> **Mục đích:** Lưu trữ thông tin hiển thị của bài hát (Metadata) như tên, ca sĩ, ảnh bìa.  
> **Nguồn:** Trích xuất từ lịch sử Log (ETL script).

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `_id` | String | **PK**. MusicBrainz Track ID (UUID) |
| `title` | String | Tên bài hát |
| `artist` | String | Tên nghệ sĩ |
| `track_index` | Long | Mã số nguyên (Mapping với Spark Model) |
| `url` | String | Đường dẫn file nhạc (MinIO URL - Optional) |

### 3.2. Collection: `user_recommendations` (Batch View)
> **Mục đích:** Phục vụ tính năng "Gợi ý cho bạn" ở trang chủ.  
> **Nguồn:** Output từ thuật toán **ALS (Collaborative Filtering)** của Spark.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `_id` | String | **PK**. User ID |
| `recommendations` | Array | Danh sách Top-K bài hát gợi ý |
| ↳ `song_id` | String | MusicBrainz Track ID |
| ↳ `score` | Double | Điểm dự đoán (Rating) |

### 3.3. Collection: `song_similarities` (Real-time Context)
> **Mục đích:** Phục vụ tính năng "Phát tiếp theo" (Next Song Prediction).  
> **Nguồn:** Output từ ma trận đặc trưng **Item-Item Similarity**.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `_id` | String | **PK**. Track ID bài đang nghe (Source) |
| `similar_songs` | Array | Danh sách bài hát tương đồng nhất |
| ↳ `song_id` | String | Track ID bài gợi ý |
| ↳ `similarity` | Double | Độ tương đồng Cosine (0.0 - 1.0) |

---

## 🔄 4. Workflow (Quy trình vận hành)

Quy trình vận hành được chia thành 3 giai đoạn hoạt động tuần hoàn:

### 🔹 Phase 1: Ingestion & Storage (Real-time)
*Thu thập hành vi người dùng và lưu trữ lâu dài.*

1.  User tương tác trên Web App (nghe, like, skip).
2.  Script `producer.py` đẩy sự kiện vào **Kafka** topic `music_log`.
3.  Script `stream_to_minio.py` (Spark Structured Streaming) đọc từ Kafka và ghi xuống **MinIO** dưới dạng file **Parquet** (được partition theo ngày `date=YYYY-MM-DD`).

### 🔹 Phase 2: Training & Computation (Batch - Daily)
*Cập nhật trí tuệ cho AI định kỳ (hàng ngày hoặc mỗi 4 giờ).*

1.  **ETL Step:** Chạy `etl_master_data.py`.
    * Quét logs trong MinIO.
    * Lọc danh sách bài hát duy nhất $\rightarrow$ Update vào MongoDB collection `songs`.
2.  **Training Step:** Chạy `train_als_model.py`.
    * Load Parquet từ MinIO.
    * Train **ALS Model** (Alternating Least Squares).
    * **Task A:** Dự đoán Top songs cho mỗi User $\rightarrow$ Ghi đè MongoDB `user_recommendations`.
    * **Task B:** Tính toán Item Similarity Matrix $\rightarrow$ Ghi đè MongoDB `song_similarities`.

### 🔹 Phase 3: Serving (Online)
*API Backend phản hồi Frontend dựa trên dữ liệu đã tính sẵn.*

* **Scenario A: Home Page (Trang chủ)**
    * Frontend gọi API $\rightarrow$ Backend query `db.user_recommendations.find({"_id": user_id})`.
    * Backend lấy danh sách ID $\rightarrow$ Join với `db.songs` để lấy tên bài/nghệ sĩ.
    * Trả về JSON cho Frontend hiển thị.
* **Scenario B: Next Song (Bài tiếp theo)**
    * User đang nghe bài **X**.
    * Backend query `db.song_similarities.find({"_id": X})`.
    * Backend lọc bỏ các bài User vừa nghe gần đây (trong Redis/Session) để tránh lặp.
    * Trả về bài hát có độ tương đồng cao nhất.

---

## ✅ 5. Implementation Checklist

- [x] **Infrastructure:** Setup Docker Compose (Spark, Kafka, Mongo, MinIO, Milvus).
- [x] **Producer:** Python script giả lập dữ liệu vào Kafka (Time Travel logic).
- [x] **Streaming Consumer:** Spark Structured Streaming đọc Kafka → Ghi MinIO Parquet.
- [x] **ETL Master Data:** Spark Batch trích xuất bài hát từ Parquet → MongoDB `songs`.
- [x] **ETL Users:** Spark Batch trích xuất users từ Parquet → MongoDB `users`.
- [x] **AI Model:** Spark MLlib train ALS & Sync vectors:
  - User Factors → MongoDB `users.latent_vector`
  - Item Factors → Milvus `music_collection`
- [ ] **Backend API:** Python/NodeJS API query MongoDB + Milvus phục vụ Frontend.

---

## 🔹 6. Milvus Vector Database

### Collection: `music_collection`
> **Mục đích:** Lưu trữ vector đặc trưng của bài hát để tìm kiếm tương đồng (Item-based).
> **Metric Type:** IP (Inner Product) - Tương thích với thuật toán ALS.
> **Index Type:** IVF_FLAT

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `id` | VARCHAR(100) | **PK**. Track ID (Map với MongoDB) |
| `embedding` | FLOAT_VECTOR(64) | Item Factors từ Spark ALS |

### How it works:
1. **Training:** Spark ALS train model → Extract itemFactors (64-dim vectors).
2. **Indexing:** Insert vectors vào Milvus với IVF_FLAT index.
3. **Search:** Query user vector (từ MongoDB) → Milvus trả về Top-K similar songs.

---

## 🔄 7. Phase 2: Batch Training (Nightly Job)

### Chiến lược Sliding Window
Dùng dữ liệu **90 ngày gần nhất** để train model.

### Workflow:
```bash
# Bước 1: ETL Master Data (songs)
docker exec spark-master spark-submit /opt/src/processing/etl_master_data.py

# Bước 2: ETL Users 
docker exec spark-master spark-submit /opt/src/processing/etl_users.py

# Bước 3: Train ALS & Sync Vectors
docker exec spark-master spark-submit /opt/src/processing/train_als_model.py
```

### Output:
- **MongoDB `users`**: Mỗi user có `latent_vector` (64-dim).
- **Milvus `music_collection`**: Mỗi bài hát có `embedding` (64-dim).

---

## 📦 8. Docker Services

| Service | Port | Purpose |
| :--- | :--- | :--- |
| Kafka | 9092, 9094 | Message Queue |
| Kafka UI | 8080 | Kafka Dashboard |
| Spark Master | 9090 | Spark Web UI |
| MinIO | 9000, 9001 | Object Storage (Data Lake) |
| MongoDB | 27017 | Metadata & User Profiles |
| Milvus | 19530 | Vector Database |
