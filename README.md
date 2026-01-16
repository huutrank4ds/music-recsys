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
├── .env
├── scripts/
│   ├── download_data.py
│   ├── fix_format.py
│   └── preprocess_sort.py
├── configs/                     # Các file cấu hình môi trường
│   └── spark-defaults.conf
├── data/                        # Dữ liệu (Mounted Volume - Máy Host)
│   ├── raw/                     # Dữ liệu thô (Logs)
│   ├── processed_sorted/        # Dữ liệu Parquet đã làm sạch (Input cho Model)
│   ├── songs_master_list/       # File JSON danh sách bài hát (Output bước ETL)
│   └── checkpoints/             # Spark Streaming Checkpoints
├── src/                         # Mã nguồn chính
│   ├── config.py
│   ├── utils.py
│   ├── app/                       <-- (Web App & API)
│   │   ├── __init__.py
│   │   ├── main.py                <-- (File chạy chính của Web)
│   │   └── templates/
│   │       └── index.html
│   └── pipelines/
│       ├── ingestion/
│       │   ├── producer.py
│       │   └── kafka_to_minio.py
│       └── batch/
│           └── sync_songs_master.py
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

- [x] **Infrastructure:** Setup Docker Compose (Spark, Kafka, Mongo, MinIO).
- [x] **Producer:** Python script giả lập dữ liệu vào Kafka (Time Travel logic).
- [ ] **Streaming Consumer:** Spark Structured Streaming đọc Kafka $\rightarrow$ Ghi MinIO Parquet.
- [ ] **ETL Master Data:** Spark Batch trích xuất bài hát từ Parquet $\rightarrow$ MongoDB `songs`.
- [ ] **AI Model:** Spark MLlib train ALS & Item Similarity $\rightarrow$ MongoDB `user_recommendations` & `song_similarities`.
- [ ] **Backend API:** Python/NodeJS API query MongoDB phục vụ Frontend.
