
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
│       │   ├── search.py
│       │   └── logging.py
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
    ├── spark.Dockerfile
    ├── utils.py
    ├── requirements.txt
    ├── batch/            
    │   ├── etl_master_data.py
    │   ├── etl_users.py  
    │   └── import_master_songs.py
    ├── ingestion/
    │   ├── producer.py
    │   └── stream_to_minio.py
    ├── modeling/
    │   └── train_als_model.py
    └── scripts/ 
        ├── download_data.py
        ├── preprocess_sort.py
        ├── fix_format.py
        └── fetch_lyrics_lrclib.py
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
| `duration` | Integer | Thời gian đã nghe (giây). |
| `total_duration` | Integer | Tổng thời lượng bài hát (giây). |
| `source` | String | Nguồn dữ liệu: `simulation` (Tool giả lập) hoặc `real_user` (Web App). |

### Phase 1. MongoDB (Metadata & User Profile)

#### Collection: `songs`

> Lưu trữ thông tin hiển thị (Metadata).

| Field | Type | Description |
| :--- | :--- | :--- |
| `_id` | String | **PK**. Track ID (UUID) |
| `track_name` | String | Tên bài hát |
| `artist_name` | String | Tên nghệ sĩ |
| `artist_id` | String | Mã định danh nghệ sĩ |
| `image_url` | String | Đường dẫn ảnh đại diện bài hát |
| `url` | String | Đường dẫn đến dữ liệu bài hát |
| `plays_7d` | String | Lượt nghe bài hát trong 7 ngày gần nhất |
| `plays_cumulative` | String | Lượt nghe bài hát tích lũy |
| `duration` | String | Thời lượng bài hát (s) |
| `release_date` | String | YYYY-MM-DD |
| `lrclib_plain_lyrics` | String | Lời bài hát |
| `lrclib_synced_lyrics` | String | Lời bài hát có thông tin thời gian |

#### Collection: `users`

> Lưu trữ vector sở thích người dùng (cập nhật hàng đêm).

| Field | Type | Description |
| :--- | :--- | :--- |
| `_id` | String | **PK**. User ID |
| `username` | String | Tên hiển thị |
| `latent_vector` | Array `<Float>` | Vector đặc trưng `[0.1, -0.5, ...]` |
| `last_updated` | Date | Thời gian chạy model gần nhất |

---

### Phase 2. Milvus (Vector Database)
#### Collection 1: `music_collection` (Collaborative Filtering)
> Lưu trữ vector đặc trưng của bài hát từ User Behavior (ALS).
* **Metric Type:** `IP` (Inner Product).
* **Dim:** 64 (latent factors).

#### Collection 2: `lyrics_embeddings` (Content-Based Filtering)
> Lưu trữ vector đặc trưng của lời bài hát (Lyrics).
* **Metric Type:** `IP` (Cosine Similarity).
* **Dim:** 384 (all-MiniLM-L6-v2).

## 🔄 Operational Workflow

### 🔹 Phase 1: Ingestion (Real-time Data Lake)
1. **Event:** User nghe nhạc -> Web App gửi log.
2. **Transport:** Kafka topic `music_log` nhận message.
3. **Storage:** Spark Streaming đọc Kafka -> Ghi xuống **MinIO** (Parquet).

### 🔹 Phase 2: Batch Training (Collaborative Filtering)
1. **Load:** Spark đọc Parquet từ MinIO.
2. **Train:** Chạy thuật toán **ALS**.
3. **Sync:** Update User Vector (MongoDB) và Item Vector (Milvus `music_collection`).

### 🔹 Phase 3: Content-Based Enrichment
1. **Fetch:** Lấy lời bài hát (Lyrics) từ **LRCLIB API**.
2. **Embed:** Dùng **Sentence Transformer** (`all-MiniLM-L6-v2`) tạo vector.
3. **Sync:** Lưu vector vào Milvus `lyrics_embeddings`.

### 🔹 Phase 4: Serving (Hybrid Recommendation)

#### Scenario A: Trang chủ (Home Page)
*Collaborative Filtering*
* **Input:** User ID.
* **Process:** Lấy User Vector -> Search Milvus `music_collection`.

#### Scenario B: Bài tiếp theo (Hybrid Logic)
*Kết hợp 60% Hành vi + 40% Nội dung*

1. **ALS Candidate:** Tìm bài user khác cũng nghe (Milvus `music_collection`).
2. **Content Candidate:** Tìm bài có lời tương tự (Milvus `lyrics_embeddings`).
3. **Merge:** Trộn kết quả tỉ lệ 60/40 -> Trả về danh sách.

## 🧠 Recommendation Engine Strategy

Hệ thống sử dụng chiến lược lai (Hybrid), kết hợp sức mạnh của **Collaborative Filtering** (hành vi đám đông) và **Content-Based Filtering** (nội dung âm nhạc), đồng thời phân tách rõ ràng giữa sở thích dài hạn (Long-term) và ngắn hạn (Short-term).

### 1. Implicit Feedback Formula (Tính điểm hành vi)
Để lượng hóa mức độ yêu thích của người dùng $u$ đối với bài hát $i$, chúng ta không chỉ đếm số lượt nghe mà sử dụng công thức tính điểm hành vi như sau:

$$R_{ui} = w_1 \cdot \mathbb{I}(\text{is\_complete}) - w_2 \cdot \mathbb{I}(\text{is\_skip}) + w_3 \cdot \log(1 + \text{duration})$$

*Trong đó:*
*   $\mathbb{I}(\cdot)$: Hàm chỉ thị (1 nếu đúng, 0 nếu sai).
*   $w_1, w_3$: Trọng số tích cực (thưởng cho việc nghe hết bài và nghe lâu).
*   $w_2$: Trọng số tiêu cực (phạt nặng hành vi bỏ qua bài hát).

### 2. Hybrid Scoring Formula (Tính điểm gợi ý)
Hệ thống sử dụng mô hình 3 tầng để cá nhân hóa chính xác nhất:

$$Score(j) = \alpha \cdot \underbrace{\text{Sim}_{Behavior}(\text{Session}, j)}_{\text{User Behavior}} + (1 - \alpha) \cdot \underbrace{\text{Sim}_{Content}(c, j)}_{\text{Instant Context}}$$

*Trong đó:*
*   **User Behavior (Nhánh hành vi):** Kết hợp giữa sở thích lâu dài và Session hiện tại.
    *   Vector dùng để search: $\vec{V}_{target} = 0.3 \cdot \vec{V}_{Long\_term} + 0.7 \cdot \vec{V}_{Short\_term\_Session}$
    *   $\vec{V}_{Short\_term\_Session}$: Vector cộng dồn các bài user vừa nghe trong phiên.
*   **Instant Context (Nhánh nội dung):** Dựa trên nội dung bài hát đang phát.
    *   $\text{Sim}_{Content}$: Độ tương đồng Lyrics giữa bài đang nghe ($c$) và bài ứng viên ($j$).
*   $\alpha$: Hệ số cân bằng (0.6). Hệ thống ưu tiên hành vi người dùng, nhưng dùng nội dung để lấp đầy và khám phá.

### 3. Phân loại chiến lược
| Chiến lược | Kỹ thuật | Mục đích | Dữ liệu đầu vào |
| :--- | :--- | :--- | :--- |
| **Long-term** | Matrix Factorization (ALS) | Gợi ý dựa trên "gu" âm nhạc cố định. | Lịch sử nghe trong 90 ngày. |
| **Short-term** | Sentence Transformers (BERT) | Gợi ý dựa trên tâm trạng/nội dung hiện tại. | Bài hát đang nghe (Lyrics). |

### 4. Lyrics Embedding Strategy (Xử lý lời bài hát)
Để máy tính có thể "hiểu" được nội dung bài hát, hệ thống áp dụng kỹ thuật **Semantic Search** thông qua các bước sau:

1.  **Vectorization (Mã hóa):** Sử dụng Pre-trained Model `sentence-transformers/all-MiniLM-L6-v2` để chuyển đổi lời bài hát (Text) thành Vector 384 chiều. Model này tối ưu cho việc tìm kiếm sự tương đồng ngữ nghĩa.
2.  **Indexing (Đánh chỉ mục):** Lưu trữ vectors vào Milvus với Index `IVF_FLAT` hoặc `HNSW` để tối ưu tốc độ tìm kiếm trong không gian lớn.
3.  **Searching (Tìm kiếm):** Sử dụng phép đo `Cosine Similarity` để tìm các bài hát có "khoảng cách" gần nhất với bài đang nghe.

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
- [X] **Training Job (`train_als_model.py`)**
  - [X] **Sliding Window:** Chỉ load dữ liệu Parquet 90 ngày gần nhất.
  - [X] **Training:** Huấn luyện mô hình ALS (Alternating Least Squares).
  - [X] **Export Users:** Lưu `userFactors` vào MongoDB (`users` collection).
  - [X] **Export Items:** Lưu `itemFactors` vào Milvus (`music_collection`).
  - [X] **Index Building:** Build Index (IVF_FLAT/HNSW) cho Milvus.

### 5. 🔌 Backend API (Serving Layer)

> Mục tiêu: API phục vụ Frontend & Tính toán Vector.

- [X] **Core Logic**
  - [X] Module kết nối MongoDB & Milvus (`database.py`).
  - [X] Hàm `vector_search(vector, top_k)` (`recommender.py`).
  - [X] Hàm tính toán `session_vector` (Weighted Average).
- [X] **API Endpoints**
  - [X] `GET /api/v1/search/songs`: Tìm kiếm bài hát.
  - [X] `GET /api/v1/recs/recommendations/{user_id}`: Gợi ý trang chủ.
  - [X] `GET /api/v1/recs/next-songs/{user_id}/{song_id}`: Gợi ý bài tiếp theo.
  - [X] `POST /api/v1/logs/event`: Nhận log từ Web App.

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
