import os
from dotenv import load_dotenv

# Tự động tìm và load file .env (nếu chạy local outside Docker)
load_dotenv()

# 1. KAFKA CONFIGURATIONS
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "music_log")
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

# 2. MINIO / DATA LAKE CONFIGURATIONS
# Endpoint dùng cho Spark/Python connect tới MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATALAKE_BUCKET = os.getenv("DATALAKE_BUCKET", "datalake")


# 3. MONGODB CONFIGURATIONS
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "music_recsys")
SONGS_MASTER_LIST_PATH = os.getenv("SONGS_MASTER_LIST_PATH", "/opt/data/songs_master_list")
COLLECTION_SONGS = os.getenv("COLLECTION_SONGS", "songs")


# 4. PATHS (FILE SYSTEM & OBJECT STORAGE)
# Đường dẫn Checkpoint (Nên dùng file:/// để ghi ổ cứng local cho nhanh)
MINIO_CHECKPOINT_PATH = os.getenv("MINIO_CHECKPOINT_PATH", "file:///opt/data/checkpoints/")

# Đường dẫn dữ liệu thô trên MinIO (s3a://...)
MINIO_RAW_MUSIC_LOGS_PATH = os.getenv("MINIO_RAW_MUSIC_LOGS_PATH", f"s3a://{DATALAKE_BUCKET}/raw/music_logs/")

# Đường dẫn dữ liệu logs đã xử lý
MUSIC_LOGS_DATA_PATH = os.getenv("MUSIC_LOGS_DATA_PATH", "/opt/data/processed_sorted")

# 5. MILVUS CONFIGURATIONS
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
MILVUS_COLLECTION = os.getenv("MILVUS_COLLECTION", "music_collection")

# 6. ALS MODEL HYPERPARAMETERS

ALS_RANK = int(os.getenv("ALS_RANK", "64"))           # Số chiều của latent vector
ALS_MAX_ITER = int(os.getenv("ALS_MAX_ITER", "15"))   # Số vòng lặp
ALS_REG_PARAM = float(os.getenv("ALS_REG_PARAM", "0.1"))  # Regularization
ALS_ALPHA = float(os.getenv("ALS_ALPHA", "40.0"))     # Confidence scaling 
SLIDING_WINDOW_DAYS = int(os.getenv("SLIDING_WINDOW_DAYS", "90"))  # Ngày dữ liệu

# 7. COLLECTION NAMES
COLLECTION_USERS = os.getenv("COLLECTION_USERS", "users")