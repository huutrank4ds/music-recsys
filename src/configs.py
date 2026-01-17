import os
from dotenv import load_dotenv

# Tự động tìm và load file .env (nếu chạy local outside Docker)
load_dotenv()

# ==========================================
# 1. KAFKA CONFIGURATIONS
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "music_log")
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

# ==========================================
# 2. MINIO / DATA LAKE CONFIGURATIONS
# ==========================================
# Endpoint dùng cho Spark/Python connect tới MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATALAKE_BUCKET = os.getenv("DATALAKE_BUCKET", "datalake")

# ==========================================
# 3. MONGODB CONFIGURATIONS
# ==========================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "music_recsys")
SONGS_MASTER_LIST_PATH = os.getenv("SONGS_MASTER_LIST_PATH", "/opt/data/songs_master_list")
COLLECTION_SONGS = os.getenv("COLLECTION_SONGS", "songs")

# ==========================================
# 4. PATHS (FILE SYSTEM & OBJECT STORAGE)
# ==========================================
# Đường dẫn Checkpoint (Nên dùng file:/// để ghi ổ cứng local cho nhanh)
MINIO_CHECKPOINT_PATH = os.getenv("MINIO_CHECKPOINT_PATH", "file:///opt/data/checkpoints/music_logs/")

# Đường dẫn dữ liệu thô trên MinIO (s3a://...)
MINIO_RAW_MUSIC_LOGS_PATH = os.getenv("MINIO_RAW_MUSIC_LOGS_PATH", f"s3a://{DATALAKE_BUCKET}/raw/music_logs/")

# Đường dẫn dữ liệu logs đã xử lý
MUSIC_LOGS_DATA_PATH = os.getenv("MUSIC_LOGS_DATA_PATH", "/opt/data/processed_sorted")